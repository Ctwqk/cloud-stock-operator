import json
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List

import boto3
import hashlib
from botocore.exceptions import ClientError

TABLE_NAME = os.environ.get("TABLE_NAME", "")
SYSTEM_OPS_TOPIC_ARN = os.environ.get("SYSTEM_OPS_TOPIC_ARN", "")

dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")
sts = boto3.client("sts")

try:
    import yfinance as yf  # type: ignore[import-not-found]
except ImportError as exc:
    # Log the real import error so it is visible in CloudWatch,
    # then fall back to "no yfinance" mode.
    import traceback

    print("ERROR: Failed to import yfinance in NewsFetcherLambda:", exc)
    traceback.print_exc()
    yf = None  # type: ignore[assignment]

_ACCOUNT_ID_CACHE: str | None = None


def _get_account_id() -> str:
    """
    Resolve the AWS account id from STS once per container.
    """
    global _ACCOUNT_ID_CACHE
    if _ACCOUNT_ID_CACHE is None:
        identity = sts.get_caller_identity()
        _ACCOUNT_ID_CACHE = identity["Account"]
    return _ACCOUNT_ID_CACHE


def _get_watchlist_symbols(account_id: str) -> List[str]:
    """
    Query DynamoDB for watchlist entries:
    PK = ACCOUNT#<account_id>, SK begins_with STOCK#
    Returns a list of stock symbols.
    """
    table = dynamodb.Table(TABLE_NAME)
    resp = table.query(
        KeyConditionExpression="PK = :pk AND begins_with(SK, :sk_prefix)",
        FilterExpression="attribute_not_exists(is_deleted) OR is_deleted = :false",
        ExpressionAttributeValues={
            ":pk": f"ACCOUNT#{account_id}",
            ":sk_prefix": "STOCK#",
            ":false": False,
        },
    )
    symbols: List[str] = []
    for item in resp.get("Items", []):
        symbol = item.get("symbol")
        if symbol:
            symbols.append(symbol)
    return symbols


def _news_hash(symbol: str, headline: str, published_at: str) -> str:
    """
    Compute a stable hash for a news item so we can deduplicate across runs.
    """
    h = hashlib.sha256()
    h.update(symbol.upper().encode("utf-8"))
    h.update(b"|")
    h.update(headline.strip().encode("utf-8"))
    h.update(b"|")
    h.update(published_at.encode("utf-8"))
    return h.hexdigest()[:32]


def _mark_news_seen(account_id: str, symbol: str, headline: str, published_at: str) -> bool:
    """
    Record that we've seen this news item.

    Returns True if this is the first time we see this hash (i.e., we should
    process/publish it), False if it's a duplicate.
    """
    table = dynamodb.Table(TABLE_NAME)
    h = _news_hash(symbol, headline, published_at)
    pk = f"ACCOUNT#{account_id}"
    sk = f"NEWSHASH#{symbol.upper()}#{h}"
    try:
        table.put_item(
            Item={
                "PK": pk,
                "SK": sk,
                "symbol": symbol.upper(),
                "headline": headline,
                "published_at": published_at,
                "created_at": datetime.now(timezone.utc).isoformat(),
            },
            ConditionExpression="attribute_not_exists(PK)",
        )
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            # Already seen this hash
            return False
        raise


def _fake_fetch_news(symbol: str) -> List[Dict[str, Any]]:
    """
    Legacy placeholder for real news fetching (no longer used in production).
    Kept only for possible local testing.
    """
    now = datetime.now(timezone.utc).isoformat()
    return [
        {
            "symbol": symbol,
            "headline": f"Dummy headline for {symbol}",
            "body": f"Dummy body text for {symbol} at {now}",
            "source": "placeholder",
            "published_at": now,
        }
    ]


def _fetch_news(symbol: str, max_items: int = 10) -> List[Dict[str, Any]]:
    """
    Fetch recent news for a symbol using yfinance if available.
    If yfinance is unavailable or errors, return an empty list (no fake news).
    """
    if yf is None:
        print("yfinance not available in layer; skipping real news fetch")
        return []

    try:
        ticker = yf.Ticker(symbol)
        raw_items = ticker.news or []
    except Exception as exc:  # pragma: no cover - best-effort for homework
        print(f"yfinance error for {symbol}: {exc}")
        return []

    items: List[Dict[str, Any]] = []
    now = datetime.now(timezone.utc).isoformat()
    for n in raw_items[:max_items]:
        # yfinance may return either a flat dict with title/summary or a nested
        # structure under "content" (as in Yahoo Finance video/news objects).
        content = n.get("content") or n

        title = (content.get("title") or "").strip()
        summary = (content.get("summary") or content.get("description") or "").strip()

        # Prefer explicit publisher/provider fields, otherwise fall back.
        provider = (
            content.get("publisher")
            or (content.get("provider") or {}).get("displayName")
            or n.get("publisher")
            or "yfinance"
        )

        published_ts = n.get("providerPublishTime")
        if published_ts:
            published_at = datetime.fromtimestamp(
                published_ts, tz=timezone.utc
            ).isoformat()
        else:
            # Fallback to pubDate if present, else "now".
            pub_date = content.get("pubDate")
            if isinstance(pub_date, str) and pub_date:
                try:
                    # Handle trailing "Z" by normalizing to +00:00
                    dt = datetime.fromisoformat(
                        pub_date.replace("Z", "+00:00")
                    )
                    published_at = dt.astimezone(timezone.utc).isoformat()
                except Exception:
                    published_at = now
            else:
                published_at = now

        # Skip completely empty news (no title and no summary).
        if not title and not summary:
            continue

        items.append(
            {
                "symbol": symbol,
                "headline": title,
                "body": summary,
                "source": provider,
                "published_at": published_at,
            }
        )

    return items


def _get_last_price(symbol: str):
    """
    Fetch last price for the symbol using yfinance if available.
    """
    if yf is None:
        return None
    try:
        ticker = yf.Ticker(symbol)
        fi = getattr(ticker, "fast_info", None)
        if fi is not None and getattr(fi, "last_price", None) is not None:
            return float(fi.last_price)
        hist = ticker.history(period="1d")
        if not hist.empty:
            return float(hist["Close"].iloc[-1])
    except Exception as exc:
        print(f"yfinance price error for {symbol}: {exc}")
    return None


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda A – News Fetcher.

    Flow:
      1. Query DynamoDB for watchlist symbols.
      2. Fetch up to 10 recent news items for each symbol (yfinance layer).
      3. Publish NEW_NEWS messages to SystemOpsTopic (Q2 via SNS→SQS).
    """
    account_id = (event or {}).get("account_id") or _get_account_id()

    symbols = _get_watchlist_symbols(account_id)
    published = 0

    for symbol in symbols:
        # Update last_fetched_price once per symbol
        last_price = _get_last_price(symbol)
        if last_price is not None:
            table = dynamodb.Table(TABLE_NAME)
            table.update_item(
                Key={"PK": f"ACCOUNT#{account_id}", "SK": f"STOCK#{symbol}"},
                UpdateExpression="SET last_fetched_price = :p, updated_at = :u",
                ExpressionAttributeValues={
                    # DynamoDB expects Decimal, not float
                    ":p": Decimal(str(last_price)),
                    ":u": datetime.now(timezone.utc).isoformat(),
                },
            )

        for news in _fetch_news(symbol, max_items=10):
            # Deduplicate based on symbol + headline + published_at.
            if not _mark_news_seen(
                account_id, news["symbol"], news["headline"], news["published_at"]
            ):
                continue
            msg = {
                "type": "NEW_NEWS",
                "account_id": account_id,
                "symbol": news["symbol"],
                "headline": news["headline"],
                "body": news["body"],
                "source": news["source"],
                "published_at": news["published_at"],
            }
            sns.publish(
                TopicArn=SYSTEM_OPS_TOPIC_ARN,
                Message=json.dumps(msg),
            )
            published += 1

    return {
        "statusCode": 200,
        "body": json.dumps({"account_id": account_id, "symbols": symbols, "messages_published": published}),
    }



