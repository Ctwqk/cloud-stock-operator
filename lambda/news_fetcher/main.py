"""Lambda A - News Fetcher.

Triggers
    * EventBridge schedule (and manual / control-plane invokes): refresh the whole watchlist.
      The event may carry ``{"account_id": ...}``; otherwise the AWS account id is used.
    * SQS ``FetchTriggerQueue``: ``STOCK_ADDED`` messages refresh just the new symbol so a
      freshly added stock gets prices and news immediately.

Per symbol it stores ``last_fetched_price`` on the watchlist item and publishes one ``NEW_NEWS``
message per *unseen* news item. Unseen = no ``NEWSHASH#`` marker yet. The marker is written
only **after** the publish succeeded, so a failed publish is retried by the next run; the
downstream consumers are idempotent, so a rare duplicate publish is harmless.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from botocore.exceptions import ClientError

from common import aws, ddb, keys, messages, news, timeutil

from . import providers

logger = logging.getLogger(__name__)

MAX_NEWS_PER_SYMBOL = 10
SEEN_TTL_DAYS = 45


def _watchlist_symbols(account_id: str) -> list[str]:
    items = ddb.query_all(
        aws.table(),
        KeyConditionExpression="PK = :pk AND begins_with(SK, :prefix)",
        FilterExpression="attribute_not_exists(is_deleted) OR is_deleted = :false",
        ExpressionAttributeValues={
            ":pk": keys.account_pk(account_id),
            ":prefix": keys.STOCK_PREFIX,
            ":false": False,
        },
    )
    return sorted({item["symbol"] for item in items if item.get("symbol")})


def _update_price(account_id: str, symbol: str, price: Decimal) -> None:
    try:
        aws.table().update_item(
            Key={"PK": keys.account_pk(account_id), "SK": keys.stock_sk(symbol)},
            UpdateExpression="SET last_fetched_price = :p, updated_at = :u",
            ConditionExpression="attribute_exists(PK)",  # never resurrect a removed item
            ExpressionAttributeValues={":p": price, ":u": timeutil.iso_utc()},
        )
    except ClientError as exc:
        if not ddb.is_conditional_failure(exc):
            raise
        logger.info("stock %s disappeared before price update", symbol)


def _seen_key(account_id: str, symbol: str, item_id: str) -> dict[str, str]:
    return {"PK": keys.account_pk(account_id), "SK": keys.newshash_sk(symbol, item_id)}


def _fetch_symbol(
    account_id: str, symbol: str, provider: providers.NewsProvider, topic_arn: str
) -> int:
    """Refresh one symbol; returns the number of NEW_NEWS messages published."""
    table = aws.table()

    price = provider.last_price(symbol)
    if price is not None:
        _update_price(account_id, symbol, price)

    published = 0
    seen_this_run: set[str] = set()
    for item in provider.fetch_news(symbol, MAX_NEWS_PER_SYMBOL):
        item_id = news.news_id(symbol, item.headline, item.published_at)
        if item_id in seen_this_run:
            continue
        seen_this_run.add(item_id)
        if table.get_item(Key=_seen_key(account_id, symbol, item_id)).get("Item"):
            continue

        messages.publish(
            topic_arn,
            {
                "type": messages.NEW_NEWS,
                "news_id": item_id,
                "account_id": account_id,
                "symbol": symbol,
                "headline": item.headline,
                "body": item.body,
                "source": item.source,
                "published_at": item.published_at,
            },
        )
        table.put_item(
            Item={
                **_seen_key(account_id, symbol, item_id),
                "symbol": symbol,
                "headline": item.headline[:300],
                "published_at": item.published_at,
                "created_at": timeutil.iso_utc(),
                keys.TTL_ATTRIBUTE: timeutil.epoch_in(days=SEEN_TTL_DAYS),
            }
        )
        published += 1
    return published


def run_fetch(account_id: str, symbols: list[str]) -> dict[str, Any]:
    topic_arn = aws.require_env("SYSTEM_OPS_TOPIC_ARN")
    provider = providers.get_provider()
    published = 0
    errors: dict[str, str] = {}
    for symbol in symbols:
        try:
            published += _fetch_symbol(account_id, symbol, provider, topic_arn)
        except Exception as exc:  # one bad symbol must not block the others
            logger.exception("fetch failed for %s", symbol)
            errors[symbol] = str(exc)
    return {
        "account_id": account_id,
        "symbols": symbols,
        "messages_published": published,
        "errors": errors,
    }


def _handle_stock_added(msg: dict[str, Any], record: Any) -> None:
    if msg.get("type") != messages.STOCK_ADDED:
        return
    account_id, symbol = msg["account_id"], messages.normalize_symbol(msg["symbol"])
    if symbol not in _watchlist_symbols(account_id):
        logger.info("STOCK_ADDED for %s but it is not on the active watchlist", symbol)
        return
    result = run_fetch(account_id, [symbol])
    if result["errors"]:
        raise RuntimeError(f"fetch failed: {result['errors']}")


def handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    event = event or {}
    if "Records" in event:  # SQS (STOCK_ADDED)
        return messages.process_sqs_batch(event, _handle_stock_added)

    account_id = event.get("account_id") or aws.get_account_id()
    result = run_fetch(account_id, _watchlist_symbols(account_id))
    if result["errors"]:
        # Surface partial failures in the Lambda Errors metric; the retry is idempotent.
        raise RuntimeError(f"news fetch failed for {sorted(result['errors'])}")
    return result
