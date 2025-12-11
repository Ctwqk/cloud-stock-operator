import json
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List

import boto3


TABLE_NAME = os.environ.get("TABLE_NAME", "")
EXTERNAL_OPS_TOPIC_ARN = os.environ.get("EXTERNAL_OPS_TOPIC_ARN", "")
SYSTEM_OPS_TOPIC_ARN = os.environ.get("SYSTEM_OPS_TOPIC_ARN", "")
NEWS_FETCHER_NAME = os.environ.get("NEWS_FETCHER_NAME", "")

dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")
sts = boto3.client("sts")
lambda_client = boto3.client("lambda")

_ACCOUNT_ID_CACHE: str | None = None


def _get_account_id() -> str:
    """
    Resolve AWS account id from STS once per container.
    """
    global _ACCOUNT_ID_CACHE
    if _ACCOUNT_ID_CACHE is None:
        identity = sts.get_caller_identity()
        _ACCOUNT_ID_CACHE = identity["Account"]
    return _ACCOUNT_ID_CACHE


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _response(status_code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def _canonical_path(event: Dict[str, Any]) -> str:
    """
    Strip API Gateway stage prefix so we can match on logical paths like
    /watchlist, /account/cash/adjust, etc.
    """
    raw_path = event.get("path", "/") or "/"
    stage = (event.get("requestContext") or {}).get("stage")

    segments = raw_path.strip("/").split("/")
    if stage and segments and segments[0] == stage:
        segments = segments[1:]

    return "/" + "/".join(segments) if segments else "/"


def _publish_external(event: Dict[str, Any]) -> None:
    """
    Optionally publish a log/audit event to ExternalOpsTopic (SNS → Q1).
    """
    if not EXTERNAL_OPS_TOPIC_ARN:
        return
    try:
        sns.publish(TopicArn=EXTERNAL_OPS_TOPIC_ARN, Message=json.dumps(event))
    except Exception as exc:  # best-effort logging
        print(f"Failed to publish external ops event: {exc}")


def _handle_add_watchlist(account_id: str, body: Dict[str, Any]) -> Dict[str, Any]:
    symbol = str(body.get("symbol", "")).upper()
    if not symbol:
        return _response(400, {"error": "symbol is required"})

    shares_managed = int(body.get("shares_managed", 0))
    now = _now_iso()
    pk = f"ACCOUNT#{account_id}"
    sk = f"STOCK#{symbol}"

    table = dynamodb.Table(TABLE_NAME)
    table.put_item(
        Item={
            "PK": pk,
            "SK": sk,
            "symbol": symbol,
            "shares_managed": shares_managed,
            "current_level_score": 0,
            "threshold_abs": 3,
            "is_deleted": False,
            "created_at": now,
            "updated_at": now,
        }
    )

    _publish_external(
        {
            "type": "ADD_STOCK",
            "account_id": account_id,
            "symbol": symbol,
            "initial_shares_managed": shares_managed,
        }
    )

    # Kick off an immediate news fetch so the new stock gets an initial score
    # from recent history (NewsFetcherLambda will also deduplicate).
    if NEWS_FETCHER_NAME:
        try:
            lambda_client.invoke(
                FunctionName=NEWS_FETCHER_NAME,
                InvocationType="Event",
                Payload=json.dumps({"account_id": account_id}).encode("utf-8"),
            )
        except Exception as exc:
            print(f"Failed to invoke news fetcher for {symbol}: {exc}")

    return _response(200, {"message": "stock added", "symbol": symbol})


def _handle_delete_watchlist(account_id: str, symbol: str) -> Dict[str, Any]:
    symbol = symbol.upper()
    # Use soft-delete Lambda via SystemOpsTopic → Q2 instead of updating the
    # item directly here, so all cleanup logic is centralized.
    if SYSTEM_OPS_TOPIC_ARN:
        sns.publish(
            TopicArn=SYSTEM_OPS_TOPIC_ARN,
            Message=json.dumps(
                {
                    "type": "MARK_STOCK_DELETED",
                    "account_id": account_id,
                    "symbol": symbol,
                }
            ),
        )

    _publish_external(
        {
            "type": "DELETE_STOCK",
            "account_id": account_id,
            "symbol": symbol,
        }
    )

    return _response(200, {"message": "stock deleted", "symbol": symbol})


def _handle_adjust_cash(account_id: str, body: Dict[str, Any]) -> Dict[str, Any]:
    if "delta_cash" not in body or "max_allowed_cash" not in body:
        return _response(
            400, {"error": "delta_cash and max_allowed_cash are required"}
        )

    # Convert to Decimal so DynamoDB accepts numeric types
    delta_cash = Decimal(str(body["delta_cash"]))
    max_allowed_cash = Decimal(str(body["max_allowed_cash"]))

    pk = f"ACCOUNT#{account_id}"
    sk = "SUMMARY"

    table = dynamodb.Table(TABLE_NAME)
    table.update_item(
        Key={"PK": pk, "SK": sk},
        UpdateExpression=(
            "SET current_cash_managed = if_not_exists(current_cash_managed, :zero) + :delta, "
            "updated_at = :u"
        ),
        ConditionExpression=(
            "if_not_exists(current_cash_managed, :zero) + :delta <= :max"
        ),
        ExpressionAttributeValues={
            ":delta": delta_cash,
            ":max": max_allowed_cash,
            ":zero": Decimal("0"),
            ":u": _now_iso(),
        },
    )

    _publish_external(
        {
            "type": "ADJUST_MANAGED_CASH",
            "account_id": account_id,
            "delta_cash": delta_cash,
            "max_allowed_cash": max_allowed_cash,
        }
    )

    return _response(200, {"message": "cash adjusted"})


def _handle_adjust_shares(account_id: str, body: Dict[str, Any]) -> Dict[str, Any]:
    if "symbol" not in body or "delta_shares" not in body or "max_shares_allowed" not in body:
        return _response(
            400, {"error": "symbol, delta_shares, max_shares_allowed are required"}
        )

    symbol = str(body["symbol"]).upper()
    delta_shares = int(body["delta_shares"])
    max_shares_allowed = int(body["max_shares_allowed"])

    pk = f"ACCOUNT#{account_id}"
    sk = f"STOCK#{symbol}"

    table = dynamodb.Table(TABLE_NAME)
    table.update_item(
        Key={"PK": pk, "SK": sk},
        UpdateExpression=(
            "SET shares_managed = if_not_exists(shares_managed, :zero) + :delta, "
            "updated_at = :u"
        ),
        ConditionExpression=(
            "if_not_exists(shares_managed, :zero) + :delta <= :max"
        ),
        ExpressionAttributeValues={
            ":delta": delta_shares,
            ":max": max_shares_allowed,
            ":zero": 0,
            ":u": _now_iso(),
        },
    )

    _publish_external(
        {
            "type": "ADJUST_MANAGED_SHARES",
            "account_id": account_id,
            "symbol": symbol,
            "delta_shares": delta_shares,
            "max_shares_allowed": max_shares_allowed,
        }
    )

    return _response(200, {"message": "shares adjusted", "symbol": symbol})


def _handle_set_threshold(
    account_id: str, symbol: str, body: Dict[str, Any]
) -> Dict[str, Any]:
    if "threshold_abs" not in body:
        return _response(400, {"error": "threshold_abs is required"})

    threshold_abs = int(body["threshold_abs"])
    symbol = symbol.upper()
    pk = f"ACCOUNT#{account_id}"
    sk = f"STOCK#{symbol}"

    table = dynamodb.Table(TABLE_NAME)
    table.update_item(
        Key={"PK": pk, "SK": sk},
        UpdateExpression="SET threshold_abs = :t, updated_at = :u",
        ExpressionAttributeValues={
            ":t": threshold_abs,
            ":u": _now_iso(),
        },
    )

    _publish_external(
        {
            "type": "SET_THRESHOLD",
            "account_id": account_id,
            "symbol": symbol,
            "threshold_abs": threshold_abs,
        }
    )

    return _response(200, {"message": "threshold updated", "symbol": symbol})


def _handle_apply_sentiment(account_id: str, body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply a sentiment delta (e.g., from FinBERT running in the local backend).

    Expected body:
      {
        "symbol": "AAPL",
        "delta_score": 1,    # integer delta to add to current_level_score
        "threshold_abs": 3,  # optional, defaults to 3
        "reason": "FINBERT"  # optional
      }
    """
    symbol = str(body.get("symbol", "")).upper()
    if not symbol:
        return _response(400, {"error": "symbol is required"})

    if "delta_score" not in body:
        return _response(400, {"error": "delta_score is required"})

    delta_score = int(body["delta_score"])
    threshold_abs = int(body.get("threshold_abs", 3))
    reason = str(body.get("reason", "FINBERT"))

    pk = f"ACCOUNT#{account_id}"
    sk = f"STOCK#{symbol}"

    table = dynamodb.Table(TABLE_NAME)

    # Get current item
    resp = table.get_item(Key={"PK": pk, "SK": sk})
    item = resp.get("Item") or {}
    current_score = int(item.get("current_level_score", 0))
    existing_threshold = int(item.get("threshold_abs", threshold_abs))

    new_score = current_score + delta_score
    threshold_abs = existing_threshold or threshold_abs

    # Update item
    table.update_item(
        Key={"PK": pk, "SK": sk},
        UpdateExpression=(
            "SET current_level_score = :s, "
            "threshold_abs = :t, "
            "updated_at = :u"
        ),
        ExpressionAttributeValues={
            ":s": new_score,
            ":t": threshold_abs,
            ":u": _now_iso(),
        },
    )

    # Record score change history per stock
    ts = _now_iso()
    score_sk = f"SCORE#{symbol}#{ts}"
    table.put_item(
        Item={
            "PK": pk,
            "SK": score_sk,
            "symbol": symbol,
            "old_score": current_score,
            "delta_score": delta_score,
            "new_score": new_score,
            "threshold_abs": threshold_abs,
            "reason": reason,
            "created_at": ts,
        }
    )

    # Threshold checks
    action: str | None = None
    if new_score >= threshold_abs:
        action = "BUY"
    elif new_score <= -threshold_abs:
        action = "SELL"

    if action and SYSTEM_OPS_TOPIC_ARN:
        decision = {
            "type": "AUTO_TRADE_DECISION",
            "account_id": account_id,
            "symbol": symbol,
            "action": action,
            "score": new_score,
            "threshold_abs": threshold_abs,
            "reason": reason,
        }
        sns.publish(TopicArn=SYSTEM_OPS_TOPIC_ARN, Message=json.dumps(decision))

    return _response(
        200,
        {
            "message": "sentiment applied",
            "symbol": symbol,
            "new_score": new_score,
            "threshold_abs": threshold_abs,
            "action": action,
        },
    )


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Control-plane API Lambda.

    Endpoints (API Gateway Lambda proxy integration):
      - POST   /watchlist                 -> add stock to watchlist
      - DELETE /watchlist/{symbol}        -> soft-delete stock
      - POST   /account/cash/adjust       -> adjust current_cash_managed
      - POST   /account/shares/adjust     -> adjust shares_managed per symbol
      - POST   /watchlist/{symbol}/threshold -> set/update per-stock threshold
    """
    method = event.get("httpMethod", "")
    path = _canonical_path(event)

    try:
        body = json.loads(event.get("body") or "{}")
    except Exception:
        body = {}

    account_id = _get_account_id()

    if method == "POST" and path == "/watchlist":
        return _handle_add_watchlist(account_id, body)

    if method == "DELETE" and path.startswith("/watchlist/"):
        parts: List[str] = path.strip("/").split("/")
        symbol = parts[-1] if len(parts) >= 2 else ""
        return _handle_delete_watchlist(account_id, symbol)

    if method == "POST" and path == "/account/cash/adjust":
        return _handle_adjust_cash(account_id, body)

    if method == "POST" and path == "/account/shares/adjust":
        return _handle_adjust_shares(account_id, body)

    if method == "POST" and path.startswith("/watchlist/") and path.endswith(
        "/threshold"
    ):
        # /watchlist/{symbol}/threshold
        parts = path.strip("/").split("/")
        if len(parts) >= 3:
            symbol = parts[1]
            return _handle_set_threshold(account_id, symbol, body)

    if method == "POST" and path == "/sentiment":
        # Local backend (FinBERT) posts sentiment delta here.
        return _handle_apply_sentiment(account_id, body)

    return _response(
        404, {"error": f"Unhandled method/path: {method} {path}"}
    )



