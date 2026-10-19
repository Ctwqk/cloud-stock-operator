"""Control-plane API (API Gateway REST, Lambda proxy integration).

    POST   /watchlist                      add a stock                         -> 202 {op_id}
    DELETE /watchlist/{symbol}             soft-delete a stock                 -> 202
    POST   /watchlist/refresh              fetch prices + news now             -> 202
    POST   /watchlist/{symbol}/threshold   set the per-stock trade threshold   -> 200
    POST   /account/cash/adjust            change managed cash                 -> 202 {op_id}
    POST   /account/shares/adjust          change managed shares of a stock    -> 202 {op_id}
    POST   /sentiment                      apply an externally computed score  -> 200
    GET    /operations/{op_id}             outcome of an asynchronous command  -> 200

Two kinds of operations:

* **Commands that move balances** (add stock, cash, shares) are validated here, published to
  ``ExternalOpsTopic`` and applied by the ``stock_info_writer`` Lambda, the only writer of those
  fields. They return ``202`` plus an ``op_id``; poll ``GET /operations/{op_id}`` for the result.
* Threshold settings and sentiment deltas are applied synchronously. Sentiment callers
  must provide a stable event_id for retry deduplication.
"""

from __future__ import annotations

import base64
import logging
import re
import uuid
from collections.abc import Callable
from typing import Any

from botocore.exceptions import ClientError

from common import aws, ddb, jsonutil, keys, messages, scoring, timeutil

logger = logging.getLogger(__name__)


class ApiError(Exception):
    def __init__(self, status: int, message: str, **extra: Any) -> None:
        super().__init__(message)
        self.status = status
        self.body = {"error": message, **extra}


def _response(status: int, body: dict[str, Any]) -> dict[str, Any]:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": jsonutil.dumps(body),
    }


def _canonical_path(event: dict[str, Any]) -> str:
    """Path without a leading stage segment (``/prod/watchlist`` -> ``/watchlist``)."""
    segments = [s for s in (event.get("path") or "/").split("/") if s]
    stage = (event.get("requestContext") or {}).get("stage")
    if stage and segments and segments[0] == stage:
        segments = segments[1:]
    return "/" + "/".join(segments)


def _parse_body(event: dict[str, Any]) -> dict[str, Any]:
    raw = event.get("body")
    if not raw:
        return {}
    if event.get("isBase64Encoded"):
        raw = base64.b64decode(raw).decode("utf-8")
    try:
        body = jsonutil.loads(raw)
    except ValueError:
        raise ApiError(400, "request body must be valid JSON") from None
    if not isinstance(body, dict):
        raise ApiError(400, "request body must be a JSON object")
    return body


def _symbol(value: Any) -> str:
    try:
        return messages.normalize_symbol(value)
    except ValueError:
        raise ApiError(400, f"invalid symbol: {value!r}") from None


def _require(body: dict[str, Any], *fields: str) -> None:
    missing = [f for f in fields if f not in body]
    if missing:
        raise ApiError(400, f"missing required field(s): {', '.join(missing)}")


def _number(body: dict[str, Any], field: str) -> Any:
    try:
        return ddb.to_decimal(body[field], field)
    except ValueError as exc:
        raise ApiError(400, str(exc)) from None


def _integer(body: dict[str, Any], field: str) -> int:
    try:
        return ddb.to_int(body[field], field)
    except ValueError as exc:
        raise ApiError(400, str(exc)) from None


def _queue_command(account_id: str, command: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    op_id = uuid.uuid4().hex
    messages.publish(
        aws.require_env("EXTERNAL_OPS_TOPIC_ARN"),
        {**command, "op_id": op_id, "account_id": account_id},
    )
    return 202, {"message": "accepted", "op_id": op_id, "status": "PENDING"}


# --- route handlers: (account_id, body, path params) -> (status, body) --------------------


def add_watchlist(account_id: str, body: dict[str, Any], params: dict[str, str]):
    _require(body, "symbol")
    symbol = _symbol(body["symbol"])
    shares = _integer(body, "shares_managed") if "shares_managed" in body else 0
    if shares < 0:
        raise ApiError(400, "shares_managed must be >= 0")
    status, payload = _queue_command(
        account_id,
        {"type": messages.ADD_STOCK, "symbol": symbol, "initial_shares_managed": shares},
    )
    return status, {**payload, "symbol": symbol}


def delete_watchlist(account_id: str, body: dict[str, Any], params: dict[str, str]):
    symbol = _symbol(params["symbol"])
    messages.publish(
        aws.require_env("SYSTEM_OPS_TOPIC_ARN"),
        {"type": messages.MARK_STOCK_DELETED, "account_id": account_id, "symbol": symbol},
    )
    return 202, {"message": "delete accepted", "symbol": symbol}


def refresh_watchlist(account_id: str, body: dict[str, Any], params: dict[str, str]):
    function_name = aws.require_env("NEWS_FETCHER_NAME")
    try:
        aws.client("lambda").invoke(
            FunctionName=function_name,
            InvocationType="Event",
            Payload=jsonutil.dumps({"account_id": account_id}).encode("utf-8"),
        )
    except ClientError:
        logger.exception("failed to invoke news fetcher")
        raise ApiError(502, "could not start the refresh") from None
    return 202, {"message": "refresh requested; prices and news will update shortly"}


def set_threshold(account_id: str, body: dict[str, Any], params: dict[str, str]):
    _require(body, "threshold_abs")
    symbol = _symbol(params["symbol"])
    threshold = _integer(body, "threshold_abs")
    if threshold < 1:
        raise ApiError(400, "threshold_abs must be >= 1")
    try:
        aws.table().update_item(
            Key={"PK": keys.account_pk(account_id), "SK": keys.stock_sk(symbol)},
            UpdateExpression="SET threshold_abs = :t, updated_at = :u",
            ConditionExpression="attribute_exists(PK)",
            ExpressionAttributeValues={":t": threshold, ":u": timeutil.iso_utc()},
        )
    except ClientError as exc:
        if ddb.is_conditional_failure(exc):
            raise ApiError(404, f"{symbol} is not on the watchlist") from None
        raise
    return 200, {"message": "threshold updated", "symbol": symbol, "threshold_abs": threshold}


def adjust_cash(account_id: str, body: dict[str, Any], params: dict[str, str]):
    _require(body, "delta_cash", "max_allowed_cash")
    status, payload = _queue_command(
        account_id,
        {
            "type": messages.ADJUST_MANAGED_CASH,
            "delta_cash": _number(body, "delta_cash"),
            "max_allowed_cash": _number(body, "max_allowed_cash"),
        },
    )
    return status, payload


def adjust_shares(account_id: str, body: dict[str, Any], params: dict[str, str]):
    _require(body, "symbol", "delta_shares", "max_shares_allowed")
    status, payload = _queue_command(
        account_id,
        {
            "type": messages.ADJUST_MANAGED_SHARES,
            "symbol": _symbol(body["symbol"]),
            "delta_shares": _integer(body, "delta_shares"),
            "max_shares_allowed": _integer(body, "max_shares_allowed"),
        },
    )
    return status, payload


def apply_sentiment(account_id: str, body: dict[str, Any], params: dict[str, str]):
    """An external scorer (e.g. a local FinBERT process) posts a delta for one stock."""
    _require(body, "symbol", "delta_score")
    symbol = _symbol(body["symbol"])
    delta = _integer(body, "delta_score")
    if not -10 <= delta <= 10:
        raise ApiError(400, "delta_score must be between -10 and 10")
    result = scoring.apply_sentiment(
        account_id=account_id,
        symbol=symbol,
        delta=delta,
        reason=str(body.get("reason", "EXTERNAL"))[:50],
        # Callers that may retry should pass a stable event_id to make the call idempotent.
        dedupe_id=f"api:{body.get('event_id') or uuid.uuid4().hex}",
        topic_arn=aws.require_env("SYSTEM_OPS_TOPIC_ARN"),
        headline=str(body.get("headline", "")),
    )
    if result.status == "unknown_symbol":
        raise ApiError(404, f"{symbol} is not on the watchlist")
    return 200, {
        "message": "sentiment applied" if result.status == "applied" else "duplicate ignored",
        "symbol": symbol,
        "new_score": result.score,
        "threshold_abs": result.threshold_abs,
        "action": result.action,
    }


def get_operation(account_id: str, body: dict[str, Any], params: dict[str, str]):
    op_id = params["op_id"]
    item = (
        aws.table()
        .get_item(
            Key={"PK": keys.account_pk(account_id), "SK": keys.op_sk(op_id)}, ConsistentRead=True
        )
        .get("Item")
    )
    if not item:
        # Not applied (yet): still queued, or unknown / expired.
        return 200, {"op_id": op_id, "status": "PENDING"}
    return 200, {
        "op_id": op_id,
        "status": item.get("status"),
        "type": item.get("type"),
        "reason": item.get("reason"),
    }


Route = tuple[str, "re.Pattern[str]", Callable[..., tuple[int, dict[str, Any]]]]
ROUTES: list[Route] = [
    ("POST", re.compile(r"^/watchlist$"), add_watchlist),
    ("POST", re.compile(r"^/watchlist/refresh$"), refresh_watchlist),
    ("DELETE", re.compile(r"^/watchlist/(?P<symbol>[^/]+)$"), delete_watchlist),
    ("POST", re.compile(r"^/watchlist/(?P<symbol>[^/]+)/threshold$"), set_threshold),
    ("POST", re.compile(r"^/account/cash/adjust$"), adjust_cash),
    ("POST", re.compile(r"^/account/shares/adjust$"), adjust_shares),
    ("POST", re.compile(r"^/sentiment$"), apply_sentiment),
    ("GET", re.compile(r"^/operations/(?P<op_id>[A-Za-z0-9_\-]+)$"), get_operation),
]


def handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    method = (event.get("httpMethod") or "").upper()
    path = _canonical_path(event)
    try:
        for route_method, pattern, func in ROUTES:
            match = pattern.match(path)
            if match and route_method == method:
                status, payload = func(aws.get_account_id(), _parse_body(event), match.groupdict())
                return _response(status, payload)
        raise ApiError(404, f"no route for {method} {path}")
    except ApiError as err:
        return _response(err.status, err.body)
    except Exception:
        logger.exception("unhandled error for %s %s", method, path)
        return _response(500, {"error": "internal error"})
