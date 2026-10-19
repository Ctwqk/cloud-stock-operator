"""Lambda D - External Ops Handler (ExternalOpsQueue -> DynamoDB).

The control plane never mutates cash/share balances itself: it validates the request, publishes
a command to ``ExternalOpsTopic`` and returns ``202`` with an ``op_id``. This Lambda is the
single writer for those commands:

    ADD_STOCK               add (or re-activate) a watchlist entry
    ADJUST_MANAGED_CASH     add ``delta_cash`` to managed cash, bounded by 0 .. max_allowed_cash
    ADJUST_MANAGED_SHARES   add ``delta_shares`` to a stock's managed shares, bounded by 0 .. max

Guarantees
    * Exactly-once effect under at-least-once delivery: the state change and an ``OP#<op_id>``
      marker are written in one transaction, so a redelivered command is a no-op.
    * Balance changes are read-modify-write guarded by an optimistic condition on the value we
      read (DynamoDB condition expressions cannot do ``a + b <= c`` arithmetic, which is why the
      previous version of this handler could never apply a delta). A concurrent change makes the
      transaction fail and the message is retried.
    * Business rejections (limit exceeded, unknown symbol, ...) are permanent: they are stored
      as ``OP#<op_id>`` with ``status = REJECTED`` and a reason, never retried. The control
      plane exposes the outcome at ``GET /operations/{op_id}``.
    * Infrastructure failures are reported per message so that only the failed message is
      retried (and eventually dead-lettered).
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from botocore.exceptions import ClientError

from common import aws, ddb, keys, messages, portfolio, scoring, timeutil

logger = logging.getLogger(__name__)

OP_TTL_DAYS = 30


class Rejected(Exception):
    """The command is valid JSON but cannot be applied (permanent, do not retry)."""


def _op_marker(account_id: str, op_id: str, op_type: str, extra: dict[str, Any]) -> dict[str, Any]:
    now = timeutil.iso_utc()
    return {
        "PK": keys.account_pk(account_id),
        "SK": keys.op_sk(op_id),
        "op_id": op_id,
        "type": op_type,
        "status": "APPLIED",
        "created_at": now,
        keys.TTL_ATTRIBUTE: timeutil.epoch_in(days=OP_TTL_DAYS),
        **extra,
    }


def _apply_once(account_id: str, op_id: str, op_type: str, update: dict[str, Any]) -> bool:
    """Write the OP marker and the state ``update`` atomically.

    Returns ``True`` when applied, ``False`` when this op was already applied. Raises when the
    state changed under us (so the message is retried and re-evaluated against fresh data).
    """
    table_name = aws.table().name
    try:
        aws.ddb_client().transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "TableName": table_name,
                        "Item": _op_marker(account_id, op_id, op_type, {}),
                        "ConditionExpression": "attribute_not_exists(PK)",
                    }
                },
                {"Update": {"TableName": table_name, **update}},
            ]
        )
        return True
    except ClientError as exc:
        codes = ddb.cancellation_codes(exc)
        if codes[:1] == ["ConditionalCheckFailed"]:
            logger.info("op %s already applied; skipping", op_id)
            return False
        if codes[1:2] == ["ConditionalCheckFailed"]:
            raise RuntimeError(f"op {op_id}: state changed concurrently, will retry") from exc
        raise


def _record_rejection(account_id: str, op_id: str, op_type: str, reason: str) -> None:
    item = _op_marker(account_id, op_id, op_type, {"status": "REJECTED", "reason": reason})
    try:
        aws.table().put_item(Item=item, ConditionExpression="attribute_not_exists(PK)")
    except ClientError as exc:
        if not ddb.is_conditional_failure(exc):
            raise
    logger.warning("op %s (%s) rejected: %s", op_id, op_type, reason)


def _add_stock(account_id: str, op_id: str, msg: dict[str, Any]) -> None:
    symbol = messages.normalize_symbol(msg.get("symbol"))
    shares = ddb.to_int(msg.get("initial_shares_managed", 0), "initial_shares_managed")
    if shares < 0:
        raise Rejected("initial_shares_managed must be >= 0")
    now = timeutil.iso_utc()
    _apply_once(
        account_id,
        op_id,
        messages.ADD_STOCK,
        {
            "Key": {"PK": keys.account_pk(account_id), "SK": keys.stock_sk(symbol)},
            # if_not_exists keeps score/threshold/shares when a deleted stock is re-added.
            "UpdateExpression": (
                "SET symbol = :symbol, "
                "shares_managed = if_not_exists(shares_managed, :shares), "
                "current_level_score = if_not_exists(current_level_score, :zero), "
                "threshold_abs = if_not_exists(threshold_abs, :threshold), "
                "is_deleted = :false, "
                "created_at = if_not_exists(created_at, :now), updated_at = :now"
            ),
            "ExpressionAttributeValues": {
                ":symbol": symbol,
                ":shares": shares,
                ":zero": 0,
                ":threshold": scoring.DEFAULT_THRESHOLD_ABS,
                ":false": False,
                ":now": now,
            },
        },
    )
    # Published even for duplicates: the fetch it triggers is idempotent, and re-publishing
    # recovers from a crash between the transaction and this call.
    messages.publish(
        aws.require_env("SYSTEM_OPS_TOPIC_ARN"),
        {"type": messages.STOCK_ADDED, "account_id": account_id, "symbol": symbol},
    )


def _adjust_cash(account_id: str, op_id: str, msg: dict[str, Any]) -> None:
    delta = ddb.to_decimal(msg.get("delta_cash"), "delta_cash")
    maximum = ddb.to_decimal(msg.get("max_allowed_cash"), "max_allowed_cash")
    key = {"PK": keys.account_pk(account_id), "SK": keys.SUMMARY_SK}

    item = aws.table().get_item(Key=key, ConsistentRead=True).get("Item") or {}
    current = item.get("current_cash_managed")
    new_value = (current if current is not None else Decimal("0")) + delta
    if new_value < 0:
        raise Rejected("managed cash cannot go below zero")
    if new_value > maximum:
        raise Rejected("managed cash would exceed the allowed maximum")

    values: dict[str, Any] = {":new": new_value, ":now": timeutil.iso_utc()}
    if current is None:
        condition = "attribute_not_exists(current_cash_managed)"
    else:
        condition = "current_cash_managed = :current"
        values[":current"] = current
    applied = _apply_once(
        account_id,
        op_id,
        messages.ADJUST_MANAGED_CASH,
        {
            "Key": key,
            "UpdateExpression": "SET current_cash_managed = :new, updated_at = :now",
            "ConditionExpression": condition,
            "ExpressionAttributeValues": values,
        },
    )
    if applied:
        portfolio.refresh_summary(account_id)


def _adjust_shares(account_id: str, op_id: str, msg: dict[str, Any]) -> None:
    symbol = messages.normalize_symbol(msg.get("symbol"))
    delta = ddb.to_int(msg.get("delta_shares"), "delta_shares")
    maximum = ddb.to_int(msg.get("max_shares_allowed"), "max_shares_allowed")
    key = {"PK": keys.account_pk(account_id), "SK": keys.stock_sk(symbol)}

    item = aws.table().get_item(Key=key, ConsistentRead=True).get("Item")
    if not item or item.get("is_deleted"):
        raise Rejected(f"{symbol} is not on the watchlist")
    current = item.get("shares_managed")
    new_value = ddb.to_int(current if current is not None else 0) + delta
    if new_value < 0:
        raise Rejected("managed shares cannot go below zero")
    if new_value > maximum:
        raise Rejected("managed shares would exceed the allowed maximum")

    values: dict[str, Any] = {":new": new_value, ":now": timeutil.iso_utc()}
    if current is None:
        condition = "attribute_exists(PK) AND attribute_not_exists(shares_managed)"
    else:
        condition = "attribute_exists(PK) AND shares_managed = :current"
        values[":current"] = current
    applied = _apply_once(
        account_id,
        op_id,
        messages.ADJUST_MANAGED_SHARES,
        {
            "Key": key,
            "UpdateExpression": "SET shares_managed = :new, updated_at = :now",
            "ConditionExpression": condition,
            "ExpressionAttributeValues": values,
        },
    )
    if applied:
        portfolio.refresh_summary(account_id)


_COMMANDS = {
    messages.ADD_STOCK: _add_stock,
    messages.ADJUST_MANAGED_CASH: _adjust_cash,
    messages.ADJUST_MANAGED_SHARES: _adjust_shares,
}


def _handle(msg: dict[str, Any], record: Any) -> None:
    op_type = msg.get("type")
    command = _COMMANDS.get(op_type)
    if command is None:
        logger.warning("ignoring unsupported message type %r", op_type)
        return
    account_id = msg.get("account_id")
    if not account_id:
        logger.error("dropping %s without account_id", op_type)
        return
    account_id = str(account_id)
    # Commands from the control plane carry an op_id; fall back to the SQS message id, which is
    # stable across redeliveries of the same message.
    op_id = str(msg.get("op_id") or f"msg-{record['messageId']}")
    try:
        command(account_id, op_id, msg)
    except (Rejected, ValueError) as exc:
        _record_rejection(account_id, op_id, str(op_type), str(exc))


def handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    return messages.process_sqs_batch(event, _handle)
