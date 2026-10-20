"""Lambda E - Soft Delete / Cleanup.

Consumes ``MARK_STOCK_DELETED`` / ``CLEANUP_STOCK`` from ``CleanupQueue``:

1. mark the watchlist item ``is_deleted = true`` (it is kept, so history stays queryable),
2. apply the news retention rules for that symbol (tag > 3 weeks, delete > 6 weeks),
3. record how many news objects were affected in ``deleted_news_count``.

The deletion and OP marker are atomic and precede S3 cleanup: an S3 failure must not leave
the stock eligible for trades. Redelivery never repeats the state transition after reactivation.
The counter records one completed cleanup pass per command (partial failures may undercount).
Unknown symbols are ignored without touching S3 or creating phantom items.
"""

from __future__ import annotations

import logging
from typing import Any

from botocore.exceptions import ClientError

from common import aws, ddb, keys, messages, retention, timeutil

logger = logging.getLogger(__name__)


def _mark_deleted(key: dict[str, str], op_id: str) -> dict[str, Any]:
    table = aws.table()
    now = timeutil.iso_utc()
    try:
        aws.ddb_client().transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "TableName": table.name,
                        "Item": {
                            "PK": key["PK"],
                            "SK": keys.op_sk(op_id),
                            "op_id": op_id,
                            "type": messages.MARK_STOCK_DELETED,
                            "status": "APPLIED",
                            "created_at": now,
                            keys.TTL_ATTRIBUTE: timeutil.epoch_in(days=30),
                        },
                        "ConditionExpression": "attribute_not_exists(PK)",
                    }
                },
                {
                    "Update": {
                        "TableName": table.name,
                        "Key": key,
                        "UpdateExpression": (
                            "SET is_deleted = :true, delete_op_id = :op, updated_at = :now"
                        ),
                        "ConditionExpression": "attribute_exists(PK)",
                        "ExpressionAttributeValues": {
                            ":true": True,
                            ":op": op_id,
                            ":now": now,
                        },
                    }
                },
            ]
        )
    except ClientError as exc:
        codes = ddb.cancellation_codes(exc)
        if codes[:1] == ["ConditionalCheckFailed"]:
            pass  # Already applied; do not mark a reactivated stock deleted again.
        elif codes[1:2] == ["ConditionalCheckFailed"]:
            return {}  # Unknown stock: the OP marker was rolled back too.
        else:
            raise
    return table.get_item(Key=key, ConsistentRead=True).get("Item", {})


def _soft_delete(msg: dict[str, Any], record: Any) -> None:
    if msg.get("type") not in {messages.MARK_STOCK_DELETED, messages.CLEANUP_STOCK}:
        logger.warning("ignoring unexpected message type %r", msg.get("type"))
        return

    account_id = str(msg["account_id"])
    symbol = messages.normalize_symbol(msg.get("symbol"))
    key = {"PK": keys.account_pk(account_id), "SK": keys.stock_sk(symbol)}
    op_id = str(msg.get("op_id") or f"delete-msg-{record['messageId']}")
    item = _mark_deleted(key, op_id)
    if (
        not item.get("is_deleted")
        or item.get("delete_op_id") != op_id
        or item.get("cleanup_op_id") == op_id
    ):
        return  # Unknown, reactivated, superseded by another delete, or already cleaned.

    counts = retention.cleanup_news_for_symbol(aws.require_env("BUCKET_NAME"), symbol)
    affected = counts["soft_tagged"] + counts["hard_deleted"]

    table = aws.table()
    try:
        # Count only once, and never overwrite a concurrent reactivation/newer deletion.
        table.update_item(
            Key=key,
            UpdateExpression=(
                "SET cleanup_op_id = :op, "
                "deleted_news_count = if_not_exists(deleted_news_count, :zero) + :affected"
            ),
            ConditionExpression=(
                "attribute_exists(PK) AND is_deleted = :true AND delete_op_id = :op "
                "AND (attribute_not_exists(cleanup_op_id) OR cleanup_op_id <> :op)"
            ),
            ExpressionAttributeValues={
                ":true": True,
                ":op": op_id,
                ":zero": 0,
                ":affected": affected,
            },
        )
    except ClientError as exc:
        if not ddb.is_conditional_failure(exc):
            raise
        logger.info("%s cleanup was already counted or superseded for %s", symbol, account_id)


def handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    return messages.process_sqs_batch(event, _soft_delete)
