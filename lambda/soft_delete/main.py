"""Lambda E - Soft Delete / Cleanup.

Consumes ``MARK_STOCK_DELETED`` / ``CLEANUP_STOCK`` from ``CleanupQueue``:

1. mark the watchlist item ``is_deleted = true`` (it is kept, so history stays queryable),
2. apply the news retention rules for that symbol (tag > 3 weeks, delete > 6 weeks),
3. record how many news objects were affected in ``deleted_news_count``.

Idempotent: the counter is only increased on the first transition to deleted, and cleanup
itself is naturally repeatable. Unknown symbols are ignored instead of creating phantom items.
"""

from __future__ import annotations

import logging
from typing import Any

from botocore.exceptions import ClientError

from common import aws, ddb, keys, messages, retention, timeutil

logger = logging.getLogger(__name__)


def _soft_delete(msg: dict[str, Any], record: Any) -> None:
    if msg.get("type") not in {messages.MARK_STOCK_DELETED, messages.CLEANUP_STOCK}:
        logger.warning("ignoring unexpected message type %r", msg.get("type"))
        return

    account_id = str(msg["account_id"])
    symbol = messages.normalize_symbol(msg.get("symbol"))
    key = {"PK": keys.account_pk(account_id), "SK": keys.stock_sk(symbol)}

    counts = retention.cleanup_news_for_symbol(aws.require_env("BUCKET_NAME"), symbol)
    affected = counts["soft_tagged"] + counts["hard_deleted"]

    table = aws.table()
    try:
        # First transition to deleted: also record the cleanup effort.
        table.update_item(
            Key=key,
            UpdateExpression=(
                "SET is_deleted = :true, updated_at = :now, "
                "deleted_news_count = if_not_exists(deleted_news_count, :zero) + :affected"
            ),
            ConditionExpression=(
                "attribute_exists(PK) AND (attribute_not_exists(is_deleted) OR is_deleted = :false)"
            ),
            ExpressionAttributeValues={
                ":true": True,
                ":false": False,
                ":zero": 0,
                ":affected": affected,
                ":now": timeutil.iso_utc(),
            },
        )
    except ClientError as exc:
        if not ddb.is_conditional_failure(exc):
            raise
        # Already deleted (redelivery / second request) or never existed: nothing else to do.
        logger.info("%s already deleted or unknown for %s", symbol, account_id)


def handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    return messages.process_sqs_batch(event, _soft_delete)
