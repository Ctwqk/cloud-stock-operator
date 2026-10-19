"""Daily news retention cleaner (EventBridge).

For every soft-deleted stock, applies the retention rules to its news in S3 (tag objects older
than 3 weeks as ``deleted=true``, delete those older than 6 weeks). This catches news that
crossed an age threshold *after* the stock was deleted.
"""

from __future__ import annotations

import logging
from typing import Any

from common import aws, ddb, keys, retention

logger = logging.getLogger(__name__)


def handler(event: dict[str, Any] | None = None, context: Any = None) -> dict[str, Any]:
    bucket = aws.require_env("BUCKET_NAME")
    soft_tagged = hard_deleted = stocks = 0

    deleted_stocks = ddb.scan_all(
        aws.table(),
        FilterExpression="begins_with(SK, :prefix) AND is_deleted = :true",
        ExpressionAttributeValues={":prefix": keys.STOCK_PREFIX, ":true": True},
    )
    for item in deleted_stocks:
        symbol = keys.symbol_from_stock_sk(item["SK"])
        counts = retention.cleanup_news_for_symbol(bucket, symbol)
        soft_tagged += counts["soft_tagged"]
        hard_deleted += counts["hard_deleted"]
        stocks += 1

    logger.info("retention: %d stocks, %d tagged, %d deleted", stocks, soft_tagged, hard_deleted)
    return {"stocks": stocks, "soft_tagged": soft_tagged, "hard_deleted": hard_deleted}
