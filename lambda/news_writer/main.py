"""Lambda F - News Writer.

Consumes ``NEW_NEWS`` from ``NewsIngestQueue``, stores each item as a JSON object in S3 and
announces it with a ``NEWS_STORED`` message (which the sentiment scorer consumes).

Idempotent: the S3 key is derived from the news id (``news/<SYMBOL>/<day>/<news_id>.json``),
so a redelivered message overwrites the same object instead of creating a duplicate.
"""

from __future__ import annotations

import logging
from typing import Any

from common import aws, jsonutil, messages, news, timeutil

logger = logging.getLogger(__name__)


def _store(msg: dict[str, Any], record: Any) -> None:
    if msg.get("type") != messages.NEW_NEWS:
        logger.warning("ignoring unexpected message type %r", msg.get("type"))
        return

    bucket = aws.require_env("BUCKET_NAME")
    topic_arn = aws.require_env("SYSTEM_OPS_TOPIC_ARN")

    symbol = messages.normalize_symbol(msg.get("symbol"))
    headline = str(msg.get("headline", ""))
    published_at = timeutil.normalize_iso(msg.get("published_at")) or timeutil.iso_utc()
    news_id = msg.get("news_id") or news.news_id(symbol, headline, published_at)
    key = news.news_key(symbol, published_at, news_id)

    payload = {
        **msg,
        "symbol": symbol,
        "news_id": news_id,
        "published_at": published_at,
        "ingested_at": timeutil.iso_utc(),
    }
    aws.client("s3").put_object(
        Bucket=bucket,
        Key=key,
        Body=jsonutil.dumps(payload).encode("utf-8"),
        ContentType="application/json",
    )
    messages.publish(
        topic_arn,
        {
            "type": messages.NEWS_STORED,
            "news_id": news_id,
            "account_id": msg.get("account_id"),
            "symbol": symbol,
            "bucket": bucket,
            "key": key,
        },
    )


def handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    return messages.process_sqs_batch(event, _store)
