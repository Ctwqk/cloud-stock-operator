"""Lambda B - Sentiment Scorer.

Consumes ``NEWS_STORED`` from ``SentimentQueue``:

1. load the stored news JSON from S3,
2. score headline + body with the configured :class:`common.sentiment.SentimentScorer`
   (VADER + finance lexicon by default; deterministic),
3. add the +1/0/-1 delta to the stock's ``current_level_score`` and, when the stock's threshold
   is reached, publish an ``AUTO_TRADE_DECISION`` (see :mod:`common.scoring`).

The news id is the idempotency key: redelivered messages never score the same article twice.
"""

from __future__ import annotations

import logging
from typing import Any

from common import aws, jsonutil, messages, scoring, sentiment

logger = logging.getLogger(__name__)


def _score_news(msg: dict[str, Any], record: Any) -> None:
    if msg.get("type") != messages.NEWS_STORED:
        logger.warning("ignoring unexpected message type %r", msg.get("type"))
        return

    bucket = aws.require_env("BUCKET_NAME")
    key = msg["key"]
    body = aws.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
    article = jsonutil.loads(body)

    account_id = article.get("account_id") or msg.get("account_id") or aws.get_account_id()
    symbol = messages.normalize_symbol(article.get("symbol") or msg.get("symbol"))
    result = sentiment.get_scorer().score(article.get("headline", ""), article.get("body", ""))

    outcome = scoring.apply_sentiment(
        account_id=account_id,
        symbol=symbol,
        delta=result.delta,
        reason=f"SENTIMENT:{result.backend}",
        dedupe_id=str(msg.get("news_id") or article.get("news_id") or key),
        topic_arn=aws.require_env("SYSTEM_OPS_TOPIC_ARN"),
        headline=article.get("headline", ""),
    )
    logger.info(
        "scored %s: %s (%.3f) -> %s, score=%s action=%s",
        key,
        result.label,
        result.compound,
        outcome.status,
        outcome.score,
        outcome.action,
    )


def handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    return messages.process_sqs_batch(event, _score_news)
