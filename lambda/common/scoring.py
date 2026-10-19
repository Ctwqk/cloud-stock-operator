"""Apply a sentiment delta to a watchlist entry and emit trade decisions.

Used by the sentiment scorer Lambda (news -> delta) and by the control plane's
``POST /sentiment`` endpoint (an external scorer posts a delta).

Semantics
---------
* Each stock keeps a running ``current_level_score``. A news item adds ``delta`` (+1/0/-1).
* When ``|score| >= threshold_abs`` an ``AUTO_TRADE_DECISION`` (BUY if positive, SELL if
  negative) is published and the score is reset to 0: a decision *consumes* the accumulated
  signal, so neutral news can no longer re-trigger the same trade.
* Applying the same ``dedupe_id`` twice changes nothing (SQS is at-least-once): a marker item
  and the score update are written in one DynamoDB transaction.
* Decisions carry a deterministic ``decision_id`` (``<account>:<symbol>:<n>``), so a retry after
  a crash between "publish" and "reset score" produces an identical message that the trade
  worker ignores.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from botocore.exceptions import ClientError

from . import aws, ddb, keys, messages, timeutil

logger = logging.getLogger(__name__)

DEFAULT_THRESHOLD_ABS = 1
MARKER_TTL_DAYS = 45


@dataclass(frozen=True)
class ScoreResult:
    status: str  # "applied" | "duplicate" | "unknown_symbol"
    symbol: str
    delta: int = 0
    score: int = 0
    threshold_abs: int = DEFAULT_THRESHOLD_ABS
    action: str | None = None
    decision_id: str | None = None


def apply_sentiment(
    *,
    account_id: str,
    symbol: str,
    delta: int,
    reason: str,
    dedupe_id: str,
    topic_arn: str,
    headline: str = "",
) -> ScoreResult:
    table = aws.table()
    pk = keys.account_pk(account_id)
    stock_key = {"PK": pk, "SK": keys.stock_sk(symbol)}

    item = table.get_item(Key=stock_key, ConsistentRead=True).get("Item")
    if not item or item.get("is_deleted"):
        logger.warning("sentiment for unknown or deleted symbol %s ignored", symbol)
        return ScoreResult(status="unknown_symbol", symbol=symbol)

    now = timeutil.iso_utc()
    ttl = timeutil.epoch_in(days=MARKER_TTL_DAYS)
    status = "applied"
    try:
        aws.ddb_client().transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "TableName": table.name,
                        "Item": {
                            "PK": pk,
                            "SK": keys.scored_sk(dedupe_id),
                            "symbol": symbol,
                            "created_at": now,
                            keys.TTL_ATTRIBUTE: ttl,
                        },
                        "ConditionExpression": "attribute_not_exists(PK)",
                    }
                },
                {
                    "Update": {
                        "TableName": table.name,
                        "Key": stock_key,
                        "UpdateExpression": (
                            "SET current_level_score = "
                            "if_not_exists(current_level_score, :zero) + :delta, "
                            "last_sentiment = :delta, last_headline = :headline, updated_at = :now"
                        ),
                        "ConditionExpression": "attribute_exists(PK)",
                        "ExpressionAttributeValues": {
                            ":zero": 0,
                            ":delta": delta,
                            ":headline": headline[:300],
                            ":now": now,
                        },
                    }
                },
                {
                    "Put": {
                        "TableName": table.name,
                        "Item": {
                            "PK": pk,
                            "SK": keys.score_sk(symbol, now),
                            "symbol": symbol,
                            "delta_score": delta,
                            "reason": reason,
                            "headline": headline[:300],
                            "created_at": now,
                        },
                    }
                },
            ]
        )
    except ClientError as exc:
        codes = ddb.cancellation_codes(exc)
        if codes[:1] == ["ConditionalCheckFailed"]:
            status = "duplicate"
        elif codes[1:2] == ["ConditionalCheckFailed"]:
            # the stock vanished between our read and the write
            return ScoreResult(status="unknown_symbol", symbol=symbol)
        else:
            raise

    # Threshold evaluation is idempotent, so it also runs for duplicates (this completes a
    # decision whose publish step was interrupted on the first delivery).
    return _evaluate_threshold(
        pk=pk, stock_key=stock_key, topic_arn=topic_arn, reason=reason, delta=delta, status=status
    )


def _evaluate_threshold(
    *, pk: str, stock_key: dict[str, str], topic_arn: str, reason: str, delta: int, status: str
) -> ScoreResult:
    table = aws.table()
    item = table.get_item(Key=stock_key, ConsistentRead=True)["Item"]
    symbol = str(item["symbol"])
    score = ddb.to_int(item.get("current_level_score", 0))
    threshold = (
        ddb.to_int(item.get("threshold_abs", DEFAULT_THRESHOLD_ABS)) or DEFAULT_THRESHOLD_ABS
    )
    threshold = abs(threshold)
    decisions_made = ddb.to_int(item.get("decision_count", 0))

    action: str | None = None
    if score >= threshold:
        action = "BUY"
    elif score <= -threshold:
        action = "SELL"

    decision_id = None
    if action:
        account_id = pk.split("#", 1)[1]
        decision_id = f"{account_id}:{symbol}:{decisions_made + 1}"
        messages.publish(
            topic_arn,
            {
                "type": messages.AUTO_TRADE_DECISION,
                "decision_id": decision_id,
                "account_id": account_id,
                "symbol": symbol,
                "action": action,
                "score": score,
                "threshold_abs": threshold,
                "reason": reason,
            },
        )
        _consume_signal(table, stock_key, decisions_made, score)

    return ScoreResult(
        status=status,
        symbol=symbol,
        delta=delta,
        score=score,
        threshold_abs=threshold,
        action=action,
        decision_id=decision_id,
    )


def _consume_signal(table: Any, stock_key: dict[str, str], decisions_made: int, score: int) -> None:
    """Reset the score after a decision, but only if nobody else changed it meanwhile."""
    try:
        table.update_item(
            Key=stock_key,
            UpdateExpression=(
                "SET current_level_score = :zero, decision_count = :next, updated_at = :now"
            ),
            ConditionExpression=(
                "(attribute_not_exists(decision_count) OR decision_count = :seen) "
                "AND current_level_score = :score"
            ),
            ExpressionAttributeValues={
                ":zero": 0,
                ":next": decisions_made + 1,
                ":seen": decisions_made,
                ":score": Decimal(score),
                ":now": timeutil.iso_utc(),
            },
        )
    except ClientError as exc:
        if not ddb.is_conditional_failure(exc):
            raise
        logger.info("score changed concurrently; leaving it for the next evaluation")
