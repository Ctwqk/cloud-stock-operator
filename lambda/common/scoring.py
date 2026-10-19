"""Apply sentiment once and persist immutable trade decisions before publishing.

The SCORED marker, score/history update and decision counter are one DynamoDB transaction.
The marker also stores the decision payload (a small transactional outbox). If SNS fails,
redelivery publishes that exact payload, even if later news has changed the stock's score.
Concurrent score updates use optimistic conditions; duplicate publishes are safe because the
trade worker applies each decision_id once. Exhausted delivery retries go to the queue's DLQ.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
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
    marker_key = {"PK": pk, "SK": keys.scored_sk(dedupe_id)}

    for _ in range(4):
        previous = table.get_item(Key=marker_key, ConsistentRead=True).get("Item")
        if previous:
            return _deliver(previous, topic_arn, "duplicate")
        item = table.get_item(Key=stock_key, ConsistentRead=True).get("Item")
        if not item or item.get("is_deleted"):
            logger.warning("sentiment for unknown or deleted symbol %s ignored", symbol)
            return ScoreResult(status="unknown_symbol", symbol=symbol)

        score = ddb.to_int(item.get("current_level_score", 0)) + delta
        threshold = abs(ddb.to_int(item.get("threshold_abs", 1))) or DEFAULT_THRESHOLD_ABS
        count = ddb.to_int(item.get("decision_count", 0))
        action = "BUY" if score >= threshold else "SELL" if score <= -threshold else None
        now = timeutil.iso_utc()
        marker: dict[str, Any] = {
            **marker_key,
            "symbol": symbol,
            "delta": delta,
            "score": score,
            "threshold_abs": threshold,
            "created_at": now,
            keys.TTL_ATTRIBUTE: timeutil.epoch_in(days=MARKER_TTL_DAYS),
        }
        if action:
            marker["decision"] = {
                "type": messages.AUTO_TRADE_DECISION,
                "decision_id": f"{account_id}:{symbol}:{count + 1}",
                "account_id": account_id,
                "symbol": symbol,
                "action": action,
                "score": score,
                "threshold_abs": threshold,
                "reason": reason,
            }

        values: dict[str, Any] = {
            ":next_score": 0 if action else score,
            ":next_count": count + bool(action),
            ":delta": delta,
            ":headline": headline[:300],
            ":now": now,
            ":false": False,
        }
        conditions = [
            "attribute_exists(PK)",
            "(attribute_not_exists(is_deleted) OR is_deleted = :false)",
        ]
        # Guard every input to the decision, including a concurrent threshold change.
        for attr in ("current_level_score", "threshold_abs", "decision_count"):
            if attr in item:
                conditions.append(f"{attr} = :old_{attr}")
                values[f":old_{attr}"] = item[attr]
            else:
                conditions.append(f"attribute_not_exists({attr})")
        try:
            aws.ddb_client().transact_write_items(
                TransactItems=[
                    {
                        "Put": {
                            "TableName": table.name,
                            "Item": marker,
                            "ConditionExpression": "attribute_not_exists(PK)",
                        }
                    },
                    {
                        "Update": {
                            "TableName": table.name,
                            "Key": stock_key,
                            "UpdateExpression": (
                                "SET current_level_score = :next_score, "
                                "decision_count = :next_count, last_sentiment = :delta, "
                                "last_headline = :headline, updated_at = :now"
                            ),
                            "ConditionExpression": " AND ".join(conditions),
                            "ExpressionAttributeValues": values,
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
            if "ConditionalCheckFailed" in ddb.cancellation_codes(exc)[:2]:
                continue  # reread a competing marker or changed stock, then reevaluate
            raise
        return _deliver(marker, topic_arn, "applied")
    raise RuntimeError(f"concurrent sentiment updates for {symbol}; retry this event")


def _deliver(marker: dict[str, Any], topic_arn: str, status: str) -> ScoreResult:
    """Replay only the recorded decision; never reevaluate a duplicate against newer news."""
    decision = marker.get("decision") or {}
    if decision:
        messages.publish(topic_arn, decision)
    return ScoreResult(
        status=status,
        symbol=marker["symbol"],
        delta=ddb.to_int(marker.get("delta", 0)),
        score=ddb.to_int(marker.get("score", 0)),
        threshold_abs=ddb.to_int(marker.get("threshold_abs", DEFAULT_THRESHOLD_ABS)),
        action=decision.get("action"),
        decision_id=decision.get("decision_id"),
    )
