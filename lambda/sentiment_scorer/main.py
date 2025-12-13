import json
import os
import random
from datetime import datetime, timezone
from typing import Any, Dict

import boto3


TABLE_NAME = os.environ.get("TABLE_NAME", "")
SYSTEM_OPS_TOPIC_ARN = os.environ.get("SYSTEM_OPS_TOPIC_ARN", "")

dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")
sts = boto3.client("sts")

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


def _parse_s3_event_record(record: Dict[str, Any]) -> Dict[str, str]:
    s3_info = record.get("s3", {})
    bucket = s3_info.get("bucket", {}).get("name")
    key = s3_info.get("object", {}).get("key")
    return {"bucket": bucket, "key": key}


def _load_news_object(bucket: str, key: str) -> Dict[str, Any]:
    s3 = boto3.client("s3")
    resp = s3.get_object(Bucket=bucket, Key=key)
    body = resp["Body"].read().decode("utf-8")
    return json.loads(body)


def _sample_random_sentiment() -> int:
    """
    Random sentiment r ∈ {+1, 0, -1}.
    """
    return random.choice([1, 0, -1])


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda B – Sentiment Scoring (Random).

    Trigger:
      - S3 ObjectCreated events on bucket S for news/ prefix.

    Flow per news item:
      1. Sample random sentiment r in {+1, 0, -1}.
      2. Read watchlist entry: PK=ACCOUNT#<account_id>, SK=STOCK#<symbol>.
      3. Update current_level_score += r, set threshold_abs=1 if missing.
      4. If |score| >= threshold_abs, publish AUTO_TRADE_DECISION to SystemOpsTopic.
    """
    table = dynamodb.Table(TABLE_NAME)
    decisions = 0

    for record in event.get("Records", []):
        s3_info = _parse_s3_event_record(record)
        if not s3_info["bucket"] or not s3_info["key"]:
            continue

        news = _load_news_object(s3_info["bucket"], s3_info["key"])
        account_id = news.get("account_id") or _get_account_id()
        symbol = news.get("symbol", "UNKNOWN")

        r = _sample_random_sentiment()

        pk = f"ACCOUNT#{account_id}"
        sk = f"STOCK#{symbol}"

        # Get current watchlist entry (if any)
        resp = table.get_item(Key={"PK": pk, "SK": sk})
        item = resp.get("Item") or {}
        current_score = int(item.get("current_level_score", 0))
        # Default absolute threshold is 1 (i.e., any non-zero score can trigger a decision)
        threshold_abs = int(item.get("threshold_abs", 1))

        new_score = current_score + r

        # Write back updated score
        table.update_item(
            Key={"PK": pk, "SK": sk},
            UpdateExpression="SET current_level_score = :s, threshold_abs = :t, updated_at = :u",
            ExpressionAttributeValues={
                ":s": new_score,
                ":t": threshold_abs,
                ":u": datetime.now(timezone.utc).isoformat(),
            },
        )

        # Threshold checks
        action = None
        if new_score >= threshold_abs:
            action = "BUY"
        elif new_score <= -threshold_abs:
            action = "SELL"

        if action:
            decision = {
                "type": "AUTO_TRADE_DECISION",
                "account_id": account_id,
                "symbol": symbol,
                "action": action,
                "score": new_score,
                "threshold_abs": threshold_abs,
                "reason": "SCORE_THRESHOLD",
            }
            sns.publish(TopicArn=SYSTEM_OPS_TOPIC_ARN, Message=json.dumps(decision))
            decisions += 1

    return {
        "statusCode": 200,
        "body": json.dumps({"decisions_emitted": decisions}),
    }



