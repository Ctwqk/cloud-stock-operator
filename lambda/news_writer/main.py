import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict

import boto3


BUCKET_NAME = os.environ.get("BUCKET_NAME", "")
SYSTEM_OPS_TOPIC_ARN = os.environ.get("SYSTEM_OPS_TOPIC_ARN", "")

s3 = boto3.client("s3")
sns = boto3.client("sns")


def _sqs_record_to_message(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract the NEW_NEWS payload from an SQS record that came from SNS.
    SQS body looks like:
      {"Type":"Notification", "Message":"{...json...}", ...}
    """
    body_str = record.get("body", "{}")
    try:
        outer = json.loads(body_str)
        inner_str = outer.get("Message", "{}")
        return json.loads(inner_str)
    except json.JSONDecodeError:
        return {}


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda F – News Writer.

    Trigger:
      - SQS Q2 (SystemOpsQueue) subscribed to SystemOpsTopic.
      - Handles messages where type == \"NEW_NEWS\".

    Flow:
      1. Read SQS records.
      2. For each NEW_NEWS, compute S3 key: news/<symbol>/<yyyy-mm-dd>/<uuid>.json.
      3. Write JSON to S3 bucket S.
      4. Publish NEWS_STORED message (with S3 bucket/key) to SystemOpsTopic for external
         sentiment scoring (e.g., FinBERT in local backend).
    """
    written = 0

    for record in event.get("Records", []):
        msg = _sqs_record_to_message(record)
        if msg.get("type") != "NEW_NEWS":
            continue

        symbol = msg.get("symbol", "UNKNOWN")
        published_at = msg.get("published_at") or datetime.now(timezone.utc).isoformat()
        day = published_at[:10]  # yyyy-mm-dd
        news_id = str(uuid.uuid4())

        key = f"news/{symbol}/{day}/{news_id}.json"
        payload = {
            **msg,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=json.dumps(payload).encode("utf-8"),
            ContentType="application/json",
        )
        written += 1

        # Notify downstream (e.g., local FinBERT worker) that a news item is stored.
        if SYSTEM_OPS_TOPIC_ARN:
            sns.publish(
                TopicArn=SYSTEM_OPS_TOPIC_ARN,
                Message=json.dumps(
                    {
                        "type": "NEWS_STORED",
                        "account_id": msg.get("account_id"),
                        "symbol": symbol,
                        "bucket": BUCKET_NAME,
                        "key": key,
                    }
                ),
            )

    return {
        "statusCode": 200,
        "body": json.dumps({"objects_written": written}),
    }





