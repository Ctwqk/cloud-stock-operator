import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

import boto3


TABLE_NAME = os.environ.get("TABLE_NAME", "")
BUCKET_NAME = os.environ.get("BUCKET_NAME", "")

dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")


def _sqs_record_to_message(record: Dict[str, Any]) -> Dict[str, Any]:
    body_str = record.get("body", "{}")
    try:
        outer = json.loads(body_str)
        inner_str = outer.get("Message", "{}")
        return json.loads(inner_str)
    except json.JSONDecodeError:
        return {}


def _cleanup_news_for_symbol(symbol: str) -> Dict[str, int]:
    """
    For all news/<symbol>/<yyyy-mm-dd>/... objects:
      - If older than 6 weeks: delete the object from S3.
      - Else if older than 3 weeks: tag with deleted=true.

    Returns counts: {"soft_tagged": x, "hard_deleted": y}.
    """
    results = {"soft_tagged": 0, "hard_deleted": 0}

    if not BUCKET_NAME:
        return results

    now = datetime.now(timezone.utc).date()
    three_weeks_ago = now - timedelta(weeks=3)
    six_weeks_ago = now - timedelta(weeks=6)

    prefix = f"news/{symbol}/"
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            # Expect key format: news/<symbol>/<yyyy-mm-dd>/<news_id>.json
            parts = key.split("/")
            if len(parts) < 3:
                continue
            date_str = parts[2]
            try:
                obj_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError:
                # If we can't parse the date, just tag it as deleted.
                obj_date = None

            try:
                if obj_date is not None and obj_date <= six_weeks_ago:
                    s3.delete_object(Bucket=BUCKET_NAME, Key=key)
                    results["hard_deleted"] += 1
                elif obj_date is None or obj_date <= three_weeks_ago:
                    s3.put_object_tagging(
                        Bucket=BUCKET_NAME,
                        Key=key,
                        Tagging={
                            "TagSet": [
                                {"Key": "deleted", "Value": "true"},
                            ]
                        },
                    )
                    results["soft_tagged"] += 1
            except Exception as exc:  # best-effort cleanup
                print(f"Cleanup error for {key}: {exc}")

    return results


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda E – Soft Delete / Cleanup.

    Trigger:
      - Q2 (SystemOpsQueue) messages where type in {MARK_STOCK_DELETED, CLEANUP_STOCK}.

    Flow:
      1. Update DynamoDB watchlist item: set is_deleted = true, updated_at.
      2. For related S3 news objects under news/<symbol>/...:
         - Tag ones older than 3 weeks as logically deleted (deleted=true).
         - Physically delete ones older than 6 weeks.
      3. Maintain a per-stock counter `deleted_news_count` of affected news items.
    """
    table = dynamodb.Table(TABLE_NAME)
    updated = 0
    soft_tagged_total = 0
    hard_deleted_total = 0

    for record in event.get("Records", []):
        msg = _sqs_record_to_message(record)
        msg_type = msg.get("type")
        if msg_type not in {"MARK_STOCK_DELETED", "CLEANUP_STOCK"}:
            continue

        account_id = msg["account_id"]
        symbol = msg["symbol"]
        pk = f"ACCOUNT#{account_id}"
        sk = f"STOCK#{symbol}"

        cleanup_counts = _cleanup_news_for_symbol(symbol)
        affected = cleanup_counts["soft_tagged"] + cleanup_counts["hard_deleted"]

        table.update_item(
            Key={"PK": pk, "SK": sk},
            UpdateExpression=(
                "SET is_deleted = :true, "
                "updated_at = :u, "
                "deleted_news_count = if_not_exists(deleted_news_count, :zero) + :inc"
            ),
            ExpressionAttributeValues={
                ":true": True,
                ":u": datetime.now(timezone.utc).isoformat(),
                ":zero": 0,
                ":inc": affected,
            },
        )
        updated += 1
        soft_tagged_total += cleanup_counts["soft_tagged"]
        hard_deleted_total += cleanup_counts["hard_deleted"]

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "items_marked_deleted": updated,
                "news_soft_tagged_deleted": soft_tagged_total,
                "news_hard_deleted": hard_deleted_total,
            }
        ),
    }


