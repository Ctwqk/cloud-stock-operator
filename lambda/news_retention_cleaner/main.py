import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

import boto3


TABLE_NAME = os.environ.get("TABLE_NAME", "")
BUCKET_NAME = os.environ.get("BUCKET_NAME", "")

dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")


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
            parts = key.split("/")
            if len(parts) < 3:
                continue
            date_str = parts[2]
            try:
                obj_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError:
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
            except Exception as exc:
                print(f"Cleanup error for {key}: {exc}")

    return results


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Periodic news retention cleaner.

    Triggered by EventBridge (e.g., once per day).
    For all stocks with is_deleted = true:
      - Ensure their news objects older than 6 weeks are removed.
      - Tag those older than 3 weeks as deleted=true.
    """
    table = dynamodb.Table(TABLE_NAME)

    deleted_soft = 0
    deleted_hard = 0

    scan_kwargs: Dict[str, Any] = {
        "FilterExpression": "begins_with(SK, :sk) AND is_deleted = :true",
        "ExpressionAttributeValues": {":sk": "STOCK#", ":true": True},
    }

    while True:
        resp = table.scan(**scan_kwargs)
        for item in resp.get("Items", []):
            sk = item.get("SK", "")
            if not sk.startswith("STOCK#"):
                continue
            symbol = sk.split("STOCK#", 1)[1]
            counts = _cleanup_news_for_symbol(symbol)
            deleted_soft += counts["soft_tagged"]
            deleted_hard += counts["hard_deleted"]

        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
        scan_kwargs["ExclusiveStartKey"] = last_key

    body = {
        "soft_tagged": deleted_soft,
        "hard_deleted": deleted_hard,
    }
    return {
        "statusCode": 200,
        "body": json.dumps(body),
    }


