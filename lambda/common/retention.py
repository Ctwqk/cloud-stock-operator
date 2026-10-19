"""News retention rules shared by the soft-delete Lambda and the daily cleaner.

News objects live at ``news/<SYMBOL>/<yyyy-mm-dd>/<news_id>.json``. For a deleted stock:

* older than 6 weeks  -> deleted from S3
* older than 3 weeks  -> tagged ``deleted=true`` (a "soft" delete)
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any

from . import aws, timeutil

logger = logging.getLogger(__name__)

SOFT_DELETE_AFTER = timedelta(weeks=3)
HARD_DELETE_AFTER = timedelta(weeks=6)


def cleanup_news_for_symbol(bucket: str, symbol: str, today: date | None = None) -> dict[str, int]:
    """Apply the retention rules to one symbol. Returns ``{"soft_tagged": n, "hard_deleted": m}``.

    Objects that are already tagged are not re-tagged, so repeated runs report only new work.
    """
    s3 = aws.client("s3")
    today = today or timeutil.now().date()
    soft_cutoff = today - SOFT_DELETE_AFTER
    hard_cutoff = today - HARD_DELETE_AFTER
    result = {"soft_tagged": 0, "hard_deleted": 0}
    failures = 0

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"news/{symbol.upper()}/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            obj_date = _date_from_key(key)
            try:
                if obj_date is not None and obj_date <= hard_cutoff:
                    s3.delete_object(Bucket=bucket, Key=key)
                    result["hard_deleted"] += 1
                elif (obj_date is None or obj_date <= soft_cutoff) and not _is_tagged(
                    s3, bucket, key
                ):
                    s3.put_object_tagging(
                        Bucket=bucket,
                        Key=key,
                        Tagging={"TagSet": [{"Key": "deleted", "Value": "true"}]},
                    )
                    result["soft_tagged"] += 1
            except Exception:  # best effort: one bad object must not block the rest
                failures += 1
                logger.exception("retention cleanup failed for %s", key)
    if failures:
        raise RuntimeError(f"retention cleanup for {symbol}: {failures} object operation(s) failed")
    return result


def _date_from_key(key: str) -> date | None:
    parts = key.split("/")
    if len(parts) < 4:
        return None
    try:
        return datetime.strptime(parts[2], "%Y-%m-%d").date()
    except ValueError:
        return None


def _is_tagged(s3: Any, bucket: str, key: str) -> bool:
    tags = s3.get_object_tagging(Bucket=bucket, Key=key).get("TagSet", [])
    return any(t["Key"] == "deleted" and t["Value"] == "true" for t in tags)
