"""Periodic net-worth snapshot writer (EventBridge, every 15 minutes).

Computes ``net_worth = managed cash + sum(shares_managed * last_fetched_price)`` from the
*current* watchlist (the previous version read a ``positions_value`` that was only refreshed on
manual adjustments, so the series went stale whenever prices moved) and appends a
``NETWORTH#<ts>`` item.

The EventBridge event's ``time`` field is used as the snapshot timestamp, so the retry of a
failed invocation overwrites the same item rather than adding a duplicate point.
"""

from __future__ import annotations

import logging
from typing import Any

from common import aws, keys, portfolio, timeutil

logger = logging.getLogger(__name__)


def handler(event: dict[str, Any] | None = None, context: Any = None) -> dict[str, Any]:
    event = event or {}
    account_id = event.get("account_id") or aws.get_account_id()
    moment = timeutil.parse_iso(event.get("time"))
    timestamp = timeutil.iso_utc(moment)

    summary = portfolio.refresh_summary(account_id)
    aws.table().put_item(
        Item={
            "PK": keys.account_pk(account_id),
            "SK": keys.networth_sk(timestamp),
            "net_worth": summary["net_worth"],
            "cash": summary["cash"],
            "positions_value": summary["positions_value"],
            "created_at": timestamp,
        }
    )
    logger.info("snapshot %s net_worth=%s", timestamp, summary["net_worth"])
    return {
        "statusCode": 200,
        "account_id": account_id,
        "timestamp": timestamp,
        "net_worth": float(summary["net_worth"]),
    }
