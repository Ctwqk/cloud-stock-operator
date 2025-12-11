import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict

import boto3


TABLE_NAME = os.environ.get("TABLE_NAME", "")

dynamodb = boto3.resource("dynamodb")
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


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Periodic net-worth snapshot writer.

    Triggered by EventBridge every 15 minutes.

    For homework/demo purposes this implementation:
      - Reads the SUMMARY item for the account (if it exists).
      - Uses current_cash_managed as `cash` (default 0.0).
      - Sets positions_value = 0.0.
      - Sets net_worth = cash + positions_value.
      - Writes a NETWORTH#<timestamp_iso> item into the table.

    You can replace the logic with a real integration that computes
    cash/positions_value from IBKR or other sources.
    """
    table = dynamodb.Table(TABLE_NAME)
    account_id = _get_account_id()
    pk = f"ACCOUNT#{account_id}"
    sk_summary = "SUMMARY"

    summary_resp = table.get_item(Key={"PK": pk, "SK": sk_summary})
    summary = summary_resp.get("Item") or {}

    # DynamoDB requires Decimal for numeric types; convert carefully.
    raw_cash = summary.get("current_cash_managed", Decimal("0"))
    raw_positions = summary.get("positions_value", Decimal("0"))

    cash = raw_cash if isinstance(raw_cash, Decimal) else Decimal(str(raw_cash))
    positions_value = (
        raw_positions if isinstance(raw_positions, Decimal) else Decimal(str(raw_positions))
    )
    net_worth = cash + positions_value

    timestamp = _now_iso()
    sk_networth = f"NETWORTH#{timestamp}"

    table.put_item(
        Item={
            "PK": pk,
            "SK": sk_networth,
            "net_worth": net_worth,
            "cash": cash,
            "positions_value": positions_value,
            "created_at": timestamp,
        }
    )

    return {
        "statusCode": 200,
        "body": f"Snapshot written for {account_id} at {timestamp}",
    }



