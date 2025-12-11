import json
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict

import boto3


TABLE_NAME = os.environ.get("TABLE_NAME", "")
SYSTEM_OPS_TOPIC_ARN = os.environ.get("SYSTEM_OPS_TOPIC_ARN", "")

dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")


def _sqs_record_to_message(record: Dict[str, Any]) -> Dict[str, Any]:
    body_str = record.get("body", "{}")
    try:
        outer = json.loads(body_str)
        inner_str = outer.get("Message", "{}")
        return json.loads(inner_str)
    except json.JSONDecodeError:
        return {}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _handle_add_stock(msg: Dict[str, Any], table) -> None:
    account_id = msg["account_id"]
    symbol = msg["symbol"]
    initial_shares = int(msg.get("initial_shares_managed", 0))
    pk = f"ACCOUNT#{account_id}"
    sk = f"STOCK#{symbol}"
    now = _now_iso()

    table.put_item(
        Item={
            "PK": pk,
            "SK": sk,
            "symbol": symbol,
            "shares_managed": initial_shares,
            "current_level_score": 0,
            "threshold_abs": 3,
            "is_deleted": False,
            "created_at": now,
            "updated_at": now,
        }
    )

    # Optionally emit STOCK_ADDED to SystemOpsTopic so News Fetcher can run immediately
    sns.publish(
        TopicArn=SYSTEM_OPS_TOPIC_ARN,
        Message=json.dumps(
            {
                "type": "STOCK_ADDED",
                "account_id": account_id,
                "symbol": symbol,
            }
        ),
    )


def _handle_adjust_managed_cash(msg: Dict[str, Any], table) -> None:
    account_id = msg["account_id"]
    # DynamoDB numeric attributes must be Decimal, not float
    delta_cash = Decimal(str(msg["delta_cash"]))
    max_allowed_cash = Decimal(str(msg["max_allowed_cash"]))

    pk = f"ACCOUNT#{account_id}"
    sk = "SUMMARY"

    # Ensure we don't exceed max_allowed_cash as enforced by backend.
    table.update_item(
        Key={"PK": pk, "SK": sk},
        UpdateExpression="SET current_cash_managed = if_not_exists(current_cash_managed, :zero) + :delta, updated_at = :u",
        ConditionExpression="if_not_exists(current_cash_managed, :zero) + :delta <= :max",
        ExpressionAttributeValues={
            ":delta": delta_cash,
            ":max": max_allowed_cash,
            ":zero": Decimal("0"),
            ":u": _now_iso(),
        },
    )


def _handle_adjust_managed_shares(msg: Dict[str, Any], table) -> None:
    account_id = msg["account_id"]
    symbol = msg["symbol"]
    delta_shares = int(msg["delta_shares"])
    max_shares_allowed = int(msg["max_shares_allowed"])

    pk = f"ACCOUNT#{account_id}"
    sk = f"STOCK#{symbol}"

    table.update_item(
        Key={"PK": pk, "SK": sk},
        UpdateExpression="SET shares_managed = if_not_exists(shares_managed, :zero) + :delta, updated_at = :u",
        ConditionExpression="if_not_exists(shares_managed, :zero) + :delta <= :max",
        ExpressionAttributeValues={
            ":delta": delta_shares,
            ":max": max_shares_allowed,
            ":zero": 0,
            ":u": _now_iso(),
        },
    )


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda D – External Ops Handler (Q1 → DynamoDB).

    Message types:
      - ADD_STOCK
      - ADJUST_MANAGED_CASH
      - ADJUST_MANAGED_SHARES
    """
    table = dynamodb.Table(TABLE_NAME)
    processed = 0

    for record in event.get("Records", []):
        msg = _sqs_record_to_message(record)
        msg_type = msg.get("type")

        try:
            if msg_type == "ADD_STOCK":
                _handle_add_stock(msg, table)
            elif msg_type == "ADJUST_MANAGED_CASH":
                _handle_adjust_managed_cash(msg, table)
            elif msg_type == "ADJUST_MANAGED_SHARES":
                _handle_adjust_managed_shares(msg, table)
            else:
                # Ignore unknown types
                continue
            processed += 1
        except Exception as exc:  # best-effort for homework
            print(f"Error processing message {msg_type}: {exc}")

    return {
        "statusCode": 200,
        "body": json.dumps({"processed": processed}),
    }





