"""
Local Trade Worker
===================

Runs on your IBKR Gateway machine (laptop/desktop), NEVER in AWS.

Responsibilities:
  - Poll SQS Q2 (SystemOpsQueue) for AUTO_TRADE_DECISION messages.
  - For each decision, place trades via IBKR Gateway on localhost:4001/4002.
  - Poll NEWS_STORED messages and run FinBERT (or any sentiment model) locally,
    then POST the sentiment delta back to AWS Management API, which applies it
    to DynamoDB and emits AUTO_TRADE_DECISION if thresholds are crossed.
  - Optionally write trade status back to DynamoDB (TRADE#... items).

This script is intentionally minimal and does NOT contain real IBKR code.
You should plug in your existing IBKR integration where indicated.
"""

import json
import os
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Optional

import boto3
import requests


# Configure these via environment variables or edit directly.
SYSTEM_OPS_QUEUE_URL = os.environ.get(
    "SYSTEM_OPS_QUEUE_URL",
    "https://sqs.<region>.amazonaws.com/<account-id>/SystemOpsQueue",
)
TABLE_NAME = os.environ.get("TABLE_NAME", "TradingTable")
MGMT_API_BASE_URL = os.environ.get("MGMT_API_BASE_URL", "").rstrip("/")

sqs = boto3.client("sqs")
dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_finbert_on_text(headline: str, body: str) -> int:
    """
    Placeholder for real FinBERT scoring.
    Return an integer delta in {-1, 0, +1} based on sentiment.
    Replace this with your actual FinBERT code.
    """
    # TODO: integrate real FinBERT model here. For now, always return +1.
    return 1


_ib: Optional[object] = None  # kept only to avoid breaking references if any


def _get_managed_cash(account_id: str) -> float:
    """
    Read current_cash_managed from the SUMMARY item for the account.
    """
    table = dynamodb.Table(TABLE_NAME)
    resp = table.get_item(Key={"PK": f"ACCOUNT#{account_id}", "SK": "SUMMARY"})
    item = resp.get("Item") or {}
    value = item.get("current_cash_managed", 0)
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _update_managed_cash(account_id: str, new_cash: float) -> None:
    """
    Persist a new current_cash_managed value to the SUMMARY item.
    """
    table = dynamodb.Table(TABLE_NAME)
    table.update_item(
        Key={"PK": f"ACCOUNT#{account_id}", "SK": "SUMMARY"},
        UpdateExpression="SET current_cash_managed = :c, updated_at = :u",
        ExpressionAttributeValues={
            ":c": Decimal(str(new_cash)),
            ":u": _now_iso(),
        },
    )


def _get_last_fetched_price(account_id: str, symbol: str) -> Optional[float]:
    """
    Load last_fetched_price from the STOCK item, if present.

    This is written by the NewsFetcher Lambda using yfinance.
    """
    table = dynamodb.Table(TABLE_NAME)
    resp = table.get_item(
        Key={"PK": f"ACCOUNT#{account_id}", "SK": f"STOCK#{symbol}"}
    )
    item = resp.get("Item") or {}
    value = item.get("last_fetched_price")
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _recompute_positions_and_net_worth(account_id: str) -> None:
    """
    Recompute positions_value and current_net_worth for the account based on
    all STOCK# items and current_cash_managed.
    """
    table = dynamodb.Table(TABLE_NAME)
    pk = f"ACCOUNT#{account_id}"

    # Query all STOCK# items for this account
    resp = table.query(
        KeyConditionExpression="PK = :pk AND begins_with(SK, :sk_prefix)",
        ExpressionAttributeValues={
            ":pk": pk,
            ":sk_prefix": "STOCK#",
        },
    )
    items = resp.get("Items", [])

    positions_value = 0.0
    for item in items:
        try:
            shares = float(item.get("shares_managed", 0) or 0)
            price = float(item.get("last_fetched_price", 0) or 0)
            positions_value += shares * price
        except (TypeError, ValueError):
            continue

    # Read current_cash_managed
    summary_resp = table.get_item(Key={"PK": pk, "SK": "SUMMARY"})
    summary = summary_resp.get("Item") or {}
    try:
        cash = float(summary.get("current_cash_managed", 0) or 0)
    except (TypeError, ValueError):
        cash = 0.0

    net_worth = cash + positions_value

    table.update_item(
        Key={"PK": pk, "SK": "SUMMARY"},
        UpdateExpression=(
            "SET positions_value = :pv, current_net_worth = :nw, updated_at = :u"
        ),
        ExpressionAttributeValues={
            ":pv": Decimal(str(positions_value)),
            ":nw": Decimal(str(net_worth)),
            ":u": _now_iso(),
        },
    )


def _ib_self_test() -> None:
    """
    IBKR is no longer used for placing real orders; this self-test is now a no-op.
    Kept for backwards compatibility with the startup flow.
    """
    print(f"[{_now_iso()}] IBKR trading disabled; running in pseudo-trading mode.")


def _handle_news_stored(msg: Dict[str, Any]) -> None:
    """
    Handle NEWS_STORED message:
      {
        \"type\": \"NEWS_STORED\",
        \"account_id\": \"...\",
        \"symbol\": \"AAPL\",
        \"bucket\": \"...\",
        \"key\": \"news/AAPL/2025-01-01/....json\"
      }
    """
    if not MGMT_API_BASE_URL:
        print("MGMT_API_BASE_URL not set; skipping FinBERT sentiment apply.")
        return

    account_id = msg.get("account_id")
    symbol = msg.get("symbol")
    bucket = msg.get("bucket")
    key = msg.get("key")

    if not (account_id and symbol and bucket and key):
        print("NEWS_STORED message missing required fields, skipping:", msg)
        return

    # Load news content from S3
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        body_str = obj["Body"].read().decode("utf-8")
        news = json.loads(body_str)
    except Exception as exc:
        print(f"Failed to load news object {bucket}/{key}: {exc}")
        return

    headline = news.get("headline", "")
    body_text = news.get("body", "")

    delta = _run_finbert_on_text(headline, body_text)

    payload = {
        "symbol": symbol,
        "delta_score": delta,
        "reason": "FINBERT",
    }

    url = f"{MGMT_API_BASE_URL}/sentiment"
    try:
        resp = requests.post(url, json=payload, timeout=10)
        print(
            f"[{_now_iso()}] Sentiment applied via {url}: {resp.status_code} {resp.text}"
        )
    except Exception as exc:
        print(f"Error posting sentiment to {url}: {exc}")


def _handle_auto_trade_decision(msg: Dict[str, Any]) -> None:
    """
    Handle a single AUTO_TRADE_DECISION message.

    Expected shape:
      {
        \"type\": \"AUTO_TRADE_DECISION\",
        \"account_id\": \"...\",
        \"symbol\": \"AAPL\",
        \"action\": \"BUY\" or \"SELL\",
        \"score\": 3,
        \"threshold_abs\": 3,
        \"reason\": \"SCORE_THRESHOLD\"
      }
    """
    if msg.get("type") != "AUTO_TRADE_DECISION":
        return

    account_id = msg["account_id"]
    symbol = msg["symbol"]
    action = msg["action"]
    score = msg.get("score")
    threshold_abs = msg.get("threshold_abs")
    reason = msg.get("reason", "SCORE_THRESHOLD")
    quantity = int(msg.get("shares", 1))

    print(
        f"[{_now_iso()}] AUTO_TRADE_DECISION: {action} {quantity} {symbol} "
        f"(score={score}, threshold_abs={threshold_abs})"
    )
    last_price = _get_last_fetched_price(account_id, symbol)
    notional = None
    if last_price is not None and last_price > 0:
        notional = last_price * quantity

    managed_cash_before = _get_managed_cash(account_id)
    managed_cash_after = managed_cash_before
    trade_status = "NOOP"

    table = dynamodb.Table(TABLE_NAME)

    if notional is None:
        print(f"[{_now_iso()}] No valid price for {symbol}; skipping virtual trade.")
        trade_status = "REJECTED_NO_PRICE"
    else:
        if action.upper() == "BUY":
            # Pseudo-trade: ensure enough managed cash, then increase virtual position.
            if managed_cash_before < notional:
                print(
                    f"[{_now_iso()}] Insufficient managed cash for BUY {quantity} {symbol}: "
                    f"need ~{notional:.2f}, have {managed_cash_before:.2f}. Rejecting."
                )
                trade_status = "REJECTED_INSUFFICIENT_MANAGED_CASH"
            else:
                # Decrease managed cash
                managed_cash_after = managed_cash_before - notional
                _update_managed_cash(account_id, managed_cash_after)
                # Increase managed shares
                pk = f"ACCOUNT#{account_id}"
                sk = f"STOCK#{symbol}"
                table.update_item(
                    Key={"PK": pk, "SK": sk},
                    UpdateExpression=(
                        "SET shares_managed = if_not_exists(shares_managed, :zero) + :q, "
                        "updated_at = :u"
                    ),
                    ExpressionAttributeValues={
                        ":q": quantity,
                        ":zero": 0,
                        ":u": _now_iso(),
                    },
                )
                trade_status = "EXECUTED_VIRTUAL"
        elif action.upper() == "SELL":
            # Pseudo-trade: check virtual position and decrease it, then increase cash.
            pk = f"ACCOUNT#{account_id}"
            sk = f"STOCK#{symbol}"
            resp = table.get_item(Key={"PK": pk, "SK": sk})
            item = resp.get("Item") or {}
            current_shares = int(item.get("shares_managed", 0) or 0)
            if current_shares < quantity:
                print(
                    f"[{_now_iso()}] Insufficient managed shares for SELL {quantity} {symbol}: "
                    f"have {current_shares}. Rejecting."
                )
                trade_status = "REJECTED_INSUFFICIENT_SHARES"
            else:
                new_shares = current_shares - quantity
                table.update_item(
                    Key={"PK": pk, "SK": sk},
                    UpdateExpression="SET shares_managed = :new, updated_at = :u",
                    ExpressionAttributeValues={
                        ":new": new_shares,
                        ":u": _now_iso(),
                    },
                )
                managed_cash_after = managed_cash_before + notional
                _update_managed_cash(account_id, managed_cash_after)
                trade_status = "EXECUTED_VIRTUAL"
        else:
            print(f"[{_now_iso()}] Unknown action '{action}' in AUTO_TRADE_DECISION")
            trade_status = "REJECTED_BAD_ACTION"

    # After any executed virtual trade, recompute positions_value and net worth.
    if trade_status == "EXECUTED_VIRTUAL":
        _recompute_positions_and_net_worth(account_id)

    # Write a TRADE#... record into DynamoDB for logging.
    ts = _now_iso()
    trade_sk = f"TRADE#{ts}"

    table.put_item(
        Item={
            "PK": f"ACCOUNT#{account_id}",
            "SK": trade_sk,
            "symbol": symbol,
            "side": action,
            "reason": reason,
            "status": trade_status,
            "score": score,
            "threshold_abs": threshold_abs,
            "quantity": quantity,
            "last_price": last_price,
            "managed_cash_before": managed_cash_before,
            "managed_cash_after": managed_cash_after,
            "created_at": ts,
        }
    )


def main() -> None:
    # Run a quick IBKR self-test with AAPL before entering the main loop.
    _ib_self_test()

    print(f"Polling SystemOpsQueue at {SYSTEM_OPS_QUEUE_URL}")
    while True:
        resp = sqs.receive_message(
            QueueUrl=SYSTEM_OPS_QUEUE_URL,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20,
        )
        records = resp.get("Messages", [])
        if not records:
            continue

        for r in records:
            body_str = r.get("Body", "{}")
            try:
                outer = json.loads(body_str)
                inner_str = outer.get("Message", "{}")
                msg = json.loads(inner_str)
            except Exception as exc:
                print(f"Failed to parse message: {exc}")
                sqs.delete_message(
                    QueueUrl=SYSTEM_OPS_QUEUE_URL, ReceiptHandle=r["ReceiptHandle"]
                )
                continue

            handled = False
            try:
                msg_type = msg.get("type")
                if msg_type == "AUTO_TRADE_DECISION":
                    _handle_auto_trade_decision(msg)
                    handled = True
                elif msg_type == "NEWS_STORED":
                    _handle_news_stored(msg)
                    handled = True
                # For other message types (e.g., MARK_STOCK_DELETED), do nothing
                # here so that the SoftDelete lambda can consume them from Q2.
            finally:
                if handled:
                    sqs.delete_message(
                        QueueUrl=SYSTEM_OPS_QUEUE_URL, ReceiptHandle=r["ReceiptHandle"]
                    )

        # Small sleep to avoid tight loop if messages are constant.
        time.sleep(1)


if __name__ == "__main__":
    main()



