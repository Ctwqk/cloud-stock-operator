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
from typing import Any, Dict

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

    print(f"[{_now_iso()}] AUTO_TRADE_DECISION: {action} {symbol} (score={score})")

    # TODO: Integrate with IBKR Gateway on localhost:4001/4002 here.
    # For example, place a market order using your IBKR Python API.
    trade_status = "PENDING"

    # Optionally, write a TRADE#... record into DynamoDB for logging.
    table = dynamodb.Table(TABLE_NAME)
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
            "created_at": ts,
        }
    )


def main() -> None:
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



