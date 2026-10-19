"""Local trade worker: the "outside AWS" consumer of trade decisions.

It polls ``TradeDecisionQueue`` for ``AUTO_TRADE_DECISION`` messages (published by the sentiment
scorer when a stock's score crosses its threshold) and executes them as **virtual trades**
against the managed cash / shares stored in DynamoDB. No broker is involved: the earlier IBKR
integration was removed, so this is paper trading at the last fetched price.

Every decision is executed atomically (one DynamoDB transaction updates cash, shares, the trade
log and a ``DECISION#<id>`` marker), which makes the worker safe against redelivery: executing
the same decision twice is a no-op. Rejected decisions (no price, not enough cash/shares...)
are logged as ``TRADE#`` items with a ``REJECTED_*`` status.

Configuration (environment)::

    TABLE_NAME                 DynamoDB table (default: TradingTable)
    TRADE_DECISION_QUEUE_URL   queue to poll; or
    TRADE_DECISION_QUEUE_NAME  queue name to resolve (default: TradeDecisionQueue)
    TRADE_QUANTITY             shares per decision when the message has no "shares" (default: 1)
    AWS_ENDPOINT_URL           point at the local emulator instead of AWS
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import time
from decimal import Decimal
from typing import Any

from botocore.exceptions import ClientError

from common import aws, ddb, keys, messages, portfolio, timeutil

logger = logging.getLogger("local_trade_worker")

DECISION_TTL_DAYS = 45

EXECUTED = "EXECUTED_VIRTUAL"
REJECTED_NO_PRICE = "REJECTED_NO_PRICE"
REJECTED_CASH = "REJECTED_INSUFFICIENT_MANAGED_CASH"
REJECTED_SHARES = "REJECTED_INSUFFICIENT_SHARES"
REJECTED_SYMBOL = "REJECTED_UNKNOWN_SYMBOL"
REJECTED_ACTION = "REJECTED_BAD_ACTION"
DUPLICATE = "DUPLICATE"


def _stock_condition() -> str:
    return "attribute_exists(PK) AND (attribute_not_exists(is_deleted) OR is_deleted = :false)"


def execute_decision(msg: dict[str, Any]) -> str:
    """Execute one ``AUTO_TRADE_DECISION``. Returns the resulting status string."""
    if msg.get("type") != messages.AUTO_TRADE_DECISION:
        raise ValueError(f"unexpected message type {msg.get('type')!r}")

    account_id = str(msg["account_id"])
    symbol = messages.normalize_symbol(msg["symbol"])
    action = str(msg["action"]).upper()
    quantity = int(msg.get("shares") or os.environ.get("TRADE_QUANTITY", "1"))
    decision_id = str(msg.get("decision_id") or f"adhoc-{timeutil.iso_utc()}")
    if quantity < 1:
        raise ValueError("shares must be >= 1")

    table = aws.table()
    pk = keys.account_pk(account_id)
    stock_key = {"PK": pk, "SK": keys.stock_sk(symbol)}
    summary_key = {"PK": pk, "SK": keys.SUMMARY_SK}

    stock = table.get_item(Key=stock_key, ConsistentRead=True).get("Item")
    price = (
        ddb.to_decimal(stock["last_fetched_price"])
        if stock and stock.get("last_fetched_price")
        else None
    )

    if action not in {"BUY", "SELL"}:
        return _record(msg, decision_id, REJECTED_ACTION, quantity, price)
    if not stock or stock.get("is_deleted"):
        return _record(msg, decision_id, REJECTED_SYMBOL, quantity, price)
    if price is None or price <= 0:
        return _record(msg, decision_id, REJECTED_NO_PRICE, quantity, price)

    notional = price * quantity
    now = timeutil.iso_utc()
    table_name = table.name
    if action == "BUY":
        money = {
            "Key": summary_key,
            "UpdateExpression": (
                "SET current_cash_managed = current_cash_managed - :n, updated_at = :now"
            ),
            "ConditionExpression": (
                "attribute_exists(current_cash_managed) AND current_cash_managed >= :n"
            ),
            "ExpressionAttributeValues": {":n": notional, ":now": now},
        }
        position = {
            "Key": stock_key,
            "UpdateExpression": (
                "SET shares_managed = if_not_exists(shares_managed, :zero) + :q, updated_at = :now"
            ),
            "ConditionExpression": _stock_condition(),
            "ExpressionAttributeValues": {":q": quantity, ":zero": 0, ":false": False, ":now": now},
        }
        failure_status = REJECTED_CASH
    else:
        money = {
            "Key": summary_key,
            "UpdateExpression": (
                "SET current_cash_managed = if_not_exists(current_cash_managed, :zero) + :n, "
                "updated_at = :now"
            ),
            "ExpressionAttributeValues": {":n": notional, ":zero": Decimal("0"), ":now": now},
        }
        position = {
            "Key": stock_key,
            "UpdateExpression": "SET shares_managed = shares_managed - :q, updated_at = :now",
            "ConditionExpression": (
                "attribute_exists(PK) AND shares_managed >= :q "
                "AND (attribute_not_exists(is_deleted) OR is_deleted = :false)"
            ),
            "ExpressionAttributeValues": {":q": quantity, ":false": False, ":now": now},
        }
        failure_status = REJECTED_SHARES

    marker = _decision_marker(pk, decision_id, EXECUTED)
    trade = _trade_item(msg, decision_id, EXECUTED, quantity, price, now)
    try:
        aws.ddb_client().transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "TableName": table_name,
                        "Item": marker,
                        "ConditionExpression": "attribute_not_exists(PK)",
                    }
                },
                {"Update": {"TableName": table_name, **money}},
                {"Update": {"TableName": table_name, **position}},
                {"Put": {"TableName": table_name, "Item": trade}},
            ]
        )
    except ClientError as exc:
        codes = ddb.cancellation_codes(exc)
        if codes[:1] == ["ConditionalCheckFailed"]:
            logger.info("decision %s already handled", decision_id)
            return DUPLICATE
        if "ConditionalCheckFailed" in codes[1:3]:
            return _record(msg, decision_id, failure_status, quantity, price)
        raise

    portfolio.refresh_summary(account_id)
    logger.info("%s %s x%s @ %s -> %s", action, symbol, quantity, price, EXECUTED)
    return EXECUTED


def _decision_marker(pk: str, decision_id: str, status: str) -> dict[str, Any]:
    return {
        "PK": pk,
        "SK": keys.decision_sk(decision_id),
        "status": status,
        "created_at": timeutil.iso_utc(),
        keys.TTL_ATTRIBUTE: timeutil.epoch_in(days=DECISION_TTL_DAYS),
    }


def _trade_item(
    msg: dict[str, Any],
    decision_id: str,
    status: str,
    quantity: int,
    price: Decimal | None,
    created_at: str,
) -> dict[str, Any]:
    item: dict[str, Any] = {
        "PK": keys.account_pk(str(msg["account_id"])),
        "SK": keys.trade_sk(created_at, decision_id),
        "decision_id": decision_id,
        "symbol": str(msg.get("symbol", "")).upper(),
        "side": str(msg.get("action", "")).upper(),
        "reason": msg.get("reason", "SCORE_THRESHOLD"),
        "status": status,
        "quantity": quantity,
        "created_at": created_at,
    }
    if msg.get("score") is not None:
        item["score"] = msg["score"]
        item["threshold_abs"] = msg.get("threshold_abs")
    if price is not None:
        item["last_price"] = price
        item["notional"] = price * quantity
    return item


def _record(
    msg: dict[str, Any], decision_id: str, status: str, quantity: int, price: Decimal | None
) -> str:
    """Log a rejected decision (and mark it handled) in one transaction."""
    table_name = aws.table().name
    now = timeutil.iso_utc()
    pk = keys.account_pk(str(msg["account_id"]))
    try:
        aws.ddb_client().transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "TableName": table_name,
                        "Item": _decision_marker(pk, decision_id, status),
                        "ConditionExpression": "attribute_not_exists(PK)",
                    }
                },
                {
                    "Put": {
                        "TableName": table_name,
                        "Item": _trade_item(msg, decision_id, status, quantity, price, now),
                    }
                },
            ]
        )
    except ClientError as exc:
        if ddb.cancellation_codes(exc)[:1] == ["ConditionalCheckFailed"]:
            return DUPLICATE
        raise
    logger.warning("decision %s rejected: %s", decision_id, status)
    return status


# --------------------------------------------------------------------------------------------
# Queue loop
# --------------------------------------------------------------------------------------------
def queue_url() -> str:
    explicit = os.environ.get("TRADE_DECISION_QUEUE_URL")
    if explicit:
        return explicit
    name = os.environ.get("TRADE_DECISION_QUEUE_NAME", "TradeDecisionQueue")
    return aws.client("sqs").get_queue_url(QueueName=name)["QueueUrl"]


def poll_once(url: str | None = None, wait_seconds: int = 0) -> int:
    """Receive one batch and execute it. A message is deleted only after it was handled."""
    sqs = aws.client("sqs")
    url = url or queue_url()
    response = sqs.receive_message(
        QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=wait_seconds
    )
    received = response.get("Messages", [])
    for message in received:
        try:
            execute_decision(messages.parse_sqs_record({"body": message["Body"]}))
        except Exception:
            # Left on the queue: it becomes visible again and, after maxReceiveCount
            # deliveries, moves to the dead-letter queue.
            logger.exception("could not process message %s", message["MessageId"])
            continue
        sqs.delete_message(QueueUrl=url, ReceiptHandle=message["ReceiptHandle"])
    return len(received)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    stopping = False

    def _stop(*_: Any) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    url = _wait_for_queue()
    logger.info("trade worker polling %s", url)
    while not stopping:
        try:
            if poll_once(url, wait_seconds=int(os.environ.get("POLL_WAIT_SECONDS", "5"))) == 0:
                time.sleep(0.5)
        except Exception:
            logger.exception("poll failed; retrying")
            time.sleep(2)


def _wait_for_queue() -> str:
    """The local emulator may still be starting; resolve the queue with retries."""
    for attempt in range(60):
        try:
            return queue_url()
        except Exception as exc:
            logger.info("waiting for queue (%s)", exc)
            time.sleep(min(2 + attempt, 5))
    sys.exit("trade decision queue never became available")


if __name__ == "__main__":
    main()
