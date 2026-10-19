"""Lambda D: external commands applied exactly once, within limits."""

from __future__ import annotations

import json
from decimal import Decimal

import boto3
import pytest

from stock_info_writer.main import handler
from tests.helpers import sqs_event

ACCOUNT = "123456789012"
PK = f"ACCOUNT#{ACCOUNT}"


def cmd(type_, op_id="op-1", **fields):
    return {"type": type_, "op_id": op_id, "account_id": ACCOUNT, **fields}


def run(*messages):
    return handler(sqs_event(*messages), None)


def item(table, sk):
    return table.get_item(Key={"PK": PK, "SK": sk}).get("Item")


def op(table, op_id):
    return item(table, f"OP#{op_id}")


def system_messages(cloud, queue="FetchTriggerQueue"):
    sqs = boto3.client("sqs")
    got = sqs.receive_message(QueueUrl=cloud.queue_urls[queue], MaxNumberOfMessages=10)
    return [json.loads(json.loads(m["Body"])["Message"]) for m in got.get("Messages", [])]


class TestAddStock:
    def test_creates_watchlist_entry_and_announces_it(self, cloud, table):
        result = run(cmd("ADD_STOCK", symbol="aapl", initial_shares_managed=4))

        assert result == {"batchItemFailures": []}
        stock = item(table, "STOCK#AAPL")
        assert stock["symbol"] == "AAPL"
        assert stock["shares_managed"] == 4
        assert stock["current_level_score"] == 0
        assert stock["threshold_abs"] == 1
        assert stock["is_deleted"] is False
        assert op(table, "op-1")["status"] == "APPLIED"
        assert system_messages(cloud) == [
            {"type": "STOCK_ADDED", "account_id": ACCOUNT, "symbol": "AAPL"}
        ]

    def test_redelivery_does_not_reset_state(self, cloud, table):
        """Regression: ADD_STOCK used an unconditional put and wiped score/threshold."""
        run(cmd("ADD_STOCK", symbol="AAPL"))
        table.update_item(
            Key={"PK": PK, "SK": "STOCK#AAPL"},
            UpdateExpression="SET current_level_score = :s, threshold_abs = :t",
            ExpressionAttributeValues={":s": 2, ":t": 5},
        )
        run(cmd("ADD_STOCK", symbol="AAPL"))  # same op_id: SQS redelivery
        stock = item(table, "STOCK#AAPL")
        assert (stock["current_level_score"], stock["threshold_abs"]) == (2, 5)

    def test_readding_a_deleted_stock_reactivates_it_and_keeps_history(self, cloud, table):
        run(cmd("ADD_STOCK", "op-a", symbol="AAPL", initial_shares_managed=7))
        table.update_item(
            Key={"PK": PK, "SK": "STOCK#AAPL"},
            UpdateExpression="SET is_deleted = :t, threshold_abs = :th",
            ExpressionAttributeValues={":t": True, ":th": 9},
        )
        run(cmd("ADD_STOCK", "op-b", symbol="AAPL", initial_shares_managed=1))
        stock = item(table, "STOCK#AAPL")
        assert stock["is_deleted"] is False
        assert (stock["shares_managed"], stock["threshold_abs"]) == (7, 9)

    @pytest.mark.parametrize(
        "fields, reason",
        [
            ({"symbol": "not a symbol"}, "invalid symbol"),
            ({"symbol": "AAPL", "initial_shares_managed": -1}, "must be >= 0"),
            ({"symbol": "AAPL", "initial_shares_managed": "x"}, "initial_shares_managed"),
        ],
    )
    def test_invalid_command_is_rejected_not_retried(self, cloud, table, fields, reason):
        result = run(cmd("ADD_STOCK", **fields))
        assert result == {"batchItemFailures": []}  # permanent: nothing to retry
        record = op(table, "op-1")
        assert record["status"] == "REJECTED"
        assert reason in record["reason"]
        assert item(table, "STOCK#AAPL") is None


class TestAdjustCash:
    def adjust(self, delta, maximum=1000, op_id="op-1"):
        return run(cmd("ADJUST_MANAGED_CASH", op_id, delta_cash=delta, max_allowed_cash=maximum))

    def test_adds_delta_and_updates_net_worth(self, cloud, table):
        self.adjust(250)
        self.adjust("100.50", op_id="op-2")
        summary = item(table, "SUMMARY")
        assert summary["current_cash_managed"] == Decimal("350.50")
        assert summary["current_net_worth"] == Decimal("350.50")

    def test_same_op_id_is_applied_once(self, cloud, table):
        """Regression: deltas were re-applied on every redelivery (and by the API itself)."""
        for _ in range(3):
            self.adjust(100)
        assert item(table, "SUMMARY")["current_cash_managed"] == 100

    def test_exceeding_maximum_is_rejected(self, cloud, table):
        self.adjust(900)
        self.adjust(200, op_id="op-2")
        assert item(table, "SUMMARY")["current_cash_managed"] == 900
        assert op(table, "op-2")["status"] == "REJECTED"
        assert "maximum" in op(table, "op-2")["reason"]

    def test_cash_cannot_go_negative(self, cloud, table):
        self.adjust(100)
        self.adjust(-150, op_id="op-2")
        assert item(table, "SUMMARY")["current_cash_managed"] == 100
        assert "below zero" in op(table, "op-2")["reason"]

    def test_withdrawal_within_balance(self, cloud, table):
        self.adjust(500)
        self.adjust(-200, op_id="op-2")
        assert item(table, "SUMMARY")["current_cash_managed"] == 300

    def test_non_numeric_values_are_rejected(self, cloud, table):
        run(cmd("ADJUST_MANAGED_CASH", delta_cash="lots", max_allowed_cash=10))
        assert op(table, "op-1")["status"] == "REJECTED"


class TestAdjustShares:
    def setup_stock(self, table, shares=5):
        run(cmd("ADD_STOCK", "seed", symbol="AAPL", initial_shares_managed=shares))

    def adjust(self, delta, maximum=100, op_id="op-1", symbol="AAPL"):
        return run(
            cmd(
                "ADJUST_MANAGED_SHARES",
                op_id,
                symbol=symbol,
                delta_shares=delta,
                max_shares_allowed=maximum,
            )
        )

    def test_changes_shares_and_recomputes_positions_value(self, cloud, table):
        self.setup_stock(table, 5)
        table.update_item(
            Key={"PK": PK, "SK": "STOCK#AAPL"},
            UpdateExpression="SET last_fetched_price = :p",
            ExpressionAttributeValues={":p": Decimal("10.5")},
        )
        self.adjust(5)
        assert item(table, "STOCK#AAPL")["shares_managed"] == 10
        summary = item(table, "SUMMARY")
        assert summary["positions_value"] == Decimal("105.0")
        assert summary["current_net_worth"] == Decimal("105.0")

    def test_idempotent(self, cloud, table):
        self.setup_stock(table, 0)
        for _ in range(2):
            self.adjust(3)
        assert item(table, "STOCK#AAPL")["shares_managed"] == 3

    @pytest.mark.parametrize(
        "delta, maximum, symbol, reason",
        [
            (200, 100, "AAPL", "maximum"),
            (-50, 100, "AAPL", "below zero"),
            (1, 100, "MSFT", "not on the watchlist"),
        ],
    )
    def test_rejections(self, cloud, table, delta, maximum, symbol, reason):
        self.setup_stock(table, 5)
        self.adjust(delta, maximum, symbol=symbol)
        record = op(table, "op-1")
        assert record["status"] == "REJECTED" and reason in record["reason"]
        assert item(table, "STOCK#AAPL")["shares_managed"] == 5
        assert item(table, "STOCK#MSFT") is None  # no phantom item


class TestBatchBehaviour:
    def test_unknown_types_and_missing_account_are_ignored(self, cloud, table):
        result = run(
            {"type": "SOMETHING", "account_id": ACCOUNT}, {"type": "ADD_STOCK", "symbol": "A"}
        )
        assert result == {"batchItemFailures": []}

    def test_infrastructure_error_fails_only_that_message(self, cloud, table, monkeypatch):
        import stock_info_writer.main as module

        real = module._add_stock

        def flaky(account_id, op_id, msg):
            if msg["symbol"] == "BAD":
                raise RuntimeError("DynamoDB throttled")
            return real(account_id, op_id, msg)

        monkeypatch.setitem(module._COMMANDS, "ADD_STOCK", flaky)
        result = run(
            cmd("ADD_STOCK", "ok-1", symbol="GOOD"),
            cmd("ADD_STOCK", "bad-1", symbol="BAD"),
        )
        assert result == {"batchItemFailures": [{"itemIdentifier": "m-1"}]}
        assert item(table, "STOCK#GOOD") is not None

    def test_op_id_falls_back_to_sqs_message_id(self, cloud, table):
        message = {"type": "ADD_STOCK", "account_id": ACCOUNT, "symbol": "AAPL"}
        run(message)
        assert op(table, "msg-m-0")["status"] == "APPLIED"

    def test_real_sns_to_sqs_delivery(self, cloud, table, sqs):
        """Publish via SNS, receive from the real queue, feed the handler: full envelope path."""
        from common import messages

        messages.publish(
            cloud.external_topic_arn,
            cmd("ADD_STOCK", "via-sns", symbol="NVDA", initial_shares_managed=2),
        )
        received = sqs.receive_message(QueueUrl=cloud.queue_urls["ExternalOpsQueue"])["Messages"]
        event = {
            "Records": [
                {"messageId": m["MessageId"], "body": m["Body"], "receiptHandle": "r"}
                for m in received
            ]
        }
        assert handler(event, None) == {"batchItemFailures": []}
        assert item(table, "STOCK#NVDA")["shares_managed"] == 2


def test_concurrent_modification_is_retried_not_overwritten(cloud, table, monkeypatch):
    """The delta is computed from a read; if someone else wrote meanwhile the txn must fail."""
    import stock_info_writer.main as module

    run(cmd("ADJUST_MANAGED_CASH", "seed", delta_cash=100, max_allowed_cash=1000))
    real_client = module.aws.ddb_client()

    class Interfering:
        def transact_write_items(self, **kwargs):
            table.update_item(  # another writer sneaks in between our read and our write
                Key={"PK": PK, "SK": "SUMMARY"},
                UpdateExpression="SET current_cash_managed = :v",
                ExpressionAttributeValues={":v": Decimal("500")},
            )
            return real_client.transact_write_items(**kwargs)

    monkeypatch.setattr(module.aws, "ddb_client", lambda: Interfering())
    result = run(cmd("ADJUST_MANAGED_CASH", "racy", delta_cash=50, max_allowed_cash=1000))

    assert result == {"batchItemFailures": [{"itemIdentifier": "m-0"}]}
    assert item(table, "SUMMARY")["current_cash_managed"] == 500  # not 150
    assert op(table, "racy") is None  # marker rolled back with the failed transaction
