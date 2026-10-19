"""Local trade worker: atomic, idempotent virtual trading."""

from __future__ import annotations

import json
from decimal import Decimal

import boto3
import pytest

import local_trade_worker as worker

ACCOUNT = "123456789012"
PK = f"ACCOUNT#{ACCOUNT}"


def decision(action="BUY", symbol="AAPL", decision_id="d1", **extra):
    return {
        "type": "AUTO_TRADE_DECISION",
        "decision_id": decision_id,
        "account_id": ACCOUNT,
        "symbol": symbol,
        "action": action,
        "score": 3,
        "threshold_abs": 3,
        "reason": "TEST",
        **extra,
    }


@pytest.fixture
def account(table):
    table.put_item(Item={"PK": PK, "SK": "SUMMARY", "current_cash_managed": Decimal("1000")})
    table.put_item(
        Item={
            "PK": PK,
            "SK": "STOCK#AAPL",
            "symbol": "AAPL",
            "shares_managed": 2,
            "last_fetched_price": Decimal("100.25"),
            "is_deleted": False,
        }
    )
    return table


def summary(table):
    return table.get_item(Key={"PK": PK, "SK": "SUMMARY"})["Item"]


def shares(table, symbol="AAPL"):
    return int(table.get_item(Key={"PK": PK, "SK": f"STOCK#{symbol}"})["Item"]["shares_managed"])


def trades(table):
    return table.query(
        KeyConditionExpression="PK = :pk AND begins_with(SK, :p)",
        ExpressionAttributeValues={":pk": PK, ":p": "TRADE#"},
    )["Items"]


class TestBuy:
    def test_executes_and_moves_cash_into_shares(self, account):
        assert worker.execute_decision(decision("BUY")) == worker.EXECUTED
        assert shares(account) == 3
        assert summary(account)["current_cash_managed"] == Decimal("899.75")
        [trade] = trades(account)
        assert (trade["side"], trade["status"], trade["quantity"]) == ("BUY", "EXECUTED_VIRTUAL", 1)
        assert trade["last_price"] == Decimal("100.25") and trade["notional"] == Decimal("100.25")

    def test_updates_net_worth(self, account):
        worker.execute_decision(decision("BUY"))
        s = summary(account)
        # 899.75 cash + 3 shares * 100.25
        assert s["positions_value"] == Decimal("300.75")
        assert s["current_net_worth"] == Decimal("1200.50")

    def test_insufficient_cash_is_rejected_and_logged(self, account):
        status = worker.execute_decision(decision("BUY", shares=11))  # 11 * 100.25 > 1000
        assert status == worker.REJECTED_CASH
        assert shares(account) == 2
        assert summary(account)["current_cash_managed"] == Decimal("1000")
        assert trades(account)[0]["status"] == worker.REJECTED_CASH

    def test_no_cash_item_at_all(self, table):
        table.put_item(
            Item={
                "PK": PK,
                "SK": "STOCK#AAPL",
                "symbol": "AAPL",
                "last_fetched_price": Decimal("5"),
            }
        )
        assert worker.execute_decision(decision("BUY")) == worker.REJECTED_CASH


class TestSell:
    def test_executes_and_returns_cash(self, account):
        assert worker.execute_decision(decision("SELL", shares=2)) == worker.EXECUTED
        assert shares(account) == 0
        assert summary(account)["current_cash_managed"] == Decimal("1200.50")

    def test_cannot_sell_more_than_held(self, account):
        assert worker.execute_decision(decision("SELL", shares=3)) == worker.REJECTED_SHARES
        assert shares(account) == 2
        assert summary(account)["current_cash_managed"] == Decimal("1000")


class TestRejections:
    def test_no_price(self, account):
        account.update_item(
            Key={"PK": PK, "SK": "STOCK#AAPL"}, UpdateExpression="REMOVE last_fetched_price"
        )
        assert worker.execute_decision(decision()) == worker.REJECTED_NO_PRICE

    def test_unknown_symbol(self, account):
        assert worker.execute_decision(decision(symbol="NOPE")) == worker.REJECTED_SYMBOL

    def test_deleted_symbol(self, account):
        account.update_item(
            Key={"PK": PK, "SK": "STOCK#AAPL"},
            UpdateExpression="SET is_deleted = :t",
            ExpressionAttributeValues={":t": True},
        )
        assert worker.execute_decision(decision()) == worker.REJECTED_SYMBOL

    def test_bad_action(self, account):
        assert worker.execute_decision(decision("HODL")) == worker.REJECTED_ACTION

    def test_wrong_message_type_raises(self, account):
        with pytest.raises(ValueError):
            worker.execute_decision({"type": "NEW_NEWS"})

    def test_quantity_must_be_positive(self, account):
        with pytest.raises(ValueError):
            worker.execute_decision(decision(shares=-1, **{}))

    @pytest.mark.parametrize("quantity", [0, True, 1.5, "NaN"])
    def test_invalid_quantities_never_move_balances(self, account, quantity):
        with pytest.raises(ValueError):
            worker.execute_decision(decision(shares=quantity))
        assert shares(account) == 2
        assert trades(account) == []

    @pytest.mark.parametrize("decision_id", [None, "", "   "])
    def test_decisions_require_a_stable_id(self, account, decision_id):
        with pytest.raises(ValueError, match="decision_id"):
            worker.execute_decision(decision(decision_id=decision_id))
        assert shares(account) == 2
        assert trades(account) == []


class TestIdempotency:
    def test_same_decision_executes_once(self, account):
        assert worker.execute_decision(decision()) == worker.EXECUTED
        assert worker.execute_decision(decision()) == worker.DUPLICATE
        assert shares(account) == 3
        assert summary(account)["current_cash_managed"] == Decimal("899.75")
        assert len(trades(account)) == 1

    def test_rejections_are_idempotent_too(self, account):
        assert worker.execute_decision(decision("SELL", shares=9)) == worker.REJECTED_SHARES
        assert worker.execute_decision(decision("SELL", shares=9)) == worker.DUPLICATE
        assert len(trades(account)) == 1

    def test_distinct_decisions_both_execute(self, account):
        worker.execute_decision(decision(decision_id="d1"))
        worker.execute_decision(decision(decision_id="d2"))
        assert shares(account) == 4

    def test_default_quantity_from_environment(self, account, monkeypatch):
        monkeypatch.setenv("TRADE_QUANTITY", "2")
        worker.execute_decision(decision())
        assert shares(account) == 4


class TestQueueLoop:
    def publish(self, cloud, message):
        from common import messages

        messages.publish(cloud.system_topic_arn, message)

    def test_poll_once_executes_and_deletes(self, cloud, account, sqs):
        self.publish(cloud, decision("BUY"))
        url = cloud.queue_urls["TradeDecisionQueue"]
        assert worker.poll_once(url) == 1
        assert shares(account) == 3
        attrs = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["All"])["Attributes"]
        assert attrs["ApproximateNumberOfMessages"] == "0"
        assert attrs["ApproximateNumberOfMessagesNotVisible"] == "0"

    def test_poll_empty_queue(self, cloud):
        assert worker.poll_once(cloud.queue_urls["TradeDecisionQueue"]) == 0

    def test_failed_message_is_left_for_retry_and_dlq(self, cloud, account, sqs):
        """An unhandleable message is not deleted: it is retried, then dead-lettered."""
        self.publish(cloud, {**decision("BUY"), "type": "AUTO_TRADE_DECISION", "shares": "many"})
        url = cloud.queue_urls["TradeDecisionQueue"]
        worker.poll_once(url)
        attrs = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["All"])["Attributes"]
        assert attrs["ApproximateNumberOfMessagesNotVisible"] == "1"

    def test_queue_url_resolution(self, cloud, monkeypatch):
        monkeypatch.delenv("TRADE_DECISION_QUEUE_URL", raising=False)
        assert worker.queue_url().endswith("/TradeDecisionQueue")
        monkeypatch.setenv("TRADE_DECISION_QUEUE_URL", "http://explicit")
        assert worker.queue_url() == "http://explicit"


def test_message_envelope_roundtrip(cloud):
    from common import messages

    messages.publish(cloud.system_topic_arn, decision())
    [raw] = boto3.client("sqs").receive_message(QueueUrl=cloud.queue_urls["TradeDecisionQueue"])[
        "Messages"
    ]
    parsed = messages.parse_sqs_record({"body": raw["Body"]})
    assert parsed["decision_id"] == "d1" and json.dumps(parsed)
