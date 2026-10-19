"""Idempotent scoring + threshold decisions (common.scoring)."""

from __future__ import annotations

import json

import boto3
import pytest

from common import messages, scoring

ACCOUNT = "123456789012"
PK = f"ACCOUNT#{ACCOUNT}"


def put_stock(table, symbol="AAPL", **attrs):
    item = {
        "PK": PK,
        "SK": f"STOCK#{symbol}",
        "symbol": symbol,
        "is_deleted": False,
        "current_level_score": 0,
        "threshold_abs": 3,
        **attrs,
    }
    table.put_item(Item=item)
    return item


def stock(table, symbol="AAPL"):
    return table.get_item(Key={"PK": PK, "SK": f"STOCK#{symbol}"})["Item"]


def decisions(cloud):
    sqs = boto3.client("sqs")
    out = []
    while True:
        batch = sqs.receive_message(
            QueueUrl=cloud.queue_urls["TradeDecisionQueue"], MaxNumberOfMessages=10
        ).get("Messages", [])
        if not batch:
            return out
        out += [json.loads(json.loads(m["Body"])["Message"]) for m in batch]


def apply(cloud, delta=1, dedupe_id="n1", symbol="AAPL", **kwargs):
    return scoring.apply_sentiment(
        account_id=ACCOUNT,
        symbol=symbol,
        delta=delta,
        reason="TEST",
        dedupe_id=dedupe_id,
        topic_arn=cloud.system_topic_arn,
        headline=kwargs.pop("headline", "headline"),
    )


def test_accumulates_score_below_threshold(cloud, table):
    put_stock(table)
    result = apply(cloud, 1, "a")
    assert (result.status, result.score, result.action) == ("applied", 1, None)
    apply(cloud, 1, "b")
    assert int(stock(table)["current_level_score"]) == 2
    assert decisions(cloud) == []


def test_duplicate_dedupe_id_is_applied_once(cloud, table):
    put_stock(table)
    apply(cloud, 1, "same")
    result = apply(cloud, 1, "same")
    assert result.status == "duplicate"
    assert int(stock(table)["current_level_score"]) == 1


def test_buy_decision_when_threshold_reached_then_score_resets(cloud, table):
    put_stock(table, current_level_score=2)
    result = apply(cloud, 1, "x")
    assert (result.action, result.score) == ("BUY", 3)

    [decision] = decisions(cloud)
    assert decision["type"] == messages.AUTO_TRADE_DECISION
    assert decision["action"] == "BUY"
    assert decision["decision_id"] == f"{ACCOUNT}:AAPL:1"
    assert decision["score"] == 3 and decision["threshold_abs"] == 3

    item = stock(table)
    assert int(item["current_level_score"]) == 0
    assert int(item["decision_count"]) == 1


def test_sell_decision_on_negative_threshold(cloud, table):
    put_stock(table, current_level_score=-2, threshold_abs=3)
    result = apply(cloud, -1, "x")
    assert result.action == "SELL"
    assert decisions(cloud)[0]["action"] == "SELL"


def test_neutral_news_after_a_decision_does_not_retrigger(cloud, table):
    """Regression: the score used to stay >= threshold, so every later article re-traded."""
    put_stock(table, current_level_score=2)
    apply(cloud, 1, "up")
    assert len(decisions(cloud)) == 1
    for i in range(3):
        apply(cloud, 0, f"neutral-{i}")
    assert decisions(cloud) == []


def test_decision_ids_increase(cloud, table):
    put_stock(table, threshold_abs=1)
    apply(cloud, 1, "a")
    apply(cloud, 1, "b")
    assert [d["decision_id"] for d in decisions(cloud)] == [
        f"{ACCOUNT}:AAPL:1",
        f"{ACCOUNT}:AAPL:2",
    ]


def test_unknown_and_deleted_symbols_are_ignored(cloud, table):
    assert apply(cloud, 1, "u", symbol="NOPE").status == "unknown_symbol"
    put_stock(table, "OLD", is_deleted=True)
    assert apply(cloud, 1, "d", symbol="OLD").status == "unknown_symbol"
    # no phantom items were created
    assert "Item" not in table.get_item(Key={"PK": PK, "SK": "STOCK#NOPE"})


def test_history_and_last_headline_are_recorded(cloud, table):
    put_stock(table)
    apply(cloud, 1, "h", headline="Apple surges")
    item = stock(table)
    assert item["last_headline"] == "Apple surges"
    assert int(item["last_sentiment"]) == 1
    history = table.query(
        KeyConditionExpression="PK = :pk AND begins_with(SK, :p)",
        ExpressionAttributeValues={":pk": PK, ":p": "SCORE#AAPL#"},
    )["Items"]
    assert [int(h["delta_score"]) for h in history] == [1]


def test_redelivery_after_failed_publish_still_emits_the_decision(cloud, table, monkeypatch):
    """Crash between 'score written' and 'decision published' must not lose the trade."""
    put_stock(table, current_level_score=2)
    real_publish = messages.publish
    calls = {"n": 0}

    def flaky_publish(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("SNS unavailable")
        return real_publish(*args, **kwargs)

    monkeypatch.setattr(messages, "publish", flaky_publish)
    with pytest.raises(RuntimeError):
        apply(cloud, 1, "retry-me")

    # SQS redelivers the same message: marker exists -> status duplicate, decision completes.
    result = apply(cloud, 1, "retry-me")
    assert result.status == "duplicate"
    assert result.action == "BUY"
    assert len(decisions(cloud)) == 1
    assert int(stock(table)["current_level_score"]) == 0


def test_failed_publish_survives_opposite_news(cloud, table, monkeypatch):
    """Later negative news must not erase a BUY awaiting SNS delivery."""
    put_stock(table, threshold_abs=1)
    real_publish = messages.publish

    def unavailable(*args, **kwargs):
        raise RuntimeError("SNS unavailable")

    monkeypatch.setattr(messages, "publish", unavailable)
    with pytest.raises(RuntimeError):
        apply(cloud, 1, "buy")
    monkeypatch.setattr(messages, "publish", real_publish)

    apply(cloud, -1, "sell")
    apply(cloud, 1, "buy")  # retry the original delivery after other news arrived
    sent = {d["decision_id"]: d["action"] for d in decisions(cloud)}
    assert sent == {f"{ACCOUNT}:AAPL:1": "BUY", f"{ACCOUNT}:AAPL:2": "SELL"}
    assert stock(table)["current_level_score"] == 0


def test_concurrent_score_update_gets_distinct_decision_ids(cloud, table, monkeypatch):
    """Interleave another transaction after the outer scorer reads the stock."""
    from common import aws

    put_stock(table, threshold_abs=1)
    client = aws.ddb_client()
    real_transact = client.transact_write_items
    interleaved = False

    def transact(**kwargs):
        nonlocal interleaved
        if not interleaved:
            interleaved = True
            apply(cloud, -1, "concurrent")
        return real_transact(**kwargs)

    monkeypatch.setattr(client, "transact_write_items", transact)
    apply(cloud, 1, "outer")
    sent = decisions(cloud)
    assert [(d["decision_id"], d["action"]) for d in sent] == [
        (f"{ACCOUNT}:AAPL:1", "SELL"),
        (f"{ACCOUNT}:AAPL:2", "BUY"),
    ]
    assert stock(table)["decision_count"] == 2


def test_ambiguous_publish_retry_keeps_original_payload(cloud, table, monkeypatch):
    """SNS accepted the message, but the client lost the acknowledgement."""
    put_stock(table, threshold_abs=1)
    real_publish = messages.publish

    def accepted_then_failed(*args, **kwargs):
        real_publish(*args, **kwargs)
        raise RuntimeError("connection lost after publish")

    monkeypatch.setattr(messages, "publish", accepted_then_failed)
    with pytest.raises(RuntimeError):
        apply(cloud, 1, "buy", headline="original")
    monkeypatch.setattr(messages, "publish", real_publish)
    apply(cloud, -1, "sell")
    apply(cloud, -1, "buy", headline="changed on retry")
    sent = decisions(cloud)
    buys = [d for d in sent if d["decision_id"] == f"{ACCOUNT}:AAPL:1"]
    assert len(buys) == 2 and buys[0] == buys[1]
    assert buys[0]["action"] == "BUY"
    assert stock(table)["decision_count"] == 2
