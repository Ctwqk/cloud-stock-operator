"""Unit tests for the shared helpers in lambda/common."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

import boto3
import pytest

from common import aws, ddb, jsonutil, keys, messages, news, timeutil, topology
from tests.conftest import load_fixture
from tests.helpers import sns_sqs_record, sqs_event


class TestTimeutil:
    def test_iso_utc_is_fixed_width_and_sortable(self):
        early = timeutil.iso_utc(datetime(2025, 1, 2, 3, 4, 5, 6, tzinfo=UTC))
        late = timeutil.iso_utc(datetime(2025, 1, 2, 3, 4, 5, 600000, tzinfo=UTC))
        assert early == "2025-01-02T03:04:05.000006Z"
        assert len(early) == len(late) and early < late

    @pytest.mark.parametrize(
        "text, expected",
        [
            ("2025-12-01T14:05:00Z", "2025-12-01T14:05:00.000000Z"),
            ("2025-12-01T14:05:00+00:00", "2025-12-01T14:05:00.000000Z"),
            ("2025-12-01T16:05:00+02:00", "2025-12-01T14:05:00.000000Z"),
            ("2025-12-01T14:05:00.5", "2025-12-01T14:05:00.500000Z"),  # naive -> UTC
        ],
    )
    def test_normalize_iso(self, text, expected):
        assert timeutil.normalize_iso(text) == expected

    @pytest.mark.parametrize("bad", [None, "", "yesterday", 42])
    def test_unparseable_returns_none(self, bad):
        assert timeutil.parse_iso(bad) is None


class TestJson:
    def test_decimal_roundtrip(self):
        payload = {"a": Decimal("3"), "b": Decimal("1.25"), "c": [Decimal("0")]}
        assert json.loads(jsonutil.dumps(payload)) == {"a": 3, "b": 1.25, "c": [0]}

    def test_loads_keeps_floats_as_decimal(self):
        assert jsonutil.loads('{"x": 0.1}')["x"] == Decimal("0.1")

    def test_rejects_unknown_types(self):
        with pytest.raises(TypeError):
            jsonutil.dumps({"x": object()})


class TestDdbHelpers:
    @pytest.mark.parametrize("value", [True, "abc", float("nan"), float("inf"), None])
    def test_to_decimal_rejects_garbage(self, value):
        with pytest.raises(ValueError, match="value"):
            ddb.to_decimal(value)

    def test_to_int_rejects_fractions(self):
        assert ddb.to_int("5") == 5
        with pytest.raises(ValueError, match="integer"):
            ddb.to_int("1.5")


class TestKeys:
    def test_key_design(self):
        assert keys.account_pk("1") == "ACCOUNT#1"
        assert keys.stock_sk("aapl") == "STOCK#AAPL"
        assert keys.symbol_from_stock_sk("STOCK#BRK.B") == "BRK.B"
        assert keys.score_sk("aapl", "T") == "SCORE#AAPL#T"
        assert keys.trade_sk("T", "d1") == "TRADE#T#d1"
        assert keys.op_sk("x") == "OP#x"


class TestNews:
    def test_news_id_is_stable_and_symbol_case_insensitive(self):
        a = news.news_id("aapl", " Headline ", "2025-12-01T00:00:00.000000Z")
        b = news.news_id("AAPL", "Headline", "2025-12-01T00:00:00.000000Z")
        assert a == b and len(a) == 32

    def test_news_key_uses_published_day(self):
        key = news.news_key("aapl", "2025-12-01T23:59:59Z", "abc")
        assert key == "news/AAPL/2025-12-01/abc.json"


class TestMessages:
    def test_parse_sns_envelope_from_real_event_shape(self):
        record = load_fixture("events/sqs_sns_new_news.json")["Records"][0]
        message = messages.parse_sqs_record(record)
        assert message["type"] == "NEW_NEWS"
        assert message["symbol"] == "AAPL"

    def test_parse_raw_delivery_body(self):
        assert messages.parse_sqs_record({"body": '{"type": "X"}'}) == {"type": "X"}

    @pytest.mark.parametrize("body", ["not json", "[1, 2]", '"text"'])
    def test_malformed_bodies_raise(self, body):
        with pytest.raises(messages.MalformedMessage):
            messages.parse_sqs_record({"body": body})

    @pytest.mark.parametrize(
        "raw, ok",
        [
            ("aapl", True),
            ("BRK.B", True),
            ("", False),
            ("a b", False),
            ("../x", False),
            ("TOOLONGSYMBOLX", False),
        ],
    )
    def test_normalize_symbol(self, raw, ok):
        if ok:
            assert messages.normalize_symbol(raw) == raw.upper()
        else:
            with pytest.raises(ValueError):
                messages.normalize_symbol(raw)

    def test_process_sqs_batch_reports_only_failed_records(self):
        seen = []

        def handle(msg, record):
            if msg["n"] == 2:
                raise RuntimeError("boom")
            seen.append(msg["n"])

        event = sqs_event({"type": "T", "n": 1}, {"type": "T", "n": 2}, {"type": "T", "n": 3})
        event["Records"].append({"messageId": "bad", "body": "{{{"})

        result = messages.process_sqs_batch(event, handle)

        assert seen == [1, 3]
        assert result == {
            "batchItemFailures": [{"itemIdentifier": "m-1"}, {"itemIdentifier": "bad"}]
        }

    def test_publish_requires_topic(self):
        with pytest.raises(aws.ConfigError):
            messages.publish("", {"type": "X"})


class TestAws:
    def test_require_env(self, monkeypatch):
        monkeypatch.delenv("SOME_SETTING", raising=False)
        with pytest.raises(aws.ConfigError, match="SOME_SETTING"):
            aws.require_env("SOME_SETTING")
        monkeypatch.setenv("SOME_SETTING", "v")
        assert aws.require_env("SOME_SETTING") == "v"

    def test_account_id_override_beats_sts(self, monkeypatch):
        monkeypatch.setenv("ACCOUNT_ID", "999")
        assert aws.get_account_id() == "999"

    def test_account_id_from_sts(self, cloud):
        assert aws.get_account_id() == "123456789012"

    def test_client_override(self):
        sentinel = object()
        aws.register_override("lambda", sentinel)
        assert aws.client("lambda") is sentinel
        aws.register_override("lambda", None)
        assert aws.client("lambda") is not sentinel


class TestTopologyRouting:
    """The SNS filter policies deliver every message type to exactly one queue."""

    def all_types(self):
        return [t for q in topology.QUEUES for t in q.message_types]

    def test_no_message_type_has_two_queues(self):
        types = self.all_types()
        assert len(types) == len(set(types))

    def test_every_lambda_consumer_exists_on_disk(self):
        from pathlib import Path

        lambda_dir = Path(__file__).parents[2] / "lambda"
        for spec in topology.QUEUES:
            if spec.consumer != topology.LOCAL_WORKER:
                assert (lambda_dir / spec.consumer / "main.py").exists()

    @pytest.mark.parametrize("spec", topology.QUEUES, ids=lambda s: s.name)
    def test_message_lands_only_in_its_queue(self, cloud, spec):
        topic_arn = (
            cloud.external_topic_arn
            if spec.topic == topology.EXTERNAL_TOPIC
            else cloud.system_topic_arn
        )
        for message_type in spec.message_types:
            messages.publish(topic_arn, {"type": message_type, "marker": message_type})

        sqs = boto3.client("sqs")
        for other in topology.QUEUES:
            received = sqs.receive_message(
                QueueUrl=cloud.queue_urls[other.name], MaxNumberOfMessages=10
            ).get("Messages", [])
            bodies = [json.loads(json.loads(m["Body"])["Message"])["type"] for m in received]
            if other.name == spec.name:
                assert sorted(bodies) == sorted(spec.message_types)
            else:
                assert not set(bodies) & set(spec.message_types)

    def test_unrouted_type_is_dropped(self, cloud):
        messages.publish(cloud.system_topic_arn, {"type": "SOMETHING_ELSE"})
        sqs = boto3.client("sqs")
        for url in cloud.queue_urls.values():
            assert not sqs.receive_message(QueueUrl=url).get("Messages")


def test_sns_record_helper_builds_valid_envelope():
    record = sns_sqs_record({"type": "T"})
    assert json.loads(json.loads(record["body"])["Message"]) == {"type": "T"}
