"""Control-plane API: routing, validation, command publishing, synchronous writes."""

from __future__ import annotations

import base64
import json

import boto3
import pytest

from common import aws
from control_plane_api.main import handler
from stock_info_writer.main import handler as stock_info_writer
from tests.conftest import load_fixture
from tests.helpers import api_event, sqs_event

ACCOUNT = "123456789012"
PK = f"ACCOUNT#{ACCOUNT}"


def call(method, path, body=None, **kwargs):
    response = handler(api_event(method, path, body, **kwargs), None)
    return response["statusCode"], json.loads(response["body"])


def queued(cloud, queue="ExternalOpsQueue"):
    sqs = boto3.client("sqs")
    got = sqs.receive_message(QueueUrl=cloud.queue_urls[queue], MaxNumberOfMessages=10)
    return [json.loads(json.loads(m["Body"])["Message"]) for m in got.get("Messages", [])]


def add_stock(table, symbol="AAPL", **attrs):
    table.put_item(
        Item={
            "PK": PK,
            "SK": f"STOCK#{symbol}",
            "symbol": symbol,
            "is_deleted": False,
            "current_level_score": 0,
            "threshold_abs": 1,
            **attrs,
        }
    )


class TestCommands:
    """Balance-changing endpoints queue a command and return 202 with an op_id."""

    def test_add_watchlist_publishes_command(self, cloud):
        status, body = call("POST", "/watchlist", {"symbol": "aapl", "shares_managed": 3})
        assert status == 202
        assert body["symbol"] == "AAPL" and body["status"] == "PENDING"
        [command] = queued(cloud)
        assert command == {
            "type": "ADD_STOCK",
            "symbol": "AAPL",
            "initial_shares_managed": 3,
            "op_id": body["op_id"],
            "account_id": ACCOUNT,
        }

    def test_add_watchlist_from_real_api_gateway_event(self, cloud):
        event = load_fixture("events/apigw_post_watchlist.json")
        response = handler(event, None)
        assert response["statusCode"] == 202
        assert response["headers"]["Content-Type"] == "application/json"

    def test_command_does_not_touch_dynamodb(self, cloud, table):
        """Regression: the API used to write balances itself *and* publish the same change."""
        call("POST", "/account/cash/adjust", {"delta_cash": 100, "max_allowed_cash": 1000})
        call("POST", "/watchlist", {"symbol": "AAPL"})
        assert table.scan()["Items"] == []

    def test_cash_command_is_applied_once_end_to_end(self, cloud, table):
        _, body = call(
            "POST", "/account/cash/adjust", {"delta_cash": 250.5, "max_allowed_cash": 1000}
        )
        [command] = queued(cloud)
        event = sqs_event(command)
        stock_info_writer(event, None)
        stock_info_writer(event, None)  # SQS redelivery
        summary = table.get_item(Key={"PK": PK, "SK": "SUMMARY"})["Item"]
        assert float(summary["current_cash_managed"]) == 250.5
        status, op = call("GET", f"/operations/{body['op_id']}")
        assert status == 200 and op["status"] == "APPLIED"

    def test_rejected_operation_reports_reason(self, cloud, table):
        _, body = call(
            "POST", "/account/cash/adjust", {"delta_cash": 5000, "max_allowed_cash": 1000}
        )
        stock_info_writer(sqs_event(queued(cloud)[0]), None)
        _, op = call("GET", f"/operations/{body['op_id']}")
        assert op["status"] == "REJECTED" and "maximum" in op["reason"]

    def test_unknown_operation_is_pending(self, cloud):
        assert call("GET", "/operations/doesnotexist") == (
            200,
            {"op_id": "doesnotexist", "status": "PENDING"},
        )

    def test_adjust_shares_command(self, cloud):
        status, _ = call(
            "POST",
            "/account/shares/adjust",
            {"symbol": "msft", "delta_shares": -2, "max_shares_allowed": 10},
        )
        assert status == 202
        assert queued(cloud)[0]["type"] == "ADJUST_MANAGED_SHARES"

    def test_delete_goes_to_the_system_topic(self, cloud):
        status, _ = call("DELETE", "/watchlist/aapl")
        assert status == 202
        [message] = queued(cloud, "CleanupQueue")
        assert message == {"type": "MARK_STOCK_DELETED", "account_id": ACCOUNT, "symbol": "AAPL"}

    @pytest.mark.parametrize(
        "method, path, body, fragment",
        [
            ("POST", "/watchlist", {}, "symbol"),
            ("POST", "/watchlist", {"symbol": "bad symbol"}, "invalid symbol"),
            ("POST", "/watchlist", {"symbol": "A", "shares_managed": -1}, ">= 0"),
            ("POST", "/watchlist", {"symbol": "A", "shares_managed": "1.5"}, "integer"),
            ("POST", "/account/cash/adjust", {"delta_cash": 1}, "max_allowed_cash"),
            ("POST", "/account/cash/adjust", {"delta_cash": "x", "max_allowed_cash": 1}, "number"),
            (
                "POST",
                "/account/shares/adjust",
                {"symbol": "A", "delta_shares": 1},
                "max_shares_allowed",
            ),
            ("POST", "/watchlist/AAPL/threshold", {}, "threshold_abs"),
            ("POST", "/watchlist/AAPL/threshold", {"threshold_abs": 0}, ">= 1"),
            ("POST", "/sentiment", {"symbol": "AAPL"}, "delta_score"),
            ("POST", "/sentiment", {"symbol": "AAPL", "delta_score": 99}, "between"),
            ("DELETE", "/watchlist/bad%20symbol", None, "invalid symbol"),
        ],
    )
    def test_validation_errors_are_400_not_500(self, cloud, method, path, body, fragment):
        status, payload = call(method, path, body)
        assert status == 400 and fragment in payload["error"]

    def test_invalid_json_body(self, cloud):
        event = api_event("POST", "/watchlist")
        event["body"] = "{not json"
        assert handler(event, None)["statusCode"] == 400

    def test_json_array_body_is_rejected(self, cloud):
        event = api_event("POST", "/watchlist")
        event["body"] = "[1]"
        assert handler(event, None)["statusCode"] == 400

    def test_base64_body(self, cloud):
        event = api_event("POST", "/watchlist")
        event["body"] = base64.b64encode(b'{"symbol": "AAPL"}').decode()
        event["isBase64Encoded"] = True
        assert handler(event, None)["statusCode"] == 202


class TestRouting:
    def test_unknown_route_is_404(self, cloud):
        status, payload = call("GET", "/nothing")
        assert status == 404 and "no route" in payload["error"]

    def test_wrong_method_is_404(self, cloud):
        assert call("GET", "/watchlist")[0] == 404

    def test_stage_prefix_is_stripped(self, cloud):
        assert call("POST", "/prod/watchlist", {"symbol": "AAPL"})[0] == 202

    def test_refresh_is_not_mistaken_for_a_symbol(self, cloud):
        class FakeLambda:
            calls: list = []

            def invoke(self, **kwargs):
                self.calls.append(kwargs)
                return {"StatusCode": 202}

        fake = FakeLambda()
        aws.register_override("lambda", fake)
        status, _ = call("POST", "/watchlist/refresh", {})
        assert status == 202
        assert fake.calls[0]["FunctionName"] == "news_fetcher"
        assert fake.calls[0]["InvocationType"] == "Event"
        assert json.loads(fake.calls[0]["Payload"]) == {"account_id": ACCOUNT}

    def test_refresh_failure_is_a_502(self, cloud):
        from botocore.exceptions import ClientError

        class Broken:
            def invoke(self, **kwargs):
                raise ClientError({"Error": {"Code": "ResourceNotFoundException"}}, "Invoke")

        aws.register_override("lambda", Broken())
        assert call("POST", "/watchlist/refresh", {})[0] == 502

    def test_unexpected_errors_do_not_leak_details(self, cloud, monkeypatch):
        monkeypatch.delenv("EXTERNAL_OPS_TOPIC_ARN")
        status, payload = call("POST", "/watchlist", {"symbol": "AAPL"})
        assert status == 500 and payload == {"error": "internal error"}


class TestSynchronousWrites:
    def test_threshold_update(self, cloud, table):
        add_stock(table)
        status, body = call("POST", "/watchlist/aapl/threshold", {"threshold_abs": 4})
        assert status == 200 and body["threshold_abs"] == 4
        assert table.get_item(Key={"PK": PK, "SK": "STOCK#AAPL"})["Item"]["threshold_abs"] == 4

    def test_threshold_for_unknown_stock_is_404_and_creates_nothing(self, cloud, table):
        assert call("POST", "/watchlist/NOPE/threshold", {"threshold_abs": 2})[0] == 404
        assert table.scan()["Items"] == []

    def test_sentiment_applies_and_returns_the_action(self, cloud, table):
        add_stock(table, threshold_abs=2)
        status, body = call(
            "POST", "/sentiment", {"symbol": "AAPL", "delta_score": 1, "event_id": "e1"}
        )
        assert status == 200 and body["new_score"] == 1 and body["action"] is None
        status, body = call(
            "POST", "/sentiment", {"symbol": "AAPL", "delta_score": 1, "event_id": "e2"}
        )
        assert body["action"] == "BUY"
        [decision] = queued(cloud, "TradeDecisionQueue")
        assert decision["action"] == "BUY"

    def test_sentiment_event_id_makes_retries_safe(self, cloud, table):
        add_stock(table, threshold_abs=5)
        call("POST", "/sentiment", {"symbol": "AAPL", "delta_score": 1, "event_id": "same"})
        _, body = call(
            "POST", "/sentiment", {"symbol": "AAPL", "delta_score": 1, "event_id": "same"}
        )
        assert body["message"] == "duplicate ignored" and body["new_score"] == 1

    def test_sentiment_for_unknown_symbol_is_404(self, cloud):
        assert call("POST", "/sentiment", {"symbol": "NOPE", "delta_score": 1})[0] == 404
