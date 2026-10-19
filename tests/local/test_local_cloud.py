"""The local runtime (queue pollers, 'Lambda' invoker, API Gateway shim)."""

from __future__ import annotations

import json
import time

import boto3
import pytest

import local_cloud
from common import messages, topology
from tests.helpers import api_event


def test_provision_is_idempotent(cloud):
    again = local_cloud.provision()
    assert again.queue_urls == cloud.queue_urls
    assert again.system_topic_arn == cloud.system_topic_arn


def test_provision_creates_everything_in_the_topology(cloud):
    sqs = boto3.client("sqs")
    names = {url.rsplit("/", 1)[-1] for url in sqs.list_queues()["QueueUrls"]}
    expected = {spec.name for spec in topology.QUEUES} | {f"{s.name}DLQ" for s in topology.QUEUES}
    assert names == expected
    ttl = boto3.client("dynamodb").describe_time_to_live(TableName=cloud.table_name)
    assert ttl["TimeToLiveDescription"]["AttributeName"] == "ttl"


class TestQueuePolling:
    def test_partial_batch_failure_keeps_only_the_failed_message(
        self, cloud, local_cloud_runtime, sqs, monkeypatch
    ):
        spec = topology.queue_by_name("NewsIngestQueue")
        calls = []

        def fake_handler(event, context):
            calls.append(len(event["Records"]))
            failing = [
                r["messageId"]
                for r in event["Records"]
                if json.loads(json.loads(r["body"])["Message"])["n"] == 2
            ]
            return {"batchItemFailures": [{"itemIdentifier": i} for i in failing]}

        monkeypatch.setitem(local_cloud_runtime._handlers, spec.consumer, fake_handler)
        for n in (1, 2, 3):
            messages.publish(cloud.system_topic_arn, {"type": "NEW_NEWS", "n": n})

        assert local_cloud_runtime.poll_queue(spec) == 3

        url = cloud.queue_urls[spec.name]
        attrs = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["All"])["Attributes"]
        assert attrs["ApproximateNumberOfMessages"] == "0"
        assert attrs["ApproximateNumberOfMessagesNotVisible"] == "1"  # only n=2 remains

    def test_handler_exception_retries_whole_batch_then_dead_letters(
        self, cloud, local_cloud_runtime, sqs, monkeypatch
    ):
        spec = topology.queue_by_name("NewsIngestQueue")
        monkeypatch.setattr(local_cloud, "LOCAL_RETRY_DELAY_S", 0)

        def always_fails(event, context):
            raise RuntimeError("boom")

        monkeypatch.setitem(local_cloud_runtime._handlers, spec.consumer, always_fails)
        messages.publish(cloud.system_topic_arn, {"type": "NEW_NEWS", "n": 1})

        for _ in range(spec.max_receive_count + 1):
            local_cloud_runtime.poll_queue(spec)
            time.sleep(0.05)

        dlq = cloud.dlq_urls[spec.name]
        assert (
            sqs.get_queue_attributes(QueueUrl=dlq, AttributeNames=["All"])["Attributes"][
                "ApproximateNumberOfMessages"
            ]
            == "1"
        )

    def test_record_shape_matches_what_lambda_receives(
        self, cloud, local_cloud_runtime, monkeypatch
    ):
        spec = topology.queue_by_name("SentimentQueue")
        seen = {}

        def capture(event, context):
            seen.update(event["Records"][0])
            return {}

        monkeypatch.setitem(local_cloud_runtime._handlers, spec.consumer, capture)
        messages.publish(cloud.system_topic_arn, {"type": "NEWS_STORED"})
        local_cloud_runtime.poll_queue(spec)
        assert {"messageId", "receiptHandle", "body", "attributes", "eventSource"} <= set(seen)
        assert seen["eventSource"] == "aws:sqs"
        assert json.loads(seen["body"])["Type"] == "Notification"

    def test_drain_detects_message_loops(self, cloud, local_cloud_runtime, monkeypatch):
        monkeypatch.setattr(local_cloud_runtime, "poll_queue", lambda spec, wait_seconds=0: 1)
        with pytest.raises(RuntimeError, match="did not drain"):
            local_cloud_runtime.drain(max_rounds=3)

    def test_worker_queue_is_not_drained_by_lambdas(self, cloud, local_cloud_runtime, sqs):
        messages.publish(cloud.system_topic_arn, {"type": "AUTO_TRADE_DECISION"})
        local_cloud_runtime.drain()
        url = cloud.queue_urls["TradeDecisionQueue"]
        attrs = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["All"])["Attributes"]
        assert attrs["ApproximateNumberOfMessages"] == "1"


class TestLambdaClient:
    def test_synchronous_invoke_runs_the_handler(self, cloud, local_cloud_runtime):
        from common import aws

        response = aws.client("lambda").invoke(
            FunctionName="networth_snapshot_writer", Payload=b"{}"
        )
        assert response["StatusCode"] == 200
        assert json.loads(response["Payload"].read())["statusCode"] == 200

    def test_async_invoke_returns_202_and_runs_in_background(
        self, cloud, local_cloud_runtime, table
    ):
        from common import aws

        response = aws.client("lambda").invoke(
            FunctionName="networth_snapshot_writer", InvocationType="Event", Payload=b"{}"
        )
        assert response["StatusCode"] == 202
        deadline = time.time() + 5
        while time.time() < deadline:
            items = table.scan()["Items"]
            if any(i["SK"].startswith("NETWORTH#") for i in items):
                return
            time.sleep(0.05)
        pytest.fail("background invocation never wrote a snapshot")


class TestApiGatewayShim:
    @pytest.fixture
    def client(self, cloud, local_cloud_runtime):
        return local_cloud.create_api(local_cloud_runtime, api_key="secret").test_client()

    def test_requires_api_key(self, client):
        response = client.post("/mgmt/watchlist", json={"symbol": "AAPL"})
        assert response.status_code == 403 and response.get_json() == {"message": "Forbidden"}

    def test_proxies_to_control_plane_like_api_gateway(self, client):
        response = client.post(
            "/mgmt/watchlist", json={"symbol": "AAPL"}, headers={"x-api-key": "secret"}
        )
        assert response.status_code == 202
        assert response.get_json()["symbol"] == "AAPL"

    def test_error_codes_pass_through(self, client):
        response = client.post("/mgmt/watchlist", json={}, headers={"x-api-key": "secret"})
        assert response.status_code == 400

    def test_networth_route(self, client):
        response = client.get("/networth", headers={"x-api-key": "secret"})
        assert response.status_code == 200 and response.get_json()["points"] == 0

    def test_invoke_helper_is_limited_to_known_functions(self, client):
        assert client.post("/_local/invoke/rm_rf").status_code == 404
        ok = client.post("/_local/invoke/news_retention_cleaner", json={})
        assert ok.status_code == 200

    def test_health_and_drain(self, client):
        assert client.get("/_local/health").get_json()["status"] == "ok"
        assert client.post("/_local/drain").get_json() == {"processed": 0}

    def test_open_when_no_key_configured(self, cloud, local_cloud_runtime):
        open_client = local_cloud.create_api(local_cloud_runtime).test_client()
        assert open_client.post("/mgmt/watchlist", json={"symbol": "A"}).status_code == 202


def test_scheduled_event_shape():
    event = local_cloud.scheduled_event("2025-12-01T00:00:00Z")
    assert event["source"] == "aws.events" and event["time"] == "2025-12-01T00:00:00Z"
    assert api_event("GET", "/")["httpMethod"] == "GET"
