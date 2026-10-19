"""End-to-end tests of the whole event pipeline, entirely in-process.

    API request -> control plane -> ExternalOps SNS/SQS -> stock_info_writer -> DynamoDB
                -> STOCK_ADDED -> news_fetcher -> NEW_NEWS -> news_writer -> S3 -> NEWS_STORED
                -> sentiment_scorer -> score / AUTO_TRADE_DECISION -> TradeDecisionQueue
                -> local trade worker -> virtual trade + net worth

All Lambda handlers are the real ones, AWS is moto, and the routing comes from
``common.topology``. The fixture provider (local_app/fixtures/market.json) supplies news.
"""

from __future__ import annotations

import json
import threading
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from werkzeug.serving import make_server

import local_cloud
import local_trade_worker as worker
from common import timeutil
from tests.helpers import api_event

FIXTURE = Path(__file__).parents[2] / "local_app" / "fixtures" / "market.json"
ACCOUNT = "123456789012"
PK = f"ACCOUNT#{ACCOUNT}"


@pytest.fixture
def pipeline(local_cloud_runtime, monkeypatch):
    monkeypatch.setenv("NEWS_PROVIDER", "fixture")
    monkeypatch.setenv("NEWS_FIXTURE_PATH", str(FIXTURE))
    return local_cloud_runtime


def call(runtime, method, path, body=None):
    response = runtime.invoke("control_plane_api", api_event(method, path, body))
    return response["statusCode"], json.loads(response["body"])


def settle(runtime, rounds=5):
    """Run Lambdas to quiescence (a Lambda can enqueue work for another one)."""
    for _ in range(rounds):
        if runtime.drain() == 0:
            return


def stock(table, symbol):
    return table.get_item(Key={"PK": PK, "SK": f"STOCK#{symbol}"}).get("Item")


def test_add_stock_flows_through_the_whole_pipeline(pipeline, table, cloud):
    status, body = call(pipeline, "POST", "/watchlist", {"symbol": "tsla", "shares_managed": 5})
    assert status == 202
    settle(pipeline)

    item = stock(table, "TSLA")
    assert item["shares_managed"] == 5
    assert item["last_fetched_price"] == Decimal("341.75")
    assert item["last_headline"]  # the scorer saw the fixture news
    assert call(pipeline, "GET", f"/operations/{body['op_id']}")[1]["status"] == "APPLIED"

    # TSLA fixture news: recall/probe (negative), downgrade (negative), deliveries (neutral).
    sqs = pipeline._sqs
    messages = sqs.receive_message(
        QueueUrl=cloud.queue_urls["TradeDecisionQueue"], MaxNumberOfMessages=10
    )["Messages"]
    decisions = [json.loads(json.loads(m["Body"])["Message"]) for m in messages]
    assert [d["action"] for d in decisions] == ["SELL", "SELL"]
    assert [d["decision_id"] for d in decisions] == [f"{ACCOUNT}:TSLA:1", f"{ACCOUNT}:TSLA:2"]


def test_trades_are_executed_and_net_worth_is_snapshotted(pipeline, table, cloud):
    # fund the account, then watch NVDA (one very positive, one very negative headline)
    call(pipeline, "POST", "/account/cash/adjust", {"delta_cash": 5000, "max_allowed_cash": 10000})
    call(pipeline, "POST", "/watchlist", {"symbol": "NVDA", "shares_managed": 1})
    settle(pipeline)
    while worker.poll_once(cloud.queue_urls["TradeDecisionQueue"]):
        pass

    trades = table.query(
        KeyConditionExpression="PK = :pk AND begins_with(SK, :p)",
        ExpressionAttributeValues={":pk": PK, ":p": "TRADE#"},
    )["Items"]
    assert sorted(t["side"] for t in trades) == ["BUY", "SELL"]
    assert all(t["status"] == "EXECUTED_VIRTUAL" for t in trades)

    # one share bought and one sold at 176.30 -> cash and shares are back where they started
    summary = table.get_item(Key={"PK": PK, "SK": "SUMMARY"})["Item"]
    assert summary["current_cash_managed"] == Decimal("5000")
    assert stock(table, "NVDA")["shares_managed"] == 1
    assert summary["current_net_worth"] == Decimal("5176.30")

    # snapshot + plot: the series now has the managed net worth
    pipeline.invoke("networth_snapshot_writer", local_cloud.scheduled_event())
    plot = pipeline.invoke(
        "networth_plotter",
        {"httpMethod": "GET", "path": "/networth", "queryStringParameters": None},
    )
    body = json.loads(plot["body"])
    assert plot["statusCode"] == 200 and body["points"] == 1


def test_cash_limit_is_enforced_end_to_end(pipeline, table):
    _, first = call(
        pipeline, "POST", "/account/cash/adjust", {"delta_cash": 900, "max_allowed_cash": 1000}
    )
    _, second = call(
        pipeline, "POST", "/account/cash/adjust", {"delta_cash": 200, "max_allowed_cash": 1000}
    )
    settle(pipeline)
    assert call(pipeline, "GET", f"/operations/{first['op_id']}")[1]["status"] == "APPLIED"
    rejected = call(pipeline, "GET", f"/operations/{second['op_id']}")[1]
    assert rejected["status"] == "REJECTED" and "maximum" in rejected["reason"]
    assert table.get_item(Key={"PK": PK, "SK": "SUMMARY"})["Item"]["current_cash_managed"] == 900


def test_second_refresh_does_not_double_count_news(pipeline, table, cloud):
    call(pipeline, "POST", "/watchlist", {"symbol": "AAPL"})
    settle(pipeline)
    before = stock(table, "AAPL")
    decisions_before = int(before.get("decision_count", 0))

    call(pipeline, "POST", "/watchlist/refresh", {})
    time.sleep(0.5)  # refresh runs the fetcher in a background thread
    settle(pipeline)

    after = stock(table, "AAPL")
    assert int(after.get("decision_count", 0)) == decisions_before
    assert after["current_level_score"] == before["current_level_score"]


def test_delete_flow_soft_deletes_and_cleans_old_news(pipeline, table, s3, cloud, monkeypatch):
    call(pipeline, "POST", "/watchlist", {"symbol": "MSFT"})
    settle(pipeline)
    assert s3.list_objects_v2(Bucket=cloud.bucket_name, Prefix="news/MSFT/")["KeyCount"] == 2

    # fast-forward: the fixture news is from Dec 2025, so 'now' = Feb 2026 makes it > 6 weeks old
    monkeypatch.setattr(timeutil, "now", lambda: datetime(2026, 2, 1, tzinfo=UTC) + timedelta(0))
    status, _ = call(pipeline, "DELETE", "/watchlist/MSFT")
    assert status == 202
    settle(pipeline)

    assert stock(table, "MSFT")["is_deleted"] is True
    assert stock(table, "MSFT")["deleted_news_count"] == 2
    assert "Contents" not in s3.list_objects_v2(Bucket=cloud.bucket_name, Prefix="news/MSFT/")
    # deleted stocks are no longer fetched
    assert "MSFT" not in pipeline.invoke("news_fetcher", {})["symbols"]


@pytest.fixture
def running_cloud(pipeline, monkeypatch):
    """The real long-running mode: poller threads + an HTTP API on a random port."""
    monkeypatch.setattr(local_cloud, "LOCAL_RETRY_DELAY_S", 1)
    api = local_cloud.create_api(pipeline, api_key="k")
    server = make_server("127.0.0.1", 0, api, threaded=True)
    threads = [
        threading.Thread(target=server.serve_forever, daemon=True),
        threading.Thread(
            target=pipeline.run_forever,
            kwargs={"fetch_interval": 3600, "snapshot_interval": 3600, "retention_interval": 3600},
            daemon=True,
        ),
    ]
    for thread in threads:
        thread.start()
    yield f"http://127.0.0.1:{server.server_port}"
    pipeline.stop()
    server.shutdown()


def test_web_app_to_trades_over_real_http(running_cloud, table, cloud, monkeypatch):
    """Browser form -> web app -> HTTP -> control plane -> pollers -> ... -> UI shows the result."""
    import web_app

    monkeypatch.setenv("MGMT_API_BASE_URL", running_cloud + "/mgmt")
    monkeypatch.setenv("NETWORTH_API_BASE_URL", running_cloud)
    monkeypatch.setenv("MGMT_API_KEY", "k")
    monkeypatch.setattr(web_app, "OPERATION_POLL_S", 15)
    client = web_app.app.test_client()

    funded = client.post(
        "/account/cash/adjust", data={"delta_cash": "3000", "max_allowed_cash": "5000"}
    )
    assert "applied" in funded.headers["Location"]
    added = client.post("/watchlist/add", data={"symbol": "AAPL", "shares_managed": "0"})
    assert "applied" in added.headers["Location"]

    deadline = time.time() + 20
    html = ""
    while time.time() < deadline:
        worker.poll_once(cloud.queue_urls["TradeDecisionQueue"])
        html = client.get("/").get_data(as_text=True)
        if "executed virtual" in html:
            break
        time.sleep(0.3)

    assert "executed virtual" in html
    assert 'class="pos">BUY' in html  # positive AAPL headlines -> BUY decisions
    assert "232.45" in html  # last price from the fixture provider
