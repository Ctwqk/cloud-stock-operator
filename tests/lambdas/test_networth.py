"""Net-worth snapshot writer and plotter."""

from __future__ import annotations

import json
from decimal import Decimal

import pytest

from common import portfolio
from networth_plotter.main import handler as plotter
from networth_snapshot_writer.main import handler as snapshot
from tests.conftest import load_fixture
from tests.helpers import api_event

ACCOUNT = "123456789012"
PK = f"ACCOUNT#{ACCOUNT}"
PNG_MAGIC = b"\x89PNG\r\n\x1a\n"


def seed(table, cash="1000", **stocks):
    table.put_item(Item={"PK": PK, "SK": "SUMMARY", "current_cash_managed": Decimal(cash)})
    for symbol, (shares, price) in stocks.items():
        table.put_item(
            Item={
                "PK": PK,
                "SK": f"STOCK#{symbol}",
                "symbol": symbol,
                "shares_managed": shares,
                "last_fetched_price": Decimal(price),
            }
        )


class TestPortfolio:
    def test_positions_value_ignores_bad_rows(self):
        items = [
            {"shares_managed": 2, "last_fetched_price": Decimal("10.5")},
            {"shares_managed": 3},  # no price -> contributes 0
            {"shares_managed": "x", "last_fetched_price": 1},  # corrupt -> skipped
        ]
        assert portfolio.positions_value(items) == Decimal("21.0")

    def test_refresh_summary_without_summary_item(self, cloud, table):
        table.put_item(
            Item={
                "PK": PK,
                "SK": "STOCK#A",
                "shares_managed": 2,
                "last_fetched_price": Decimal("3"),
            }
        )
        result = portfolio.refresh_summary(ACCOUNT)
        assert result == {
            "cash": Decimal("0"),
            "positions_value": Decimal("6"),
            "net_worth": Decimal("6"),
        }


class TestSnapshotWriter:
    def test_snapshot_uses_live_prices(self, cloud, table):
        """Regression: net worth used a `positions_value` that price updates never refreshed."""
        seed(table, "1000", AAPL=(10, "200.50"), MSFT=(2, "400"))
        event = load_fixture("events/eventbridge_scheduled.json")

        result = snapshot(event, None)

        assert result["net_worth"] == pytest.approx(1000 + 2005 + 800)
        item = table.get_item(Key={"PK": PK, "SK": "NETWORTH#2025-12-01T12:15:00.000000Z"})["Item"]
        assert item["net_worth"] == Decimal("3805.00")
        assert item["cash"] == Decimal("1000")
        assert item["positions_value"] == Decimal("2805.00")

    def test_retry_of_the_same_event_does_not_add_a_second_point(self, cloud, table):
        seed(table)
        event = load_fixture("events/eventbridge_scheduled.json")
        snapshot(event, None)
        snapshot(event, None)
        points = table.query(
            KeyConditionExpression="PK = :pk AND begins_with(SK, :p)",
            ExpressionAttributeValues={":pk": PK, ":p": "NETWORTH#"},
        )["Items"]
        assert len(points) == 1

    def test_summary_item_is_refreshed(self, cloud, table):
        seed(table, "10", AAPL=(1, "5"))
        snapshot({}, None)
        summary = table.get_item(Key={"PK": PK, "SK": "SUMMARY"})["Item"]
        assert summary["current_net_worth"] == Decimal("15")

    def test_works_without_any_data(self, cloud, table):
        assert snapshot({}, None)["net_worth"] == 0


def add_points(table, *points):
    for timestamp, value in points:
        table.put_item(Item={"PK": PK, "SK": f"NETWORTH#{timestamp}", "net_worth": Decimal(value)})


class TestPlotter:
    def call(self, **query):
        response = plotter(api_event("GET", "/networth", query=query or None), None)
        return response["statusCode"], json.loads(response["body"])

    def test_renders_png_to_s3_and_returns_presigned_url(self, cloud, table, s3):
        add_points(
            table,
            ("2025-12-01T10:00:00.000000Z", "100"),
            ("2025-12-01T11:00:00.000000Z", "110"),
            ("2025-12-01T12:00:00.000000Z", "105"),
        )
        status, body = self.call()
        assert status == 200
        assert body["points"] == 3
        assert body["s3_key"].startswith(f"plots/networth/{ACCOUNT}/")
        assert body["url"].startswith("http") and body["s3_key"] in body["url"]
        png = s3.get_object(Bucket=cloud.bucket_name, Key=body["s3_key"])["Body"].read()
        assert png.startswith(PNG_MAGIC) and len(png) > 1000

    def test_time_window_is_inclusive_and_format_insensitive(self, cloud, table):
        add_points(
            table,
            ("2025-12-01T09:00:00.000000Z", "1"),
            ("2025-12-01T10:00:00.000000Z", "2"),
            ("2025-12-01T11:00:00.000000Z", "3"),
            ("2025-12-01T12:00:00.000000Z", "4"),
        )
        # offsets and 'Z' both accepted: 10:00Z .. 11:00Z
        status, body = self.call(
            start_time="2025-12-01T12:00:00+02:00", end_time="2025-12-01T11:00:00Z"
        )
        assert status == 200 and body["points"] == 2

    def test_empty_series_still_renders(self, cloud, s3):
        status, body = self.call()
        assert status == 200 and body["points"] == 0
        png = s3.get_object(Bucket=cloud.bucket_name, Key=body["s3_key"])["Body"].read()
        assert png.startswith(PNG_MAGIC)

    @pytest.mark.parametrize(
        "query",
        [
            {"start_time": "yesterday"},
            {"end_time": "2025-13-45"},
            {"start_time": "2025-12-02T00:00:00Z", "end_time": "2025-12-01T00:00:00Z"},
        ],
    )
    def test_bad_input_is_a_400(self, cloud, query):
        status, body = self.call(**query)
        assert status == 400 and "error" in body

    def test_other_accounts_are_not_visible(self, cloud, table):
        table.put_item(
            Item={
                "PK": "ACCOUNT#other",
                "SK": "NETWORTH#2025-12-01T10:00:00.000000Z",
                "net_worth": 1,
            }
        )
        _, body = self.call()
        assert body["points"] == 0
