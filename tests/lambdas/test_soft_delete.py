"""Lambda E (soft delete) and the daily retention cleaner share common.retention."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from common import retention, timeutil
from news_retention_cleaner.main import handler as cleaner
from soft_delete.main import handler as soft_delete
from tests.helpers import sqs_event

ACCOUNT = "123456789012"
PK = f"ACCOUNT#{ACCOUNT}"
TODAY = datetime(2025, 12, 31, 12, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def frozen_clock(monkeypatch):
    monkeypatch.setattr(timeutil, "now", lambda: TODAY)


def day(days_old: int) -> str:
    return (TODAY - timedelta(days=days_old)).strftime("%Y-%m-%d")


def put_news(s3, cloud, symbol, days_old, name="n"):
    key = f"news/{symbol}/{day(days_old)}/{name}-{days_old}.json"
    s3.put_object(Bucket=cloud.bucket_name, Key=key, Body=b"{}")
    return key


def keys_in_bucket(s3, cloud):
    listing = s3.list_objects_v2(Bucket=cloud.bucket_name)
    return {o["Key"] for o in listing.get("Contents", [])}


def tags(s3, cloud, key):
    return {
        t["Key"]: t["Value"]
        for t in s3.get_object_tagging(Bucket=cloud.bucket_name, Key=key)["TagSet"]
    }


def add_stock(table, symbol="AAPL", **attrs):
    table.put_item(
        Item={"PK": PK, "SK": f"STOCK#{symbol}", "symbol": symbol, "is_deleted": False, **attrs}
    )


def stock(table, symbol="AAPL"):
    return table.get_item(Key={"PK": PK, "SK": f"STOCK#{symbol}"}).get("Item")


class TestRetentionRules:
    def test_fresh_news_untouched_old_tagged_ancient_deleted(self, cloud, s3):
        fresh = put_news(s3, cloud, "AAPL", 5)
        month = put_news(s3, cloud, "AAPL", 25)  # > 3 weeks
        ancient = put_news(s3, cloud, "AAPL", 50)  # > 6 weeks
        other = put_news(s3, cloud, "MSFT", 50)  # another symbol: never touched

        counts = retention.cleanup_news_for_symbol(cloud.bucket_name, "AAPL")

        assert counts == {"soft_tagged": 1, "hard_deleted": 1}
        assert keys_in_bucket(s3, cloud) == {fresh, month, other}
        assert ancient not in keys_in_bucket(s3, cloud)
        assert tags(s3, cloud, month) == {"deleted": "true"}
        assert tags(s3, cloud, fresh) == {}

    def test_boundaries(self, cloud, s3):
        exactly_3w = put_news(s3, cloud, "AAPL", 21)
        exactly_6w = put_news(s3, cloud, "AAPL", 42)
        just_under_3w = put_news(s3, cloud, "AAPL", 20)
        counts = retention.cleanup_news_for_symbol(cloud.bucket_name, "AAPL")
        assert counts == {"soft_tagged": 1, "hard_deleted": 1}
        assert exactly_6w not in keys_in_bucket(s3, cloud)
        assert tags(s3, cloud, exactly_3w) == {"deleted": "true"}
        assert tags(s3, cloud, just_under_3w) == {}

    def test_second_run_does_not_retag(self, cloud, s3):
        put_news(s3, cloud, "AAPL", 30)
        retention.cleanup_news_for_symbol(cloud.bucket_name, "AAPL")
        again = retention.cleanup_news_for_symbol(cloud.bucket_name, "AAPL")
        assert again == {"soft_tagged": 0, "hard_deleted": 0}

    def test_unparseable_date_is_soft_deleted(self, cloud, s3):
        key = "news/AAPL/not-a-date/x.json"
        s3.put_object(Bucket=cloud.bucket_name, Key=key, Body=b"{}")
        counts = retention.cleanup_news_for_symbol(cloud.bucket_name, "AAPL")
        assert counts["soft_tagged"] == 1


class TestSoftDeleteLambda:
    def message(self, symbol="AAPL", type_="MARK_STOCK_DELETED"):
        return {"type": type_, "account_id": ACCOUNT, "symbol": symbol}

    def test_marks_stock_deleted_and_cleans_news(self, cloud, table, s3):
        add_stock(table)
        put_news(s3, cloud, "AAPL", 30)
        put_news(s3, cloud, "AAPL", 60)

        assert soft_delete(sqs_event(self.message()), None) == {"batchItemFailures": []}

        item = stock(table)
        assert item["is_deleted"] is True
        assert item["deleted_news_count"] == 2

    def test_redelivery_does_not_inflate_the_counter(self, cloud, table, s3):
        add_stock(table)
        put_news(s3, cloud, "AAPL", 30)
        soft_delete(sqs_event(self.message()), None)
        soft_delete(sqs_event(self.message(type_="CLEANUP_STOCK")), None)
        assert stock(table)["deleted_news_count"] == 1

    def test_unknown_stock_creates_no_phantom_item(self, cloud, table):
        """Regression: update_item upserted a stub item for symbols that never existed."""
        assert soft_delete(sqs_event(self.message("NOPE")), None) == {"batchItemFailures": []}
        assert stock(table, "NOPE") is None

    def test_other_message_types_are_ignored(self, cloud, table):
        add_stock(table)
        soft_delete(sqs_event({"type": "NEW_NEWS", "account_id": ACCOUNT, "symbol": "AAPL"}), None)
        assert stock(table)["is_deleted"] is False

    def test_bad_symbol_is_a_failure(self, cloud, table):
        result = soft_delete(sqs_event(self.message("bad symbol!")), None)
        assert result["batchItemFailures"] == [{"itemIdentifier": "m-0"}]


class TestRetentionCleanerLambda:
    def test_only_deleted_stocks_are_cleaned(self, cloud, table, s3):
        add_stock(table, "OLD", is_deleted=True)
        add_stock(table, "LIVE", is_deleted=False)
        gone = put_news(s3, cloud, "OLD", 50)
        kept = put_news(s3, cloud, "LIVE", 50)

        result = cleaner({}, None)

        assert result == {"stocks": 1, "soft_tagged": 0, "hard_deleted": 1}
        assert gone not in keys_in_bucket(s3, cloud)
        assert kept in keys_in_bucket(s3, cloud)

    def test_no_deleted_stocks(self, cloud, table):
        assert cleaner(None, None) == {"stocks": 0, "soft_tagged": 0, "hard_deleted": 0}
