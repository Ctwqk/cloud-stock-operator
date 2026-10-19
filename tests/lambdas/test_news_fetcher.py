"""Lambda A: provider normalisation, de-duplication, scheduling modes."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

import boto3
import pytest

from common import messages
from news_fetcher import providers
from news_fetcher.main import handler
from tests.conftest import load_fixture
from tests.helpers import sqs_event

ACCOUNT = "123456789012"
PK = f"ACCOUNT#{ACCOUNT}"
NOW = datetime(2025, 12, 5, 12, 0, tzinfo=UTC)


class FakeProvider:
    def __init__(self, news=None, prices=None, fail_on=()):
        self.news = news or {}
        self.prices = prices or {}
        self.fail_on = set(fail_on)

    def fetch_news(self, symbol, max_items=10):
        if symbol in self.fail_on:
            raise RuntimeError(f"upstream error for {symbol}")
        return self.news.get(symbol, [])[:max_items]

    def last_price(self, symbol):
        return self.prices.get(symbol)


def item(headline, published="2025-12-01T10:00:00.000000Z", body="body"):
    return providers.NewsItem(headline=headline, body=body, source="test", published_at=published)


@pytest.fixture
def use_provider(monkeypatch):
    def install(provider):
        monkeypatch.setattr(providers, "get_provider", lambda: provider)
        return provider

    return install


def put_stock(table, symbol, **attrs):
    table.put_item(Item={"PK": PK, "SK": f"STOCK#{symbol}", "symbol": symbol, **attrs})


def news_messages(cloud):
    sqs = boto3.client("sqs")
    out = []
    while True:
        got = sqs.receive_message(
            QueueUrl=cloud.queue_urls["NewsIngestQueue"], MaxNumberOfMessages=10
        ).get("Messages", [])
        if not got:
            return out
        out += [json.loads(json.loads(m["Body"])["Message"]) for m in got]


class TestNormalizeYFinance:
    def test_nested_content_shape(self):
        raw = load_fixture("yfinance_news_nested.json")
        first = providers.normalize_yfinance_item(raw[0], now=NOW)
        assert first.headline == "Apple unveils new chip"
        assert first.body == "The chip is faster."
        assert first.source == "Reuters"
        assert first.published_at == "2025-12-01T14:05:00.000000Z"

    def test_flat_shape_with_epoch(self):
        raw = load_fixture("yfinance_news_flat.json")
        converted = providers.normalize_yfinance_item(raw[0], now=NOW)
        assert converted.headline == "Tesla recalls vehicles"
        assert converted.source == "Bloomberg"
        assert converted.published_at.startswith("2025-12-01T")

    def test_empty_items_are_dropped(self):
        raw = load_fixture("yfinance_news_nested.json")
        assert providers.normalize_yfinance_item(raw[1], now=NOW) is None

    def test_missing_date_falls_back_to_now(self):
        converted = providers.normalize_yfinance_item({"title": "x"}, now=NOW)
        assert converted.published_at == "2025-12-05T12:00:00.000000Z"
        assert converted.source == "yfinance"

    def test_long_summary_is_truncated(self):
        converted = providers.normalize_yfinance_item({"title": "x", "summary": "y" * 5000})
        assert len(converted.body) == providers.MAX_BODY_CHARS


class TestProviderSelection:
    def test_fixture_provider(self, tmp_path, monkeypatch):
        path = tmp_path / "m.json"
        path.write_text(
            json.dumps(
                {
                    "prices": {"AAPL": 10.25},
                    "news": {"AAPL": [{"headline": "h", "published_at": "2025-12-01T00:00:00Z"}]},
                }
            )
        )
        monkeypatch.setenv("NEWS_PROVIDER", "fixture")
        monkeypatch.setenv("NEWS_FIXTURE_PATH", str(path))
        provider = providers.get_provider()
        assert provider.last_price("aapl") == Decimal("10.25")
        assert provider.last_price("MSFT") is None
        assert provider.fetch_news("AAPL")[0].published_at == "2025-12-01T00:00:00.000000Z"

    def test_fixture_drift_is_bounded_and_deterministic(self, tmp_path, monkeypatch):
        from datetime import UTC, datetime

        from common import timeutil

        path = tmp_path / "m.json"
        path.write_text(json.dumps({"prices": {"AAPL": 100}, "news": {}}))
        steady = providers.FixtureProvider(path)
        drifting = providers.FixtureProvider(path, drift=True)
        seen = set()
        for minute in range(0, 60, 5):
            monkeypatch.setattr(
                timeutil, "now", lambda minute=minute: datetime(2025, 12, 1, 10, minute, tzinfo=UTC)
            )
            price = drifting.last_price("AAPL")
            assert Decimal("98") <= price <= Decimal("102")
            assert price == drifting.last_price("AAPL")  # same instant -> same price
            assert steady.last_price("AAPL") == Decimal("100")
            seen.add(price)
        assert len(seen) > 5  # it actually moves

    def test_fixture_requires_path(self, monkeypatch):
        monkeypatch.setenv("NEWS_PROVIDER", "fixture")
        monkeypatch.delenv("NEWS_FIXTURE_PATH", raising=False)
        with pytest.raises(providers.ProviderUnavailable):
            providers.get_provider()

    def test_missing_yfinance_fails_loudly(self, monkeypatch):
        monkeypatch.setenv("NEWS_PROVIDER", "yfinance")
        monkeypatch.setitem(__import__("sys").modules, "yfinance", None)
        with pytest.raises(providers.ProviderUnavailable, match="layer"):
            providers.get_provider()

    def test_unknown_provider(self, monkeypatch):
        monkeypatch.setenv("NEWS_PROVIDER", "bloomberg-terminal")
        with pytest.raises(providers.ProviderUnavailable):
            providers.get_provider()


class TestYFinanceProvider:
    """YFinanceProvider against a stand-in for the yfinance module (no network)."""

    def install(self, monkeypatch, ticker_news, search_news=None, price=123.456789):
        import sys
        import types

        class FakeTicker:
            def __init__(self, symbol):
                self.news = ticker_news
                self.fast_info = types.SimpleNamespace(last_price=price)

        class FakeSearch:
            def __init__(self, symbol, news_count=10):
                if isinstance(search_news, Exception):
                    raise search_news
                self.news = search_news or []

        module = types.SimpleNamespace(Ticker=FakeTicker, Search=FakeSearch)
        monkeypatch.setitem(sys.modules, "yfinance", module)
        return providers.YFinanceProvider()

    def test_uses_ticker_news_when_available(self, monkeypatch):
        provider = self.install(monkeypatch, load_fixture("yfinance_news_nested.json"))
        assert [n.headline for n in provider.fetch_news("AAPL")] == ["Apple unveils new chip"]

    def test_falls_back_to_search_when_ticker_news_is_empty(self, monkeypatch):
        provider = self.install(
            monkeypatch, [], search_news=load_fixture("yfinance_news_flat.json")
        )
        assert [n.headline for n in provider.fetch_news("TSLA")] == ["Tesla recalls vehicles"]

    def test_search_failure_means_no_news_not_a_crash(self, monkeypatch):
        provider = self.install(monkeypatch, [], search_news=RuntimeError("429"))
        assert provider.fetch_news("TSLA") == []

    def test_price_is_rounded_decimal(self, monkeypatch):
        provider = self.install(monkeypatch, [])
        assert provider.last_price("AAPL") == Decimal("123.4568")


class TestScheduledRun:
    def test_publishes_news_and_updates_prices(self, cloud, table, use_provider):
        put_stock(table, "AAPL", is_deleted=False)
        use_provider(
            FakeProvider(
                news={"AAPL": [item("Apple beats"), item("Apple expands", "2025-12-02T10:00:00Z")]},
                prices={"AAPL": Decimal("201.5")},
            )
        )

        result = handler({}, None)

        assert result["messages_published"] == 2
        stock = table.get_item(Key={"PK": PK, "SK": "STOCK#AAPL"})["Item"]
        assert stock["last_fetched_price"] == Decimal("201.5")
        published = news_messages(cloud)
        assert {m["headline"] for m in published} == {"Apple beats", "Apple expands"}
        assert all(m["type"] == "NEW_NEWS" and len(m["news_id"]) == 32 for m in published)
        assert all(m["account_id"] == ACCOUNT for m in published)

    def test_second_run_does_not_republish(self, cloud, table, use_provider):
        put_stock(table, "AAPL")
        use_provider(FakeProvider(news={"AAPL": [item("Apple beats")]}))
        assert handler({}, None)["messages_published"] == 1
        assert handler({}, None)["messages_published"] == 0
        assert len(news_messages(cloud)) == 1

    def test_duplicates_within_a_batch_are_collapsed(self, cloud, table, use_provider):
        put_stock(table, "AAPL")
        use_provider(FakeProvider(news={"AAPL": [item("Same"), item("Same")]}))
        assert handler({}, None)["messages_published"] == 1

    def test_marker_is_written_only_after_publish(self, cloud, table, use_provider, monkeypatch):
        """A failed publish must be retried by the next run (the old code lost the news)."""
        put_stock(table, "AAPL")
        use_provider(FakeProvider(news={"AAPL": [item("Apple beats")]}))
        real = messages.publish
        monkeypatch.setattr(
            messages, "publish", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("sns down"))
        )
        with pytest.raises(RuntimeError, match="news fetch failed"):
            handler({}, None)
        monkeypatch.setattr(messages, "publish", real)
        assert handler({}, None)["messages_published"] == 1

    def test_one_failing_symbol_does_not_block_the_others(self, cloud, table, use_provider):
        put_stock(table, "BAD")
        put_stock(table, "GOOD")
        use_provider(FakeProvider(news={"GOOD": [item("fine news")]}, fail_on={"BAD"}))
        with pytest.raises(RuntimeError, match="BAD"):
            handler({}, None)
        assert [m["symbol"] for m in news_messages(cloud)] == ["GOOD"]

    def test_deleted_stocks_are_skipped(self, cloud, table, use_provider):
        put_stock(table, "OLD", is_deleted=True)
        put_stock(table, "NEW", is_deleted=False)
        use_provider(FakeProvider(news={"OLD": [item("old")], "NEW": [item("new")]}))
        assert handler({}, None)["symbols"] == ["NEW"]

    def test_price_update_never_creates_items(self, cloud, table, use_provider):
        from news_fetcher import main

        main._update_price(ACCOUNT, "GONE", Decimal("1"))
        assert "Item" not in table.get_item(Key={"PK": PK, "SK": "STOCK#GONE"})

    def test_explicit_account_in_event(self, cloud, table, use_provider):
        table.put_item(Item={"PK": "ACCOUNT#other", "SK": "STOCK#X", "symbol": "X"})
        use_provider(FakeProvider(news={"X": [item("x news")]}))
        result = handler({"account_id": "other"}, None)
        assert result["account_id"] == "other" and result["messages_published"] == 1

    def test_runs_with_eventbridge_event(self, cloud, table, use_provider):
        put_stock(table, "AAPL")
        use_provider(FakeProvider())
        event = load_fixture("events/eventbridge_scheduled.json")
        assert handler(event, None)["symbols"] == ["AAPL"]


class TestStockAddedTrigger:
    def test_fetches_only_the_new_symbol(self, cloud, table, use_provider):
        put_stock(table, "AAPL", is_deleted=False)
        put_stock(table, "MSFT", is_deleted=False)
        use_provider(
            FakeProvider(
                news={"AAPL": [item("a")], "MSFT": [item("m")]}, prices={"AAPL": Decimal("5")}
            )
        )
        event = sqs_event({"type": "STOCK_ADDED", "account_id": ACCOUNT, "symbol": "AAPL"})
        assert handler(event, None) == {"batchItemFailures": []}
        assert [m["symbol"] for m in news_messages(cloud)] == ["AAPL"]

    def test_ignores_stock_that_is_no_longer_active(self, cloud, table, use_provider):
        put_stock(table, "AAPL", is_deleted=True)
        use_provider(FakeProvider(news={"AAPL": [item("a")]}))
        event = sqs_event({"type": "STOCK_ADDED", "account_id": ACCOUNT, "symbol": "AAPL"})
        assert handler(event, None) == {"batchItemFailures": []}
        assert news_messages(cloud) == []

    def test_provider_failure_is_reported_for_retry(self, cloud, table, use_provider):
        put_stock(table, "AAPL", is_deleted=False)
        use_provider(FakeProvider(fail_on={"AAPL"}))
        event = sqs_event({"type": "STOCK_ADDED", "account_id": ACCOUNT, "symbol": "AAPL"})
        assert handler(event, None) == {"batchItemFailures": [{"itemIdentifier": "m-0"}]}
