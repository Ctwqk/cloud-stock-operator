"""News/price providers for the news fetcher.

``yfinance`` (default) is the real source. ``fixture`` serves canned headlines and prices from a
JSON file, which is what the local demo and the tests use: it is deterministic and works offline.
Select with ``NEWS_PROVIDER=yfinance|fixture`` (``NEWS_FIXTURE_PATH`` for the file).
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol

from common import timeutil

logger = logging.getLogger(__name__)

MAX_BODY_CHARS = 2000


class ProviderUnavailable(RuntimeError):
    """The configured provider cannot be used (e.g. the yfinance layer is missing)."""


@dataclass(frozen=True)
class NewsItem:
    headline: str
    body: str
    source: str
    published_at: str  # canonical fixed-width UTC timestamp


class NewsProvider(Protocol):
    def fetch_news(self, symbol: str, max_items: int = 10) -> list[NewsItem]: ...

    def last_price(self, symbol: str) -> Decimal | None: ...


def normalize_yfinance_item(raw: dict[str, Any], now: datetime | None = None) -> NewsItem | None:
    """Convert one ``yfinance.Ticker.news`` entry into a :class:`NewsItem`.

    yfinance has returned two shapes over time: a flat dict (``title``, ``publisher``,
    ``providerPublishTime`` epoch) and a nested one under ``content`` (``title``, ``summary``,
    ``provider.displayName``, ``pubDate`` ISO string). Both are supported. Entries without a
    title *and* summary are dropped.
    """
    now = now or timeutil.now()
    content = raw.get("content") or raw
    title = (content.get("title") or "").strip()
    summary = (content.get("summary") or content.get("description") or "").strip()
    if not title and not summary:
        return None

    provider = (
        content.get("publisher")
        or (content.get("provider") or {}).get("displayName")
        or raw.get("publisher")
        or "yfinance"
    )

    published: datetime | None = None
    epoch = raw.get("providerPublishTime")
    if epoch:
        try:
            published = datetime.fromtimestamp(float(epoch), tz=UTC)
        except (TypeError, ValueError, OverflowError, OSError):
            published = None
    if published is None:
        published = timeutil.parse_iso(content.get("pubDate"))

    return NewsItem(
        headline=title,
        body=summary[:MAX_BODY_CHARS],
        source=str(provider),
        published_at=timeutil.iso_utc(published or now),
    )


class YFinanceProvider:
    def __init__(self) -> None:
        try:
            import yfinance  # type: ignore[import-not-found]
        except ImportError as exc:
            raise ProviderUnavailable(
                "yfinance is not importable; attach the yfinance layer (scripts/build_layers.sh)"
            ) from exc
        self._yf = yfinance

    def fetch_news(self, symbol: str, max_items: int = 10) -> list[NewsItem]:
        raw_items = self._yf.Ticker(symbol).news or []
        if not raw_items:
            # Ticker.news is intermittently empty (verified with yfinance 1.7); the search
            # endpoint serves the same stories in the flat shape.
            try:
                raw_items = self._yf.Search(symbol, news_count=max_items).news or []
            except Exception:
                logger.warning("yfinance search fallback failed for %s", symbol, exc_info=True)
                raw_items = []
        items = (normalize_yfinance_item(raw) for raw in raw_items[:max_items])
        return [item for item in items if item is not None]

    def last_price(self, symbol: str) -> Decimal | None:
        ticker = self._yf.Ticker(symbol)
        fast_info = getattr(ticker, "fast_info", None)
        price = getattr(fast_info, "last_price", None) if fast_info is not None else None
        if price is None:
            history = ticker.history(period="1d")
            if history.empty:
                return None
            price = history["Close"].iloc[-1]
        return Decimal(str(round(float(price), 4)))


class FixtureProvider:
    """Canned data: ``{"prices": {"AAPL": 195.2}, "news": {"AAPL": [{headline, body, ...}]}}``."""

    def __init__(self, path: str | os.PathLike[str]) -> None:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        self._prices: dict[str, Any] = data.get("prices", {})
        self._news: dict[str, list[dict[str, Any]]] = data.get("news", {})

    def fetch_news(self, symbol: str, max_items: int = 10) -> list[NewsItem]:
        items = []
        for raw in self._news.get(symbol.upper(), [])[:max_items]:
            published = timeutil.normalize_iso(raw.get("published_at")) or timeutil.iso_utc()
            items.append(
                NewsItem(
                    headline=raw["headline"],
                    body=raw.get("body", ""),
                    source=raw.get("source", "fixture"),
                    published_at=published,
                )
            )
        return items

    def last_price(self, symbol: str) -> Decimal | None:
        price = self._prices.get(symbol.upper())
        return Decimal(str(price)) if price is not None else None


def get_provider() -> NewsProvider:
    name = os.environ.get("NEWS_PROVIDER", "yfinance").lower()
    if name == "fixture":
        path = os.environ.get("NEWS_FIXTURE_PATH")
        if not path:
            raise ProviderUnavailable("NEWS_PROVIDER=fixture requires NEWS_FIXTURE_PATH")
        return FixtureProvider(path)
    if name == "yfinance":
        return YFinanceProvider()
    raise ProviderUnavailable(f"unknown NEWS_PROVIDER {name!r}")
