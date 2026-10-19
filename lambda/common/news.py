"""News item identity and S3 layout."""

from __future__ import annotations

from datetime import date

from . import messages, timeutil


def news_id(symbol: str, headline: str, published_at: str) -> str:
    """Stable id of a news item; the same story always maps to the same id and S3 key."""
    return messages.stable_id(symbol.upper(), headline.strip(), published_at)


def news_key(symbol: str, published_at: str, item_id: str) -> str:
    """``news/<SYMBOL>/<yyyy-mm-dd>/<id>.json`` (day taken from ``published_at``)."""
    parsed = timeutil.parse_iso(published_at)
    day: date = (parsed or timeutil.now()).date()
    return f"news/{symbol.upper()}/{day.isoformat()}/{item_id}.json"
