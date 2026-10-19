"""UTC timestamp helpers.

All timestamps stored in sort keys use one fixed-width format so that plain string
comparison (which is what DynamoDB does for ``BETWEEN`` / ``begins_with``) is also
chronological order::

    2025-12-01T09:30:00.000000Z
"""

from __future__ import annotations

from datetime import UTC, datetime

ISO_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def now() -> datetime:
    """Current UTC time. Tests monkeypatch this to control the clock."""
    return datetime.now(UTC)


def iso_utc(moment: datetime | None = None) -> str:
    moment = moment or now()
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC).strftime(ISO_FORMAT)


def parse_iso(value: str | None) -> datetime | None:
    """Parse an ISO-8601 string (``Z`` or offset) into an aware UTC datetime.

    Returns ``None`` for empty or unparseable input instead of raising.
    """
    if not value or not isinstance(value, str):
        return None
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def normalize_iso(value: str | None) -> str | None:
    """Re-format any parseable ISO timestamp into the canonical fixed-width form."""
    parsed = parse_iso(value)
    return iso_utc(parsed) if parsed else None


def epoch_in(days: float = 0, seconds: float = 0) -> int:
    """Epoch seconds ``days``/``seconds`` from now (used for DynamoDB TTL attributes)."""
    return int(now().timestamp() + days * 86400 + seconds)
