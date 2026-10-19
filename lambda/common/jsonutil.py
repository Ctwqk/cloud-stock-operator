"""Decimal-aware JSON helpers (DynamoDB returns ``Decimal`` for every number)."""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any


def _default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, set | frozenset):
        return sorted(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def dumps(value: Any) -> str:
    return json.dumps(value, default=_default, separators=(",", ":"))


def loads(text: str | bytes) -> Any:
    """Parse JSON keeping floats as ``Decimal`` so they can be written to DynamoDB."""
    return json.loads(text, parse_float=Decimal)
