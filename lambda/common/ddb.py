"""Small DynamoDB helpers: pagination, Decimal coercion, transaction error inspection."""

from __future__ import annotations

from collections.abc import Iterator
from decimal import Decimal, InvalidOperation
from typing import Any

from botocore.exceptions import ClientError


def query_all(table: Any, **kwargs: Any) -> list[dict[str, Any]]:
    """``table.query`` that follows ``LastEvaluatedKey`` until the result is complete."""
    items: list[dict[str, Any]] = []
    while True:
        response = table.query(**kwargs)
        items.extend(response.get("Items", []))
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            return items
        kwargs["ExclusiveStartKey"] = last_key


def scan_all(table: Any, **kwargs: Any) -> Iterator[dict[str, Any]]:
    while True:
        response = table.scan(**kwargs)
        yield from response.get("Items", [])
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            return
        kwargs["ExclusiveStartKey"] = last_key


def to_decimal(value: Any, field: str = "value") -> Decimal:
    """Coerce JSON numbers/strings to a finite ``Decimal`` (DynamoDB rejects ``float``)."""
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a number")
    try:
        number = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError(f"{field} must be a number") from None
    if not number.is_finite():
        raise ValueError(f"{field} must be finite")
    return number


def to_int(value: Any, field: str = "value") -> int:
    number = to_decimal(value, field)
    if number != number.to_integral_value():
        raise ValueError(f"{field} must be an integer")
    return int(number)


def error_code(exc: ClientError) -> str:
    return exc.response.get("Error", {}).get("Code", "")


def is_conditional_failure(exc: ClientError) -> bool:
    return error_code(exc) == "ConditionalCheckFailedException"


def cancellation_codes(exc: ClientError) -> list[str]:
    """Per-item reason codes of a ``TransactionCanceledException`` (``"None"`` = not at fault)."""
    if error_code(exc) != "TransactionCanceledException":
        return []
    reasons = exc.response.get("CancellationReasons") or []
    return [reason.get("Code", "None") for reason in reasons]
