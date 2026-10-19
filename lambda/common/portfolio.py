"""Net-worth maths shared by the control plane, the snapshot writer and the trade worker."""

from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal
from typing import Any

from . import aws, ddb, keys, timeutil


def positions_value(stock_items: Iterable[dict[str, Any]]) -> Decimal:
    """Sum of ``shares_managed * last_fetched_price`` over watchlist entries."""
    total = Decimal("0")
    for item in stock_items:
        try:
            shares = ddb.to_decimal(item.get("shares_managed", 0) or 0)
            price = ddb.to_decimal(item.get("last_fetched_price", 0) or 0)
        except ValueError:
            continue
        total += shares * price
    return total


def load_stocks(account_id: str) -> list[dict[str, Any]]:
    return ddb.query_all(
        aws.table(),
        KeyConditionExpression="PK = :pk AND begins_with(SK, :prefix)",
        ExpressionAttributeValues={
            ":pk": keys.account_pk(account_id),
            ":prefix": keys.STOCK_PREFIX,
        },
        ConsistentRead=True,
    )


def refresh_summary(account_id: str) -> dict[str, Decimal]:
    """Recompute ``positions_value`` / ``current_net_worth`` on the SUMMARY item.

    Safe to call repeatedly (it derives everything from current state), so callers can
    run it after any change without worrying about message redelivery.
    """
    table = aws.table()
    pv = positions_value(load_stocks(account_id))
    response = table.update_item(
        Key={"PK": keys.account_pk(account_id), "SK": keys.SUMMARY_SK},
        UpdateExpression=(
            "SET positions_value = :pv, "
            "current_net_worth = if_not_exists(current_cash_managed, :zero) + :pv, "
            "updated_at = :u"
        ),
        ExpressionAttributeValues={":pv": pv, ":zero": Decimal("0"), ":u": timeutil.iso_utc()},
        ReturnValues="ALL_NEW",
    )
    attrs = response["Attributes"]
    return {
        "cash": ddb.to_decimal(attrs.get("current_cash_managed", 0)),
        "positions_value": ddb.to_decimal(attrs["positions_value"]),
        "net_worth": ddb.to_decimal(attrs["current_net_worth"]),
    }
