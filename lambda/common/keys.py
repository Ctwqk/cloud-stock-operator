"""Single-table key design for ``TradingTable``.

Every item of one tenant lives in one partition, ``PK = ACCOUNT#<account_id>``; the
sort key's prefix says what the item is::

    SK                                  item
    ----------------------------------  ---------------------------------------------
    SUMMARY                             managed cash, positions value, net worth
    STOCK#<SYMBOL>                      watchlist entry (shares, score, threshold, price)
    NETWORTH#<ts>                       net-worth snapshot (time series)
    TRADE#<ts>#<decision_id>            virtual trade log written by the local worker
    SCORE#<SYMBOL>#<ts>                 sentiment history for one stock
    NEWSHASH#<SYMBOL>#<hash>            news de-duplication marker        (TTL)
    SCORED#<news_id>                    "this news was already scored"     (TTL)
    OP#<op_id>                          result of an external command      (TTL)
    DECISION#<decision_id>              "this trade decision was executed" (TTL)

Timestamps use :mod:`common.timeutil` (fixed width) so ``begins_with`` / ``BETWEEN``
queries return chronological ranges.
"""

from __future__ import annotations

SUMMARY_SK = "SUMMARY"
STOCK_PREFIX = "STOCK#"
NETWORTH_PREFIX = "NETWORTH#"
TRADE_PREFIX = "TRADE#"
SCORE_PREFIX = "SCORE#"

TTL_ATTRIBUTE = "ttl"


def account_pk(account_id: str) -> str:
    return f"ACCOUNT#{account_id}"


def stock_sk(symbol: str) -> str:
    return f"{STOCK_PREFIX}{symbol.upper()}"


def symbol_from_stock_sk(sk: str) -> str:
    return sk[len(STOCK_PREFIX) :]


def networth_sk(timestamp: str) -> str:
    return f"{NETWORTH_PREFIX}{timestamp}"


def trade_sk(timestamp: str, decision_id: str) -> str:
    return f"{TRADE_PREFIX}{timestamp}#{decision_id}"


def score_sk(symbol: str, timestamp: str) -> str:
    return f"{SCORE_PREFIX}{symbol.upper()}#{timestamp}"


def newshash_sk(symbol: str, news_hash: str) -> str:
    return f"NEWSHASH#{symbol.upper()}#{news_hash}"


def scored_sk(news_id: str) -> str:
    return f"SCORED#{news_id}"


def op_sk(op_id: str) -> str:
    return f"OP#{op_id}"


def decision_sk(decision_id: str) -> str:
    return f"DECISION#{decision_id}"
