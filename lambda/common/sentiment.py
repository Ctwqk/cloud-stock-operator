"""Pluggable, deterministic sentiment scoring for financial news.

The rest of the system only depends on :class:`SentimentScorer`. The default
implementation, :class:`VaderScorer`, uses the VADER lexicon (a ~0.5 MB pure-Python
package, so it fits comfortably in a Lambda layer and needs no model download) extended with
finance-specific terms ("downgrade", "plunge", "beats", "recall", ...).

Scores are deterministic: the same text always yields the same delta, which makes the
pipeline testable (the previous implementation drew a random number).

To add another backend (FinBERT behind an endpoint, an LLM, ...) implement ``score`` and
register it in :data:`BACKENDS`; select it with ``SENTIMENT_BACKEND``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Protocol

# VADER valence scale is roughly -4 (very negative) .. +4 (very positive).
# fmt: off
FINANCE_LEXICON: dict[str, float] = {
    # positive
    "surge": 2.2, "surges": 2.2, "surged": 2.2, "soar": 2.5, "soars": 2.5, "soared": 2.5,
    "rally": 1.9, "rallies": 1.9, "rallied": 1.9, "jump": 1.6, "jumps": 1.6, "jumped": 1.6,
    "climb": 1.3, "climbs": 1.3, "climbed": 1.3, "rebound": 1.5, "rebounds": 1.5,
    "upgrade": 2.0, "upgrades": 2.0, "upgraded": 2.0, "outperform": 2.1, "outperforms": 2.1,
    "bullish": 2.4, "beat": 1.6, "beats": 1.8, "buyback": 1.3, "dividend": 0.9,
    "breakthrough": 2.2, "record": 1.2, "raises": 1.0, "raised": 1.0, "expands": 1.1,
    "expansion": 1.1, "accelerates": 1.4, "profitable": 2.0, "tops": 1.8, "outpaces": 1.6,
    "approval": 2.1, "approved": 1.8, "wins": 1.8, "secures": 1.3,
    # negative
    "plunge": -2.7, "plunges": -2.7, "plunged": -2.7, "tumble": -2.3, "tumbles": -2.3,
    "tumbled": -2.3, "slump": -2.1, "slumps": -2.1, "slumped": -2.1, "slide": -1.6,
    "slides": -1.6, "sank": -2.0, "sinks": -2.0, "drops": -1.5, "dropped": -1.5, "falls": -1.4,
    "fell": -1.4, "selloff": -2.2, "sell-off": -2.2, "downgrade": -2.1, "downgrades": -2.1,
    "downgraded": -2.1, "underperform": -2.1, "bearish": -2.4, "miss": -1.6, "misses": -1.9,
    "missed": -1.9, "shortfall": -1.7, "lawsuit": -1.9, "lawsuits": -1.9, "sued": -1.9,
    "probe": -1.6, "investigation": -1.6, "subpoena": -1.7, "recall": -1.9, "recalls": -1.9,
    "layoffs": -1.9, "bankruptcy": -3.2, "insolvency": -3.0, "default": -2.2, "defaults": -2.2,
    "fined": -1.6, "fine": -0.4, "antitrust": -1.3, "breach": -2.0, "hack": -2.0,
    "outage": -1.6, "delays": -1.4, "delayed": -1.4, "warns": -1.6, "warning": -1.6,
    "slashes": -1.6, "halts": -1.5, "halted": -1.5, "resigns": -1.4, "weakness": -1.7,
    "scandal": -2.4, "fraud": -3.0,
}
# fmt: on

POSITIVE_THRESHOLD = 0.05  # VADER's conventional compound-score cut-offs


@dataclass(frozen=True)
class Sentiment:
    """Result of scoring one piece of text."""

    compound: float  # -1.0 .. 1.0
    label: str  # "positive" | "neutral" | "negative"
    delta: int  # +1 | 0 | -1: the amount added to a stock's level score
    backend: str


class SentimentScorer(Protocol):
    """Anything that can turn a headline/body pair into a :class:`Sentiment`."""

    name: str

    def score(self, headline: str, body: str = "") -> Sentiment: ...


class VaderScorer:
    """VADER + finance lexicon. Headline counts 60 %, body 40 % (headline only if no body)."""

    name = "vader-finance"

    def __init__(self, threshold: float = POSITIVE_THRESHOLD) -> None:
        # Imported lazily so that importing this module never requires the dependency.
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

        self._threshold = threshold
        self._analyzer = SentimentIntensityAnalyzer()
        self._analyzer.lexicon.update(FINANCE_LEXICON)

    def _compound(self, text: str) -> float:
        return float(self._analyzer.polarity_scores(text)["compound"])

    def score(self, headline: str, body: str = "") -> Sentiment:
        headline = (headline or "").strip()
        body = (body or "").strip()
        if headline and body:
            compound = 0.6 * self._compound(headline) + 0.4 * self._compound(body)
        elif headline or body:
            compound = self._compound(headline or body)
        else:
            compound = 0.0
        compound = max(-1.0, min(1.0, round(compound, 4)))
        if compound >= self._threshold:
            label, delta = "positive", 1
        elif compound <= -self._threshold:
            label, delta = "negative", -1
        else:
            label, delta = "neutral", 0
        return Sentiment(compound=compound, label=label, delta=delta, backend=self.name)


BACKENDS = {"vader": VaderScorer}


@lru_cache(maxsize=1)
def get_scorer() -> SentimentScorer:
    """Scorer selected by ``SENTIMENT_BACKEND`` (default ``vader``), created once."""
    backend = os.environ.get("SENTIMENT_BACKEND", "vader").lower()
    try:
        factory = BACKENDS[backend]
    except KeyError:
        raise ValueError(
            f"unknown SENTIMENT_BACKEND {backend!r}; available: {sorted(BACKENDS)}"
        ) from None
    return factory()
