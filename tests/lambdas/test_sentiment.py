"""The sentiment scorer must be deterministic and sensible on financial headlines."""

from __future__ import annotations

import pytest

from common import sentiment


@pytest.fixture(scope="module")
def scorer():
    return sentiment.VaderScorer()


@pytest.mark.parametrize(
    "headline, body, label",
    [
        (
            "Apple beats earnings estimates as iPhone sales surge",
            "Revenue topped forecasts.",
            "positive",
        ),
        ("Analyst upgrades Apple to outperform", "", "positive"),
        ("Microsoft shares rally after cloud revenue accelerates", "", "positive"),
        ("Nvidia surges to record high", "", "positive"),
        (
            "Tesla recalls 200,000 vehicles amid safety probe",
            "Regulators opened an investigation.",
            "negative",
        ),
        ("Nvidia shares plunge after analyst downgrade", "", "negative"),
        ("Meta faces antitrust lawsuit", "", "negative"),
        ("Company declares bankruptcy", "", "negative"),
        ("Stocks tumble as inflation fears return", "", "negative"),
        (
            "Microsoft announces new data center in Ohio",
            "Construction begins next year.",
            "neutral",
        ),
        ("Fed holds rates steady", "", "neutral"),
        ("", "", "neutral"),
    ],
)
def test_labels(scorer, headline, body, label):
    assert scorer.score(headline, body).label == label


def test_delta_follows_label(scorer):
    assert scorer.score("Stock surges on upgrade").delta == 1
    assert scorer.score("Stock plunges on downgrade").delta == -1
    assert scorer.score("Quarterly report released").delta == 0


def test_is_deterministic(scorer):
    first = [scorer.score("Apple beats estimates", "Revenue surged.") for _ in range(5)]
    assert len(set(first)) == 1


def test_compound_is_bounded(scorer):
    result = scorer.score("surge " * 50, "plunge " * 50)
    assert -1.0 <= result.compound <= 1.0


def test_headline_outweighs_body(scorer):
    positive_headline = scorer.score(
        "Company surges to record high", "Some concerns remain about a lawsuit."
    )
    assert positive_headline.delta == 1


def test_finance_lexicon_changes_the_outcome():
    """VADER alone has no idea that 'plunge' or 'downgrade' are bad; the extension does."""
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

    plain = SentimentIntensityAnalyzer().polarity_scores("Shares plunge after downgrade")[
        "compound"
    ]
    extended = sentiment.VaderScorer().score("Shares plunge after downgrade").compound
    assert plain > -0.05  # plain VADER does not see this as negative at all
    assert extended < -0.3


def test_threshold_is_configurable():
    strict = sentiment.VaderScorer(threshold=0.9)
    assert strict.score("Company reports good growth").label == "neutral"


def test_get_scorer_default_and_unknown(monkeypatch):
    sentiment.get_scorer.cache_clear()
    monkeypatch.delenv("SENTIMENT_BACKEND", raising=False)
    assert sentiment.get_scorer().name == "vader-finance"
    sentiment.get_scorer.cache_clear()
    monkeypatch.setenv("SENTIMENT_BACKEND", "magic")
    with pytest.raises(ValueError, match="magic"):
        sentiment.get_scorer()
    sentiment.get_scorer.cache_clear()
