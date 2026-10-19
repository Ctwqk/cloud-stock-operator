"""Lambda B: NEWS_STORED -> VADER score -> DynamoDB / AUTO_TRADE_DECISION."""

from __future__ import annotations

import json

from sentiment_scorer.main import handler
from tests.helpers import sqs_event

ACCOUNT = "123456789012"
PK = f"ACCOUNT#{ACCOUNT}"


def store_article(s3, cloud, headline, body="", symbol="AAPL", news_id="n1"):
    key = f"news/{symbol}/2025-12-01/{news_id}.json"
    s3.put_object(
        Bucket=cloud.bucket_name,
        Key=key,
        Body=json.dumps(
            {"account_id": ACCOUNT, "symbol": symbol, "headline": headline, "body": body}
        ),
    )
    return {
        "type": "NEWS_STORED",
        "news_id": news_id,
        "account_id": ACCOUNT,
        "symbol": symbol,
        "bucket": cloud.bucket_name,
        "key": key,
    }


def add_stock(table, symbol="AAPL", threshold=3, score=0):
    table.put_item(
        Item={
            "PK": PK,
            "SK": f"STOCK#{symbol}",
            "symbol": symbol,
            "is_deleted": False,
            "current_level_score": score,
            "threshold_abs": threshold,
        }
    )


def stock(table, symbol="AAPL"):
    return table.get_item(Key={"PK": PK, "SK": f"STOCK#{symbol}"})["Item"]


def test_positive_news_raises_score(cloud, table, s3):
    add_stock(table)
    message = store_article(
        s3, cloud, "Apple beats earnings as sales surge", "Revenue topped forecasts."
    )
    assert handler(sqs_event(message), None) == {"batchItemFailures": []}
    item = stock(table)
    assert item["current_level_score"] == 1
    assert item["last_headline"] == "Apple beats earnings as sales surge"


def test_negative_news_lowers_score(cloud, table, s3):
    add_stock(table)
    handler(sqs_event(store_article(s3, cloud, "Apple plunges after downgrade")), None)
    assert stock(table)["current_level_score"] == -1


def test_neutral_news_leaves_score_unchanged(cloud, table, s3):
    add_stock(table, score=2)
    handler(sqs_event(store_article(s3, cloud, "Apple publishes annual report")), None)
    assert stock(table)["current_level_score"] == 2


def test_same_article_is_scored_once(cloud, table, s3):
    add_stock(table)
    message = store_article(s3, cloud, "Apple surges on upgrade")
    handler(sqs_event(message), None)
    handler(sqs_event(message), None)
    assert stock(table)["current_level_score"] == 1


def test_threshold_crossing_emits_trade_decision(cloud, table, s3, sqs):
    add_stock(table, threshold=1)
    handler(sqs_event(store_article(s3, cloud, "Apple surges to record high")), None)
    [raw] = sqs.receive_message(QueueUrl=cloud.queue_urls["TradeDecisionQueue"])["Messages"]
    decision = json.loads(json.loads(raw["Body"])["Message"])
    assert decision["action"] == "BUY"
    assert decision["symbol"] == "AAPL"
    assert decision["reason"] == "SENTIMENT:vader-finance"


def test_scoring_is_not_random(cloud, table, s3):
    """The original implementation drew a random score; identical input must now agree."""
    results = []
    for i in range(3):
        add_stock(table, score=0)
        handler(sqs_event(store_article(s3, cloud, "Apple surges", news_id=f"r{i}")), None)
        results.append(int(stock(table)["current_level_score"]))
        table.delete_item(Key={"PK": PK, "SK": "STOCK#AAPL"})
    assert results == [1, 1, 1]


def test_news_for_unknown_stock_is_acknowledged_without_side_effects(cloud, table, s3):
    message = store_article(s3, cloud, "Ghost corp surges", symbol="GHST")
    assert handler(sqs_event(message), None) == {"batchItemFailures": []}
    assert "Item" not in table.get_item(Key={"PK": PK, "SK": "STOCK#GHST"})


def test_missing_s3_object_is_retried(cloud, table):
    add_stock(table)
    message = {
        "type": "NEWS_STORED",
        "news_id": "gone",
        "account_id": ACCOUNT,
        "symbol": "AAPL",
        "bucket": "b",
        "key": "news/AAPL/2025-12-01/gone.json",
    }
    assert handler(sqs_event(message), None)["batchItemFailures"] == [{"itemIdentifier": "m-0"}]


def test_wrong_message_type_is_ignored(cloud, table):
    assert handler(sqs_event({"type": "NEW_NEWS"}), None) == {"batchItemFailures": []}
