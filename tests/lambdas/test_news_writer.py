"""Lambda F: NEW_NEWS -> S3 object + NEWS_STORED."""

from __future__ import annotations

import json

from news_writer.main import handler
from tests.conftest import load_fixture
from tests.helpers import sqs_event

NEWS = {
    "type": "NEW_NEWS",
    "news_id": "abc123",
    "account_id": "123456789012",
    "symbol": "aapl",
    "headline": "Apple beats earnings estimates",
    "body": "Revenue topped forecasts.",
    "source": "wire",
    "published_at": "2025-12-01T14:05:00Z",
}


def stored(s3, cloud):
    listing = s3.list_objects_v2(Bucket=cloud.bucket_name, Prefix="news/")
    return [o["Key"] for o in listing.get("Contents", [])]


def news_stored_messages(sqs, cloud):
    got = sqs.receive_message(QueueUrl=cloud.queue_urls["SentimentQueue"], MaxNumberOfMessages=10)
    return [json.loads(json.loads(m["Body"])["Message"]) for m in got.get("Messages", [])]


def test_writes_object_with_deterministic_key_and_announces_it(cloud, s3, sqs):
    assert handler(sqs_event(NEWS), None) == {"batchItemFailures": []}

    assert stored(s3, cloud) == ["news/AAPL/2025-12-01/abc123.json"]
    body = json.loads(
        s3.get_object(Bucket=cloud.bucket_name, Key="news/AAPL/2025-12-01/abc123.json")[
            "Body"
        ].read()
    )
    assert body["headline"] == "Apple beats earnings estimates"
    assert body["symbol"] == "AAPL"
    assert "ingested_at" in body

    [announcement] = news_stored_messages(sqs, cloud)
    assert announcement == {
        "type": "NEWS_STORED",
        "news_id": "abc123",
        "account_id": "123456789012",
        "symbol": "AAPL",
        "bucket": cloud.bucket_name,
        "key": "news/AAPL/2025-12-01/abc123.json",
    }


def test_redelivery_overwrites_instead_of_duplicating(cloud, s3):
    """Regression: the key used uuid4(), so every redelivery created another object."""
    handler(sqs_event(NEWS), None)
    handler(sqs_event(NEWS), None)
    assert len(stored(s3, cloud)) == 1


def test_news_id_is_derived_when_missing(cloud, s3):
    message = {k: v for k, v in NEWS.items() if k != "news_id"}
    handler(sqs_event(message), None)
    [key] = stored(s3, cloud)
    assert key.startswith("news/AAPL/2025-12-01/") and len(key.split("/")[-1]) == len(
        "x" * 32 + ".json"
    )


def test_real_event_shape_from_fixture(cloud, s3):
    event = load_fixture("events/sqs_sns_new_news.json")
    assert handler(event, None) == {"batchItemFailures": []}
    assert stored(s3, cloud) == ["news/AAPL/2025-12-01/abc123.json"]


def test_bad_records_fail_individually(cloud, s3):
    event = sqs_event(NEWS, {**NEWS, "symbol": "../../etc"}, {"type": "OTHER"})
    event["Records"].append({"messageId": "garbage", "body": "not-json"})
    result = handler(event, None)
    assert result["batchItemFailures"] == [{"itemIdentifier": "m-1"}, {"itemIdentifier": "garbage"}]
    assert len(stored(s3, cloud)) == 1  # the good one still went through; OTHER was ignored


def test_missing_bucket_config_fails_the_message(cloud, s3, monkeypatch):
    monkeypatch.delenv("BUCKET_NAME")
    assert handler(sqs_event(NEWS), None)["batchItemFailures"] == [{"itemIdentifier": "m-0"}]
