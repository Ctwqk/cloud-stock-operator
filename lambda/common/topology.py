"""Messaging topology: which message types go to which queue and consumer.

This module is the single source of truth for the SNS -> SQS fan-out. The CDK stack builds
its queues, DLQs, SNS filter subscriptions and Lambda event sources from :data:`QUEUES`, and
the local runtime (``local_app/local_cloud.py``) builds the same wiring on an emulator, so the
local demo exercises the deployed routing rather than a re-implementation of it.

Why one queue per consumer?  The first version had several consumers (two Lambdas and a local
worker) competing for *one* standard queue: every message went to whichever consumer polled
first, and a Lambda that ignores a message still deletes it, so messages were silently lost.
Each consumer now owns a queue that only receives the message types it handles.
"""

from __future__ import annotations

from dataclasses import dataclass

from . import message_types as messages

EXTERNAL_TOPIC = "external"
SYSTEM_TOPIC = "system"

LOCAL_WORKER = "local_trade_worker"  # polls its queue from outside AWS (IBKR-less demo trader)


@dataclass(frozen=True)
class QueueSpec:
    name: str  # CloudFormation logical id, e.g. "ExternalOpsQueue"
    topic: str  # EXTERNAL_TOPIC | SYSTEM_TOPIC
    message_types: tuple[str, ...]  # SNS filter policy on the "type" message attribute
    consumer: str  # Lambda package name (lambda/<consumer>/main.py) or LOCAL_WORKER
    batch_size: int = 10
    visibility_timeout_s: int = 180  # >= 6 x the consumer Lambda timeout (AWS guidance)
    max_receive_count: int = 5  # deliveries before a message is moved to the DLQ


QUEUES: tuple[QueueSpec, ...] = (
    QueueSpec(
        name="ExternalOpsQueue",
        topic=EXTERNAL_TOPIC,
        message_types=(
            messages.ADD_STOCK,
            messages.ADJUST_MANAGED_CASH,
            messages.ADJUST_MANAGED_SHARES,
        ),
        consumer="stock_info_writer",
    ),
    QueueSpec(
        name="FetchTriggerQueue",
        topic=SYSTEM_TOPIC,
        message_types=(messages.STOCK_ADDED,),
        consumer="news_fetcher",
        batch_size=5,
        visibility_timeout_s=1800,  # 6 x the fetcher Lambda timeout (300 s)
    ),
    QueueSpec(
        name="NewsIngestQueue",
        topic=SYSTEM_TOPIC,
        message_types=(messages.NEW_NEWS,),
        consumer="news_writer",
    ),
    QueueSpec(
        name="SentimentQueue",
        topic=SYSTEM_TOPIC,
        message_types=(messages.NEWS_STORED,),
        consumer="sentiment_scorer",
    ),
    QueueSpec(
        name="CleanupQueue",
        topic=SYSTEM_TOPIC,
        message_types=(messages.MARK_STOCK_DELETED, messages.CLEANUP_STOCK),
        consumer="soft_delete",
    ),
    QueueSpec(
        name="TradeDecisionQueue",
        topic=SYSTEM_TOPIC,
        message_types=(messages.AUTO_TRADE_DECISION,),
        consumer=LOCAL_WORKER,
        batch_size=10,
        visibility_timeout_s=60,
    ),
)

# Lambda functions that are not queue consumers (invoked by EventBridge or API Gateway).
SCHEDULED_FUNCTIONS = ("news_fetcher", "networth_snapshot_writer", "news_retention_cleaner")
API_FUNCTIONS = ("control_plane_api", "networth_plotter")
ALL_FUNCTIONS = (
    "news_fetcher",
    "news_writer",
    "sentiment_scorer",
    "stock_info_writer",
    "soft_delete",
    "networth_plotter",
    "networth_snapshot_writer",
    "news_retention_cleaner",
    "control_plane_api",
)


def queue_by_name(name: str) -> QueueSpec:
    for spec in QUEUES:
        if spec.name == name:
            return spec
    raise KeyError(name)
