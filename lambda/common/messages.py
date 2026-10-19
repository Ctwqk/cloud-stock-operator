"""Message contract between the Lambdas, the control plane and the local trade worker.

Every message is a JSON object with a ``type`` field. The same value is also sent as the
SNS message attribute ``type`` so that SNS *filter policies* can route each message type to
exactly one consumer queue (see :mod:`common.topology`).
"""

from __future__ import annotations

import hashlib
import logging
import re
from collections.abc import Callable, Mapping
from typing import Any

from . import aws, jsonutil
from .message_types import (  # noqa: F401  (re-exported: the message contract lives here)
    ADD_STOCK,
    ADJUST_MANAGED_CASH,
    ADJUST_MANAGED_SHARES,
    AUTO_TRADE_DECISION,
    CLEANUP_STOCK,
    MARK_STOCK_DELETED,
    NEW_NEWS,
    NEWS_STORED,
    STOCK_ADDED,
)

logger = logging.getLogger(__name__)

SYMBOL_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")


class MalformedMessage(ValueError):
    """The SQS body is not a JSON object (retrying cannot fix it -> ends up in the DLQ)."""


def normalize_symbol(value: Any) -> str:
    symbol = str(value or "").strip().upper()
    if not SYMBOL_RE.match(symbol):
        raise ValueError(f"invalid symbol: {value!r}")
    return symbol


def publish(topic_arn: str, message: Mapping[str, Any]) -> str:
    """Publish ``message`` to an SNS topic with the ``type`` attribute set for filtering."""
    if not topic_arn:
        raise aws.ConfigError("cannot publish: topic ARN is empty")
    response = aws.client("sns").publish(
        TopicArn=topic_arn,
        Message=jsonutil.dumps(message),
        MessageAttributes={"type": {"DataType": "String", "StringValue": str(message["type"])}},
    )
    return response["MessageId"]


def parse_sqs_record(record: Mapping[str, Any]) -> dict[str, Any]:
    """Return the application message from an SQS record.

    Handles both an SNS envelope (``{"Type": "Notification", "Message": "<json>"}``) and a
    raw-delivery body.
    """
    try:
        body = jsonutil.loads(record["body"])
        if isinstance(body, dict) and body.get("Type") == "Notification" and "Message" in body:
            body = jsonutil.loads(body["Message"])
    except (KeyError, TypeError, ValueError) as exc:
        raise MalformedMessage(f"unparseable SQS body: {exc}") from exc
    if not isinstance(body, dict):
        raise MalformedMessage("SQS body is not a JSON object")
    return body


def process_sqs_batch(
    event: Mapping[str, Any],
    handle: Callable[[dict[str, Any], Mapping[str, Any]], None],
) -> dict[str, list[dict[str, str]]]:
    """Run ``handle(message, record)`` for every record, reporting per-record failures.

    The return value is the Lambda "partial batch response": only the failed records are
    returned to the queue (and eventually moved to the DLQ); the rest are deleted.
    The event source mapping must set ``ReportBatchItemFailures``.
    """
    failures: list[dict[str, str]] = []
    for record in event.get("Records", []):
        try:
            handle(parse_sqs_record(record), record)
        except Exception:
            logger.exception("failed to process SQS record %s", record.get("messageId"))
            failures.append({"itemIdentifier": record["messageId"]})
    return {"batchItemFailures": failures}


def stable_id(*parts: Any, length: int = 32) -> str:
    """Deterministic short id from the given parts (used for de-duplication keys)."""
    digest = hashlib.sha256("|".join(str(p) for p in parts).encode("utf-8")).hexdigest()
    return digest[:length]
