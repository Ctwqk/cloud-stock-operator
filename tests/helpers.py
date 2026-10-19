"""Builders for the event shapes the handlers receive."""

from __future__ import annotations

import json
import uuid
from typing import Any


def sns_sqs_record(message: dict[str, Any], message_id: str | None = None) -> dict[str, Any]:
    """An SQS record whose body is an SNS notification envelope (SNS -> SQS fan-out)."""
    return {
        "messageId": message_id or str(uuid.uuid4()),
        "receiptHandle": "receipt-handle",
        "body": json.dumps(
            {
                "Type": "Notification",
                "MessageId": str(uuid.uuid4()),
                "TopicArn": "arn:aws:sns:us-east-1:123456789012:SystemOpsTopic",
                "Message": json.dumps(message),
                "MessageAttributes": {"type": {"Type": "String", "Value": message["type"]}},
            }
        ),
        "attributes": {"ApproximateReceiveCount": "1"},
        "messageAttributes": {},
        "eventSource": "aws:sqs",
        "awsRegion": "us-east-1",
    }


def sqs_event(*messages: dict[str, Any]) -> dict[str, Any]:
    return {"Records": [sns_sqs_record(m, message_id=f"m-{i}") for i, m in enumerate(messages)]}


def api_event(
    method: str, path: str, body: dict[str, Any] | None = None, query: dict[str, str] | None = None
) -> dict[str, Any]:
    """API Gateway REST (v1) Lambda-proxy event."""
    return {
        "httpMethod": method,
        "path": path,
        "headers": {"content-type": "application/json"},
        "queryStringParameters": query,
        "body": json.dumps(body) if body is not None else None,
        "isBase64Encoded": False,
        "requestContext": {"stage": "prod"},
    }
