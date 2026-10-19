"""Shared helpers packaged into every Lambda function (and reused by local_app).

Modules:
    keys       single-table key builders (the one place that knows the key design)
    timeutil   fixed-width UTC timestamps so sort keys order chronologically
    jsonutil   Decimal-aware JSON encode/decode
    aws        cached boto3 clients, honours AWS_ENDPOINT_URL for local runs
    messages   message type constants + SNS publish / SQS-envelope helpers
    topology   the SNS -> SQS -> consumer wiring (used by CDK and the local runtime)
    sentiment  pluggable sentiment scorers (VADER + finance lexicon)
    scoring    apply a sentiment delta to a watchlist entry (idempotent)
    portfolio  positions value / net worth maths
    retention  S3 news retention rules
"""

import logging
import os

# Lambda's root logger defaults to WARNING, which would hide every logger.info() line.
logging.getLogger().setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())
