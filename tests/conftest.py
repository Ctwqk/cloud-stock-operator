"""Shared fixtures: a moto-backed AWS with the project's resources provisioned.

``cloud`` provisions exactly what the CDK stack creates (table, bucket, topics, queues with
DLQs and SNS filter policies; both are generated from ``common.topology``), so handler tests
run against realistic routing rather than hand-made mocks.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import boto3
import pytest
from moto import mock_aws

from common import aws

FIXTURES = Path(__file__).parent / "fixtures"
ACCOUNT = "123456789012"  # account id moto's STS reports


@pytest.fixture(autouse=True)
def _aws_environment(monkeypatch):
    for name, value in {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_SESSION_TOKEN": "testing",
        "AWS_DEFAULT_REGION": "us-east-1",
        "AWS_REGION": "us-east-1",
    }.items():
        monkeypatch.setenv(name, value)
    for name in ("AWS_ENDPOINT_URL", "AWS_PROFILE", "ACCOUNT_ID", "NEWS_PROVIDER"):
        monkeypatch.delenv(name, raising=False)
    aws.reset_caches()
    yield
    aws.reset_caches()


@pytest.fixture
def cloud(monkeypatch):
    """Provisioned AWS resources (``local_cloud.Resources``) inside ``mock_aws``."""
    import local_cloud

    with mock_aws():
        res = local_cloud.provision()
        for name, value in res.environment().items():
            monkeypatch.setenv(name, value)
        yield res


@pytest.fixture
def table(cloud):
    return boto3.resource("dynamodb").Table(cloud.table_name)


@pytest.fixture
def s3(cloud):
    return boto3.client("s3")


@pytest.fixture
def sqs(cloud):
    return boto3.client("sqs")


@pytest.fixture
def local_cloud_runtime(cloud):
    """The in-process 'Lambda' runtime that drains queues into the real handlers."""
    import local_cloud

    runtime = local_cloud.LocalCloud(cloud)
    yield runtime
    aws.register_override("lambda", None)


def load_fixture(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))
