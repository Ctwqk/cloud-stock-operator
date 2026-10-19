"""Cached boto3 clients/resources.

* Clients are created lazily and reused across invocations (Lambda container reuse).
* ``AWS_ENDPOINT_URL`` (honoured natively by boto3) redirects every call to a local
  emulator, which is how ``local_app`` runs the whole system without AWS.
* ``ACCOUNT_ID`` overrides the STS lookup; the account id is the tenant key in the table.
"""

from __future__ import annotations

import os
import threading
from functools import cache, lru_cache
from typing import Any

import boto3
from botocore.config import Config

_CONFIG = Config(retries={"max_attempts": 5, "mode": "standard"})


class ConfigError(RuntimeError):
    """A required environment variable is missing."""


def require_env(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise ConfigError(f"environment variable {name} is not set")
    return value


_OVERRIDES: dict[str, Any] = {}


def register_override(service: str, replacement: Any | None) -> None:
    """Substitute the client for ``service`` (e.g. the local runtime fakes ``lambda``)."""
    if replacement is None:
        _OVERRIDES.pop(service, None)
    else:
        _OVERRIDES[service] = replacement


def client(service: str) -> Any:
    if service in _OVERRIDES:
        return _OVERRIDES[service]
    return _cached_client(service)


@cache
def _cached_client(service: str) -> Any:
    return boto3.client(service, config=_CONFIG)


_local = threading.local()


def dynamodb_resource() -> Any:
    """boto3 *resources* are not thread-safe, so cache one per thread (Lambda: just one)."""
    if getattr(_local, "dynamodb", None) is None:
        _local.dynamodb = boto3.resource("dynamodb", config=_CONFIG)
    return _local.dynamodb


def table() -> Any:
    return dynamodb_resource().Table(require_env("TABLE_NAME"))


def ddb_client() -> Any:
    """Low-level DynamoDB client (shares the resource's (de)serialisation of Python types)."""
    return dynamodb_resource().meta.client


def get_account_id() -> str:
    override = os.environ.get("ACCOUNT_ID")
    if override:
        return override
    return _sts_account_id()


@lru_cache(maxsize=1)
def _sts_account_id() -> str:
    return client("sts").get_caller_identity()["Account"]


def reset_caches() -> None:
    """Drop cached clients (used by tests so each test gets a fresh mocked AWS)."""
    _cached_client.cache_clear()
    _local.dynamodb = None
    _sts_account_id.cache_clear()
    _OVERRIDES.clear()
