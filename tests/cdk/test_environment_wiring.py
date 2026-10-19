"""Run the real pipeline with each Lambda seeing ONLY the environment CDK gives it.

If a handler reads a variable that the stack forgot to pass (a classic deploy-time failure
that unit tests with a global environment never catch), this test fails.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from aws_cdk import assertions

from tests.helpers import api_event

pytestmark = pytest.mark.slow

APP_VARIABLES = {
    "TABLE_NAME",
    "BUCKET_NAME",
    "SYSTEM_OPS_TOPIC_ARN",
    "EXTERNAL_OPS_TOPIC_ARN",
    "NEWS_FETCHER_NAME",
}
FIXTURE = Path(__file__).parents[2] / "local_app" / "fixtures" / "market.json"


@pytest.fixture(scope="module")
def declared_env():
    import app as cdk_app

    template = assertions.Template.from_stack(cdk_app.build_app().node.find_child("AppStack"))
    declared = {}
    for resource in template.find_resources("AWS::Lambda::Function").values():
        package = resource["Properties"]["Handler"].split(".")[0]
        declared[package] = set(resource["Properties"]["Environment"]["Variables"])
    return declared


def test_pipeline_runs_with_only_the_declared_environment(
    local_cloud_runtime, table, declared_env, monkeypatch
):
    monkeypatch.setenv("NEWS_PROVIDER", "fixture")
    monkeypatch.setenv("NEWS_FIXTURE_PATH", str(FIXTURE))
    runtime = local_cloud_runtime
    original_handler = runtime.handler

    def restricted(function):
        real = original_handler(function)

        def run(event, context):
            saved = {
                k: os.environ.pop(k)
                for k in APP_VARIABLES - declared_env[function]
                if k in os.environ
            }
            try:
                return real(event, context)
            finally:
                os.environ.update(saved)

        return run

    monkeypatch.setattr(runtime, "handler", restricted)

    response = runtime.invoke(
        "control_plane_api",
        api_event("POST", "/watchlist", {"symbol": "TSLA", "shares_managed": 1}),
    )
    assert response["statusCode"] == 202
    for _ in range(5):
        if runtime.drain() == 0:
            break

    item = table.get_item(Key={"PK": "ACCOUNT#123456789012", "SK": "STOCK#TSLA"})["Item"]
    assert item["last_headline"], "news did not travel through every Lambda"
    assert int(item["decision_count"]) >= 1, json.dumps(sorted(declared_env))

    # scheduled / API-only functions too
    for function in ("networth_snapshot_writer", "news_retention_cleaner"):
        runtime.invoke(function, {})
    plot = runtime.invoke(
        "networth_plotter",
        {"httpMethod": "GET", "path": "/networth", "queryStringParameters": None},
    )
    assert plot["statusCode"] == 200
