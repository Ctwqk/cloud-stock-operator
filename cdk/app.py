#!/usr/bin/env python3
"""CDK entry point.

Context (``-c key=value``):
    account, region          deployment target (default: CDK_DEFAULT_ACCOUNT / CDK_DEFAULT_REGION);
                             without either, stacks are environment-agnostic (enough to synth)
    fetch_interval_minutes   news fetch schedule (default 5)
    retain_data=true         keep the table and bucket on `cdk destroy`
"""

import os

import aws_cdk as cdk

from infra_stack import AppStack, DataStack, MessagingStack


def build_app(context: dict[str, object] | None = None) -> cdk.App:
    app = cdk.App(context=context)

    account = app.node.try_get_context("account") or os.environ.get("CDK_DEFAULT_ACCOUNT")
    region = app.node.try_get_context("region") or os.environ.get("CDK_DEFAULT_REGION")
    env = cdk.Environment(account=account, region=region) if account or region else None

    data_stack = DataStack(app, "DataStack", env=env)
    messaging_stack = MessagingStack(app, "MessagingStack", env=env)
    AppStack(
        app,
        "AppStack",
        data_stack=data_stack,
        messaging_stack=messaging_stack,
        env=env,
    )
    cdk.Tags.of(app).add("project", "cloud-stock-operator")
    return app


if __name__ == "__main__":
    build_app().synth()
