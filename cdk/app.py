#!/usr/bin/env python3
import aws_cdk as cdk

from infra_stack import AppStack, DataStack, MessagingStack


def main() -> None:
    app = cdk.App()

    env = cdk.Environment(
            account=app.node.try_get_context("account"),
            region=app.node.try_get_context("region"),
    )

    data_stack = DataStack(
        app,
        "DataStack",
        env=env,
    )

    messaging_stack = MessagingStack(
        app,
        "MessagingStack",
        env=env,
    )

    AppStack(
        app,
        "AppStack",
        data_stack=data_stack,
        messaging_stack=messaging_stack,
        env=env,
    )

    app.synth()


if __name__ == "__main__":
    main()


