"""Assertions on the synthesized CloudFormation (no AWS access needed)."""

from __future__ import annotations

from pathlib import Path

import pytest
from aws_cdk import assertions

from common import messages, topology

pytestmark = pytest.mark.slow

LAMBDA_DIR = Path(__file__).parents[2] / "lambda"


@pytest.fixture(scope="module")
def app():
    import app as cdk_app  # cdk/app.py

    # asset metadata lets the tests map each Lambda to its packaged directory
    return cdk_app.build_app({"aws:cdk:enable-asset-metadata": True})


@pytest.fixture(scope="module")
def stacks(app):
    return {
        name: assertions.Template.from_stack(app.node.find_child(name))
        for name in ("DataStack", "MessagingStack", "AppStack")
    }


@pytest.fixture(scope="module")
def data(stacks):
    return stacks["DataStack"]


@pytest.fixture(scope="module")
def messaging(stacks):
    return stacks["MessagingStack"]


@pytest.fixture(scope="module")
def application(stacks):
    return stacks["AppStack"]


def functions(template):
    """handler package -> (logical id, properties)."""
    found = {}
    for logical_id, resource in template.find_resources("AWS::Lambda::Function").items():
        properties = resource["Properties"]
        package = properties["Handler"].split(".")[0]
        found[package] = (logical_id, properties, resource)
    return found


class TestDataStack:
    def test_single_table_with_pk_sk(self, data):
        data.has_resource_properties(
            "AWS::DynamoDB::Table",
            {
                "TableName": "TradingTable",
                "KeySchema": [
                    {"AttributeName": "PK", "KeyType": "HASH"},
                    {"AttributeName": "SK", "KeyType": "RANGE"},
                ],
                "BillingMode": "PAY_PER_REQUEST",
                "TimeToLiveSpecification": {"AttributeName": "ttl", "Enabled": True},
            },
        )
        data.resource_count_is("AWS::DynamoDB::Table", 1)

    def test_bucket_is_private_encrypted_and_ssl_only(self, data):
        data.has_resource_properties(
            "AWS::S3::Bucket",
            {
                "PublicAccessBlockConfiguration": {
                    "BlockPublicAcls": True,
                    "BlockPublicPolicy": True,
                    "IgnorePublicAcls": True,
                    "RestrictPublicBuckets": True,
                },
                "BucketEncryption": assertions.Match.any_value(),
                "LifecycleConfiguration": {
                    "Rules": [
                        assertions.Match.object_like(
                            {"Prefix": "plots/", "ExpirationInDays": 7, "Status": "Enabled"}
                        )
                    ]
                },
            },
        )
        data.has_resource_properties(
            "AWS::S3::BucketPolicy",
            {
                "PolicyDocument": {
                    "Statement": assertions.Match.array_with(
                        [
                            assertions.Match.object_like(
                                {
                                    "Effect": "Deny",
                                    "Condition": {"Bool": {"aws:SecureTransport": "false"}},
                                }
                            )
                        ]
                    )
                }
            },
        )

    def test_demo_friendly_removal_by_default(self, data):
        data.has_resource("AWS::DynamoDB::Table", {"DeletionPolicy": "Delete"})

    def test_retain_data_context_keeps_state(self):
        import aws_cdk as cdk

        from infra_stack import DataStack

        app = cdk.App(context={"retain_data": "true"})
        template = assertions.Template.from_stack(DataStack(app, "RetainingData"))
        template.has_resource("AWS::DynamoDB::Table", {"DeletionPolicy": "Retain"})
        template.resource_count_is("Custom::S3AutoDeleteObjects", 0)


class TestMessagingStack:
    def test_two_topics_enforce_ssl(self, messaging):
        messaging.resource_count_is("AWS::SNS::Topic", 2)
        messaging.has_resource_properties(
            "AWS::SNS::TopicPolicy",
            {
                "PolicyDocument": {
                    "Statement": assertions.Match.array_with(
                        [assertions.Match.object_like({"Effect": "Deny"})]
                    )
                }
            },
        )

    def test_every_consumer_queue_has_a_dlq_with_redrive(self, messaging):
        queues = messaging.find_resources("AWS::SQS::Queue")
        assert len(queues) == 2 * len(topology.QUEUES)
        for spec in topology.QUEUES:
            messaging.has_resource_properties(
                "AWS::SQS::Queue",
                {
                    "VisibilityTimeout": spec.visibility_timeout_s,
                    "RedrivePolicy": {
                        "maxReceiveCount": spec.max_receive_count,
                        "deadLetterTargetArn": assertions.Match.any_value(),
                    },
                },
            )

    def test_dead_letter_queues_keep_messages_for_14_days(self, messaging):
        dlqs = [
            r
            for r in messaging.find_resources("AWS::SQS::Queue").values()
            if "RedrivePolicy" not in r["Properties"]
        ]
        assert len(dlqs) == len(topology.QUEUES)
        assert all(r["Properties"]["MessageRetentionPeriod"] == 14 * 86400 for r in dlqs)

    def test_subscriptions_filter_on_message_type(self, messaging):
        subscriptions = messaging.find_resources("AWS::SNS::Subscription")
        assert len(subscriptions) == len(topology.QUEUES)
        policies = sorted(
            tuple(sorted(s["Properties"]["FilterPolicy"]["type"])) for s in subscriptions.values()
        )
        assert policies == sorted(tuple(sorted(q.message_types)) for q in topology.QUEUES)
        for subscription in subscriptions.values():
            assert subscription["Properties"]["Protocol"] == "sqs"
            # raw delivery would drop the SNS envelope the handlers unwrap
            assert not subscription["Properties"].get("RawMessageDelivery")

    def test_each_message_type_is_routed_exactly_once(self, messaging):
        seen = []
        for subscription in messaging.find_resources("AWS::SNS::Subscription").values():
            seen += subscription["Properties"]["FilterPolicy"]["type"]
        assert len(seen) == len(set(seen))
        # every type the code publishes has a consumer
        published = {
            getattr(messages, name)
            for name in dir(messages)
            if name.isupper() and isinstance(getattr(messages, name), str) and name != "SYMBOL_RE"
        }
        assert published == set(seen)

    def test_outputs_for_the_local_worker(self, messaging):
        outputs = messaging.to_json()["Outputs"]
        assert {"TradeDecisionQueueUrl", "SystemOpsTopicArn", "ExternalOpsTopicArn"} <= set(outputs)


class TestAppStack:
    def test_all_functions_present_on_python_312(self, application):
        found = functions(application)
        assert set(found) == set(topology.ALL_FUNCTIONS)
        for _, properties, _ in found.values():
            assert properties["Runtime"] == "python3.12"
            assert properties["Handler"].endswith(".main.handler")
            assert properties["Architectures"] == ["x86_64"]

    def test_each_asset_contains_only_its_package_plus_common(self, application, app):
        assembly_dir = Path(app.synth().directory)
        for package, (_, _, resource) in functions(application).items():
            asset_path = resource["Metadata"]["aws:asset:path"]
            contents = {p.name for p in (assembly_dir / asset_path).iterdir()}
            assert contents == {"common", package}, package

    def test_sqs_triggers_report_partial_failures(self, application):
        mappings = application.find_resources("AWS::Lambda::EventSourceMapping")
        lambda_consumers = [q for q in topology.QUEUES if q.consumer != topology.LOCAL_WORKER]
        assert len(mappings) == len(lambda_consumers)
        for mapping in mappings.values():
            assert mapping["Properties"]["FunctionResponseTypes"] == ["ReportBatchItemFailures"]
        batch_sizes = sorted(m["Properties"]["BatchSize"] for m in mappings.values())
        assert batch_sizes == sorted(q.batch_size for q in lambda_consumers)

    def test_queue_visibility_covers_six_times_the_function_timeout(self, application):
        """AWS guidance: SQS visibility timeout >= 6 x consumer timeout (avoids duplicate runs)."""
        found = functions(application)
        for spec in topology.QUEUES:
            if spec.consumer == topology.LOCAL_WORKER:
                continue
            timeout = found[spec.consumer][1]["Timeout"]
            assert spec.visibility_timeout_s >= 6 * timeout, spec.name

    def test_schedules(self, application):
        rules = application.find_resources("AWS::Events::Rule")
        expressions = sorted(r["Properties"]["ScheduleExpression"] for r in rules.values())
        assert expressions == ["rate(1 day)", "rate(15 minutes)", "rate(5 minutes)"]

    def test_fetch_interval_is_configurable(self):
        import aws_cdk as cdk

        from infra_stack import AppStack, DataStack, MessagingStack

        app = cdk.App(context={"fetch_interval_minutes": "30"})
        data, messaging = DataStack(app, "D"), MessagingStack(app, "M")
        template = assertions.Template.from_stack(
            AppStack(app, "A", data_stack=data, messaging_stack=messaging)
        )
        template.has_resource_properties(
            "AWS::Events::Rule", {"ScheduleExpression": "rate(30 minutes)"}
        )

    def test_async_invocation_failures_are_captured(self, application):
        found = functions(application)
        for package in ("news_fetcher", "networth_snapshot_writer", "news_retention_cleaner"):
            assert "DeadLetterConfig" in found[package][1], package

    def test_layers_exist_for_heavy_dependencies(self, application):
        application.resource_count_is("AWS::Lambda::LayerVersion", 3)
        found = functions(application)
        for package in ("news_fetcher", "networth_plotter", "sentiment_scorer"):
            assert found[package][1]["Layers"], package
        assert "Layers" not in found["stock_info_writer"][1]

    def test_log_groups_have_retention(self, application):
        groups = application.find_resources("AWS::Logs::LogGroup")
        assert len(groups) == len(topology.ALL_FUNCTIONS)
        assert all(g["Properties"]["RetentionInDays"] == 30 for g in groups.values())

    def test_environment_variables(self, application):
        env = {p: f[1]["Environment"]["Variables"] for p, f in functions(application).items()}
        assert {
            "TABLE_NAME",
            "NEWS_FETCHER_NAME",
            "EXTERNAL_OPS_TOPIC_ARN",
            "SYSTEM_OPS_TOPIC_ARN",
        } <= set(env["control_plane_api"])
        assert "BUCKET_NAME" in env["news_writer"] and "TABLE_NAME" not in env["news_writer"]

    def test_apis_require_an_api_key_and_are_throttled(self, application):
        application.resource_count_is("AWS::ApiGateway::RestApi", 2)
        methods = application.find_resources("AWS::ApiGateway::Method")
        assert methods and all(m["Properties"]["ApiKeyRequired"] for m in methods.values())
        application.has_resource_properties(
            "AWS::ApiGateway::UsagePlan",
            {"Throttle": {"RateLimit": 10, "BurstLimit": 20}},
        )
        application.resource_count_is("AWS::ApiGateway::UsagePlanKey", 1)
        stages = application.find_resources("AWS::ApiGateway::Stage")
        assert len(stages) == 2

    def test_networth_endpoint_is_get_only(self, application):
        application.has_resource_properties(
            "AWS::ApiGateway::Method", {"HttpMethod": "GET", "ApiKeyRequired": True}
        )
        application.has_resource_properties("AWS::ApiGateway::Resource", {"PathPart": "networth"})

    def test_outputs_used_by_the_local_app(self, application):
        outputs = application.to_json()["Outputs"]
        assert {"ManagementApiUrl", "NetWorthApiUrl", "ApiKeyId"} <= set(outputs)

    def _statements(self, application, role_prefix):
        statements = []
        for resource in application.find_resources("AWS::IAM::Policy").values():
            if role_prefix in str(resource["Properties"]["Roles"]):
                statements += resource["Properties"]["PolicyDocument"]["Statement"]
        return statements

    def test_least_privilege_s3_prefixes(self, application):
        """The writer may only put under news/, the plotter only under plots/."""

        def s3_resources(prefix):
            return str(
                [
                    s["Resource"]
                    for s in self._statements(application, prefix)
                    if any(
                        a.startswith("s3:")
                        for a in (s["Action"] if isinstance(s["Action"], list) else [s["Action"]])
                    )
                ]
            )

        writer = s3_resources("NewsWriterLambdaServiceRole")
        plotter = s3_resources("NetWorthPlotterLambdaServiceRole")
        assert "/news/*" in writer and "/plots/*" not in writer
        assert "/plots/*" in plotter and "/news/*" not in plotter
        # the control plane never touches S3
        assert s3_resources("ControlPlaneApiLambdaServiceRole") == "[]"

    def test_no_wildcard_actions(self, application):
        for resource in application.find_resources("AWS::IAM::Policy").values():
            for statement in resource["Properties"]["PolicyDocument"]["Statement"]:
                actions = (
                    statement["Action"]
                    if isinstance(statement["Action"], list)
                    else [statement["Action"]]
                )
                assert all("*" not in action for action in actions), actions

    def test_dynamodb_grants_match_handler_operations(self, application):
        allowed = {
            "NewsFetcher": {"GetItem", "PutItem", "UpdateItem", "Query"},
            "SentimentScorer": {"GetItem", "PutItem", "UpdateItem"},
            "StockInfoWriter": {"GetItem", "PutItem", "UpdateItem", "Query"},
            "SoftDelete": {"GetItem", "PutItem", "UpdateItem"},
            "NewsRetentionCleaner": {"Scan"},
            "NetWorthSnapshotWriter": {"Query", "PutItem", "UpdateItem"},
            "NetWorthPlotter": {"Query"},
            "ControlPlaneApi": {"GetItem", "PutItem", "UpdateItem"},
        }
        for name, expected in allowed.items():
            statements = self._statements(application, f"{name}LambdaServiceRole")
            actions = {
                action
                for s in statements
                for action in (s["Action"] if isinstance(s["Action"], list) else [s["Action"]])
                if action.startswith("dynamodb:")
            }
            assert actions == {f"dynamodb:{action}" for action in expected}, name

    def test_only_cleaners_can_list_news_and_cannot_overwrite_objects(self, application):
        for name in ("SoftDelete", "NewsRetentionCleaner"):
            statements = self._statements(application, f"{name}LambdaServiceRole")
            s3_statements = [s for s in statements if "s3:" in str(s["Action"])]
            actions = {
                a
                for s in s3_statements
                for a in (s["Action"] if isinstance(s["Action"], list) else [s["Action"]])
            }
            assert actions == {
                "s3:ListBucket",
                "s3:GetObjectTagging",
                "s3:PutObjectTagging",
                "s3:DeleteObject",
            }
            for statement in s3_statements:
                if "s3:ListBucket" in statement["Action"]:
                    assert statement["Condition"] == {"StringLike": {"s3:prefix": "news/*"}}
                else:
                    assert "/news/*" in str(statement["Resource"])

    def test_fetcher_cannot_be_invoked_by_anything_but_the_control_plane_and_events(
        self, application
    ):
        permissions = application.find_resources("AWS::Lambda::Permission")
        principals = {p["Properties"]["Principal"] for p in permissions.values()}
        assert principals <= {"events.amazonaws.com", "apigateway.amazonaws.com"}


def test_unbuilt_layers_are_flagged_with_a_warning(app):
    """Synth works offline with placeholders, but must tell the user not to deploy that."""
    stack = app.node.find_child("AppStack")
    built = {p.name for p in (Path(__file__).parents[2] / "layers" / "build").glob("*")}
    warnings = assertions.Annotations.from_stack(stack).find_warning(
        "*", assertions.Match.string_like_regexp("Layer '.*' is not built.*")
    )
    expected_unbuilt = {"yfinance", "matplotlib", "sentiment"} - built
    assert len(warnings) == len(expected_unbuilt)


def test_handler_environment_is_sufficient_for_the_pipeline(application):
    """Every variable a Lambda reads must be declared in its CDK environment block."""
    declared = {p: set(f[1]["Environment"]["Variables"]) for p, f in functions(application).items()}
    required = {
        "news_fetcher": {"TABLE_NAME", "SYSTEM_OPS_TOPIC_ARN"},
        "news_writer": {"BUCKET_NAME", "SYSTEM_OPS_TOPIC_ARN"},
        "sentiment_scorer": {"TABLE_NAME", "BUCKET_NAME", "SYSTEM_OPS_TOPIC_ARN"},
        "stock_info_writer": {"TABLE_NAME", "SYSTEM_OPS_TOPIC_ARN"},
        "soft_delete": {"TABLE_NAME", "BUCKET_NAME"},
        "news_retention_cleaner": {"TABLE_NAME", "BUCKET_NAME"},
        "networth_snapshot_writer": {"TABLE_NAME"},
        "networth_plotter": {"TABLE_NAME", "BUCKET_NAME"},
        "control_plane_api": {
            "TABLE_NAME",
            "NEWS_FETCHER_NAME",
            "EXTERNAL_OPS_TOPIC_ARN",
            "SYSTEM_OPS_TOPIC_ARN",
        },
    }
    for package, names in required.items():
        assert names <= declared[package], f"{package} is missing {names - declared[package]}"


def test_cdk_app_imports_do_not_need_boto3():
    """The CDK project only depends on aws-cdk-lib/constructs; topology must stay boto3-free."""
    import subprocess
    import sys

    code = (
        "import sys; sys.modules['boto3'] = None; sys.path.insert(0, sys.argv[1]);"
        "from common import topology; assert len(topology.QUEUES) == 6"
    )
    result = subprocess.run(
        [sys.executable, "-c", code, str(LAMBDA_DIR)], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
