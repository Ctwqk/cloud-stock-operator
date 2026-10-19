"""CDK stacks for the cloud-stock-operator system.

DataStack       DynamoDB single table + S3 bucket (stateful resources)
MessagingStack  SNS topics, one SQS queue (+ DLQ) per consumer, SNS filter subscriptions
AppStack        Lambda functions, event sources, EventBridge schedules, API Gateway

The messaging layout is generated from ``common.topology`` (shared with the local runtime).
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from aws_cdk import Annotations, CfnOutput, Duration, RemovalPolicy, Stack
from aws_cdk import aws_apigateway as apigw
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_lambda_event_sources as lambda_event_sources
from aws_cdk import aws_logs as logs
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as subs
from aws_cdk import aws_sqs as sqs
from constructs import Construct

ROOT = Path(__file__).resolve().parent.parent
LAMBDA_DIR = ROOT / "lambda"
LAYERS_BUILD_DIR = ROOT / "layers" / "build"
LAYERS_PLACEHOLDER_DIR = ROOT / "layers" / "placeholder"

sys.path.insert(0, str(LAMBDA_DIR))
from common import topology  # noqa: E402  (needs the sys.path entry above)


def _retain_data(scope: Construct) -> bool:
    return str(scope.node.try_get_context("retain_data")).lower() == "true"


def _removal_policy(scope: Construct) -> RemovalPolicy:
    """DESTROY by default (demo friendly); ``-c retain_data=true`` keeps table and bucket."""
    return RemovalPolicy.RETAIN if _retain_data(scope) else RemovalPolicy.DESTROY


class DataStack(Stack):
    """Stateful resources: the single DynamoDB table and the news/plots bucket."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs: Any) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.table_t = dynamodb.Table(
            self,
            "TradingTable",
            table_name="TradingTable",
            partition_key=dynamodb.Attribute(name="PK", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="SK", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            # De-duplication markers, operation results etc. carry a `ttl` epoch attribute.
            time_to_live_attribute="ttl",
            removal_policy=_removal_policy(self),
        )

        self.bucket_s = s3.Bucket(
            self,
            "NewsAndPlotsBucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            # Rendered plots are throw-away: presigned URLs live one hour.
            lifecycle_rules=[
                s3.LifecycleRule(id="ExpirePlots", prefix="plots/", expiration=Duration.days(7))
            ],
            removal_policy=_removal_policy(self),
            auto_delete_objects=not _retain_data(self),
        )

        CfnOutput(self, "TableName", value=self.table_t.table_name)
        CfnOutput(self, "BucketName", value=self.bucket_s.bucket_name)


class MessagingStack(Stack):
    """SNS topics and the per-consumer SQS queues (each with a dead-letter queue)."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs: Any) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.external_ops_topic = sns.Topic(self, "ExternalOpsTopic", enforce_ssl=True)
        self.system_ops_topic = sns.Topic(self, "SystemOpsTopic", enforce_ssl=True)
        topics = {
            topology.EXTERNAL_TOPIC: self.external_ops_topic,
            topology.SYSTEM_TOPIC: self.system_ops_topic,
        }

        self.queues: dict[str, sqs.Queue] = {}
        self.dead_letter_queues: dict[str, sqs.Queue] = {}
        for spec in topology.QUEUES:
            dlq = sqs.Queue(
                self,
                f"{spec.name}DLQ",
                retention_period=Duration.days(14),
                encryption=sqs.QueueEncryption.SQS_MANAGED,
                enforce_ssl=True,
            )
            queue = sqs.Queue(
                self,
                spec.name,
                visibility_timeout=Duration.seconds(spec.visibility_timeout_s),
                encryption=sqs.QueueEncryption.SQS_MANAGED,
                enforce_ssl=True,
                dead_letter_queue=sqs.DeadLetterQueue(
                    max_receive_count=spec.max_receive_count, queue=dlq
                ),
            )
            # Route by the `type` message attribute: each queue only receives its own types.
            topics[spec.topic].add_subscription(
                subs.SqsSubscription(
                    queue,
                    filter_policy={
                        "type": sns.SubscriptionFilter.string_filter(
                            allowlist=list(spec.message_types)
                        )
                    },
                )
            )
            self.queues[spec.name] = queue
            self.dead_letter_queues[spec.name] = dlq

        # Convenience aliases used by the original stack / docs.
        self.external_ops_queue = self.queues["ExternalOpsQueue"]
        self.trade_decision_queue = self.queues["TradeDecisionQueue"]

        CfnOutput(self, "TradeDecisionQueueUrl", value=self.trade_decision_queue.queue_url)
        CfnOutput(self, "ExternalOpsTopicArn", value=self.external_ops_topic.topic_arn)
        CfnOutput(self, "SystemOpsTopicArn", value=self.system_ops_topic.topic_arn)


class AppStack(Stack):
    """Lambda functions, their triggers and the API Gateway entry points."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        data_stack: DataStack,
        messaging_stack: MessagingStack,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        table = data_stack.table_t
        bucket = data_stack.bucket_s
        external_topic = messaging_stack.external_ops_topic
        system_topic = messaging_stack.system_ops_topic

        self.functions: dict[str, _lambda.Function] = {}
        yfinance_layer = self._layer("YFinanceLayer", "yfinance", "yfinance for NewsFetcher")
        matplotlib_layer = self._layer("MatplotlibLayer", "matplotlib", "matplotlib for Plotter")
        sentiment_layer = self._layer("SentimentLayer", "sentiment", "VADER for SentimentScorer")

        # Failed *asynchronous* invocations (EventBridge, API refresh) land here after retries.
        async_failures = sqs.Queue(
            self,
            "AsyncInvocationFailures",
            retention_period=Duration.days(14),
            encryption=sqs.QueueEncryption.SQS_MANAGED,
            enforce_ssl=True,
        )

        # --- Lambda A: news fetcher (schedule + STOCK_ADDED) -------------------------------
        news_fetcher = self._function(
            "NewsFetcherLambda",
            "news_fetcher",
            timeout=300,
            memory=512,
            layers=[yfinance_layer],
            environment={
                "TABLE_NAME": table.table_name,
                "SYSTEM_OPS_TOPIC_ARN": system_topic.topic_arn,
            },
            dead_letter_queue=async_failures,
        )
        table.grant_read_write_data(news_fetcher)
        system_topic.grant_publish(news_fetcher)
        fetch_minutes = int(self.node.try_get_context("fetch_interval_minutes") or 5)
        events.Rule(
            self,
            "NewsFetcherScheduleRule",
            schedule=events.Schedule.rate(Duration.minutes(fetch_minutes)),
            targets=[targets.LambdaFunction(news_fetcher)],
        )

        # --- Lambda F: news writer (NEW_NEWS -> S3 -> NEWS_STORED) ---------------------------
        news_writer = self._function(
            "NewsWriterLambda",
            "news_writer",
            environment={
                "BUCKET_NAME": bucket.bucket_name,
                "SYSTEM_OPS_TOPIC_ARN": system_topic.topic_arn,
            },
        )
        bucket.grant_put(news_writer, "news/*")
        system_topic.grant_publish(news_writer)

        # --- Lambda B: sentiment scorer (NEWS_STORED -> score -> AUTO_TRADE_DECISION) -----------
        sentiment_scorer = self._function(
            "SentimentScorerLambda",
            "sentiment_scorer",
            memory=256,
            layers=[sentiment_layer],
            environment={
                "TABLE_NAME": table.table_name,
                "BUCKET_NAME": bucket.bucket_name,
                "SYSTEM_OPS_TOPIC_ARN": system_topic.topic_arn,
            },
        )
        table.grant_read_write_data(sentiment_scorer)
        bucket.grant_read(sentiment_scorer, "news/*")
        system_topic.grant_publish(sentiment_scorer)

        # --- Lambda D: external ops (ADD_STOCK / ADJUST_*) ---------------------------------
        stock_info_writer = self._function(
            "StockInfoWriterLambda",
            "stock_info_writer",
            environment={
                "TABLE_NAME": table.table_name,
                "SYSTEM_OPS_TOPIC_ARN": system_topic.topic_arn,
            },
        )
        table.grant_read_write_data(stock_info_writer)
        system_topic.grant_publish(stock_info_writer)

        # --- Lambda E: soft delete / cleanup -------------------------------------------------
        soft_delete = self._function(
            "SoftDeleteLambda",
            "soft_delete",
            environment={"TABLE_NAME": table.table_name, "BUCKET_NAME": bucket.bucket_name},
        )
        table.grant_read_write_data(soft_delete)
        bucket.grant_read_write(soft_delete, "news/*")

        # --- Daily news retention cleaner ----------------------------------------------------
        retention_cleaner = self._function(
            "NewsRetentionCleanerLambda",
            "news_retention_cleaner",
            timeout=300,
            environment={"TABLE_NAME": table.table_name, "BUCKET_NAME": bucket.bucket_name},
            dead_letter_queue=async_failures,
        )
        table.grant_read_data(retention_cleaner)
        bucket.grant_read_write(retention_cleaner, "news/*")
        events.Rule(
            self,
            "NewsRetentionCleanupRule",
            schedule=events.Schedule.rate(Duration.days(1)),
            targets=[targets.LambdaFunction(retention_cleaner)],
        )

        # --- Net-worth snapshots (every 15 min) ----------------------------------------------
        snapshot_writer = self._function(
            "NetWorthSnapshotWriterLambda",
            "networth_snapshot_writer",
            environment={"TABLE_NAME": table.table_name},
            dead_letter_queue=async_failures,
        )
        table.grant_read_write_data(snapshot_writer)
        events.Rule(
            self,
            "NetWorthSnapshotScheduleRule",
            schedule=events.Schedule.rate(Duration.minutes(15)),
            targets=[targets.LambdaFunction(snapshot_writer)],
        )

        # --- Lambda C: net-worth plotter (API Gateway GET /networth) ---------------------------
        plotter = self._function(
            "NetWorthPlotterLambda",
            "networth_plotter",
            memory=512,
            layers=[matplotlib_layer],
            environment={"TABLE_NAME": table.table_name, "BUCKET_NAME": bucket.bucket_name},
        )
        table.grant_read_data(plotter)
        bucket.grant_put(plotter, "plots/*")
        bucket.grant_read(plotter, "plots/*")  # the presigned URL is signed with this role

        # --- Control-plane API ---------------------------------------------------------------
        control_plane = self._function(
            "ControlPlaneApiLambda",
            "control_plane_api",
            environment={
                "TABLE_NAME": table.table_name,
                "EXTERNAL_OPS_TOPIC_ARN": external_topic.topic_arn,
                "SYSTEM_OPS_TOPIC_ARN": system_topic.topic_arn,
                "NEWS_FETCHER_NAME": news_fetcher.function_name,
            },
        )
        table.grant_read_write_data(control_plane)
        external_topic.grant_publish(control_plane)
        system_topic.grant_publish(control_plane)
        news_fetcher.grant_invoke(control_plane)

        stage_options = apigw.StageOptions(
            stage_name="prod", throttling_rate_limit=10, throttling_burst_limit=20
        )
        require_key = apigw.MethodOptions(api_key_required=True)
        management_api = apigw.LambdaRestApi(
            self,
            "ManagementApi",
            handler=control_plane,
            proxy=True,
            rest_api_name="ManagementService",
            deploy_options=stage_options,
            default_method_options=require_key,
        )
        networth_api = apigw.LambdaRestApi(
            self,
            "NetWorthApi",
            handler=plotter,
            proxy=False,
            rest_api_name="NetWorthService",
            deploy_options=stage_options,
            default_method_options=require_key,
        )
        networth_api.root.add_resource("networth").add_method("GET", api_key_required=True)

        # One API key + usage plan for both APIs. A key is a shared secret, not user
        # authentication: put Cognito/IAM authorisation in front before exposing this widely.
        api_key = apigw.ApiKey(self, "TradingApiKey")
        usage_plan = apigw.UsagePlan(
            self,
            "TradingUsagePlan",
            throttle=apigw.ThrottleSettings(rate_limit=10, burst_limit=20),
            api_stages=[
                apigw.UsagePlanPerApiStage(
                    api=management_api, stage=management_api.deployment_stage
                ),
                apigw.UsagePlanPerApiStage(api=networth_api, stage=networth_api.deployment_stage),
            ],
        )
        usage_plan.add_api_key(api_key)

        # SQS -> Lambda event source mappings, generated from the shared topology.
        for spec in topology.QUEUES:
            if spec.consumer == topology.LOCAL_WORKER:
                continue  # polled from outside AWS by local_app/local_trade_worker.py
            self.functions[spec.consumer].add_event_source(
                lambda_event_sources.SqsEventSource(
                    messaging_stack.queues[spec.name],
                    batch_size=spec.batch_size,
                    report_batch_item_failures=True,
                )
            )

        CfnOutput(self, "ManagementApiUrl", value=management_api.url)
        CfnOutput(self, "NetWorthApiUrl", value=networth_api.url)
        CfnOutput(self, "ApiKeyId", value=api_key.key_id)

    # -- helpers -------------------------------------------------------------------------------
    def _layer(self, construct_id: str, name: str, description: str) -> _lambda.LayerVersion:
        """Layer from ``layers/build/<name>`` (made by ``scripts/build_layers.sh``).

        If it has not been built, synthesis still works (so tests and CI need no network) but
        a placeholder is used and a warning is attached: do not deploy in that state.
        """
        built = LAYERS_BUILD_DIR / name
        if built.is_dir():
            path = built
        else:
            path = LAYERS_PLACEHOLDER_DIR
            Annotations.of(self).add_warning(
                f"Layer '{name}' is not built (run scripts/build_layers.sh before cdk deploy); "
                "using an empty placeholder."
            )
        return _lambda.LayerVersion(
            self,
            construct_id,
            code=_lambda.Code.from_asset(str(path)),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_12],
            compatible_architectures=[_lambda.Architecture.X86_64],
            description=description,
        )

    def _function(
        self,
        construct_id: str,
        package: str,
        *,
        environment: dict[str, str],
        timeout: int = 30,
        memory: int = 256,
        layers: list[_lambda.ILayerVersion] | None = None,
        dead_letter_queue: sqs.IQueue | None = None,
    ) -> _lambda.Function:
        """One Lambda from ``lambda/<package>``; the asset also carries ``lambda/common``."""
        other_packages = [p for p in topology.ALL_FUNCTIONS if p != package]
        function = _lambda.Function(
            self,
            construct_id,
            runtime=_lambda.Runtime.PYTHON_3_12,
            architecture=_lambda.Architecture.X86_64,
            handler=f"{package}.main.handler",
            code=_lambda.Code.from_asset(
                str(LAMBDA_DIR), exclude=[*other_packages, "**/__pycache__", "**/*.pyc"]
            ),
            timeout=Duration.seconds(timeout),
            memory_size=memory,
            layers=layers or [],
            environment={"LOG_LEVEL": "INFO", **environment},
            dead_letter_queue=dead_letter_queue,
            log_group=logs.LogGroup(
                self,
                f"{construct_id}Logs",
                retention=logs.RetentionDays.ONE_MONTH,
                removal_policy=RemovalPolicy.DESTROY,
            ),
        )
        self.functions[package] = function
        return function
