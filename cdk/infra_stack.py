from typing import Any

from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_dynamodb as dynamodb,
    aws_s3 as s3,
    aws_sns as sns,
    aws_sqs as sqs,
    aws_sns_subscriptions as subs,
    aws_lambda as _lambda,
    aws_lambda_event_sources as lambda_event_sources,
    aws_events as events,
    aws_events_targets as targets,
    aws_s3_notifications as s3_notifications,
    aws_apigateway as apigw,
)
from constructs import Construct


class DataStack(Stack):
    """
    Stack 1 – DataStack
 
    Contains persistent data resources:
    - DynamoDB table T (single-table design with PK/SK)
    - S3 bucket S for news and net-worth plots
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs: Any) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # DynamoDB table T
        self.table_t = dynamodb.Table(
            self,
            "TradingTable",
            table_name="TradingTable",
            partition_key=dynamodb.Attribute(
                name="PK", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="SK", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=_default_removal_policy(),
        )

        # NOTE: S3 bucket S is created in AppStack to avoid cyclic dependencies
        # when wiring S3 notifications to Lambdas.
 
 
class MessagingStack(Stack):
    """
    Stack 2 – MessagingStack
 
    Contains SNS topics and SQS queues:
    - ExternalOpsTopic → ExternalOpsQueue (Q1)
    - SystemOpsTopic → SystemOpsQueue (Q2)
    """
 
    def __init__(self, scope: Construct, construct_id: str, **kwargs: Any) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # SNS topics
        self.external_ops_topic = sns.Topic(self, "ExternalOpsTopic")
        self.system_ops_topic = sns.Topic(self, "SystemOpsTopic")

        # SQS queues
        self.external_ops_queue = sqs.Queue(
            self,
            "ExternalOpsQueue",
            visibility_timeout=Duration.seconds(300),
        )
        self.system_ops_queue = sqs.Queue(
            self,
            "SystemOpsQueue",
            visibility_timeout=Duration.seconds(300),
        )

        # SNS → SQS subscriptions
        self.external_ops_topic.add_subscription(
            subs.SqsSubscription(self.external_ops_queue)
        )
        self.system_ops_topic.add_subscription(
            subs.SqsSubscription(self.system_ops_queue)
        )
 
 
class AppStack(Stack):
    """
    Stack 3 – AppStack
 
    Contains Lambdas, triggers, and API entry points.
    Depends on DataStack (table + bucket) and MessagingStack (topics + queues).
    """
 
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
 
        table_t = data_stack.table_t
        external_ops_topic = messaging_stack.external_ops_topic
        system_ops_topic = messaging_stack.system_ops_topic
        external_ops_queue = messaging_stack.external_ops_queue
        system_ops_queue = messaging_stack.system_ops_queue

        # Matplotlib Lambda layer built from a local asset zip.
        # Place your matplotlib layer contents in ../matplotlib-layer.zip and CDK
        # will upload it as a Lambda layer in whatever region you deploy to.
        matplotlib_layer = _lambda.LayerVersion(
            self,
            "MatplotlibLayer",
            code=_lambda.Code.from_asset("../matplotlib-layer.zip"),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_11],
            description="Matplotlib dependency layer for NetWorthPlotterLambda",
        )

        # yfinance layer packaged locally as ../yfinance-layer.zip (for news fetcher).
        yfinance_layer = _lambda.LayerVersion(
            self,
            "YFinanceLayer",
            code=_lambda.Code.from_asset("../yfinance-layer.zip"),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_12],
            description="yfinance dependency layer for NewsFetcherLambda",
        )

        # Lambda code base path
        lambda_code_path = "lambda"

        # S3 bucket S for news and plots (application-owned)
        bucket_s = s3.Bucket(
            self,
            "NewsAndPlotsBucket",
            bucket_name=None,  # Let CDK generate, or set explicitly if you prefer
            versioned=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            removal_policy=_default_removal_policy(),
            auto_delete_objects=False,
        )

        # Lambda A – News Fetcher
        lambda_a = _lambda.Function(
            self,
            "NewsFetcherLambda",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="main.handler",
            code=_lambda.Code.from_asset(f"../lambda/news_fetcher"),
            timeout=Duration.seconds(900),
            layers=[yfinance_layer],
            environment={
                "TABLE_NAME": table_t.table_name,
                "SYSTEM_OPS_TOPIC_ARN": system_ops_topic.topic_arn,
                "BUCKET_NAME": bucket_s.bucket_name,
            },
        )

        # Allow A to query table and publish to system topic and/or write to S3 (if desired)
        table_t.grant_read_write_data(lambda_a)
        system_ops_topic.grant_publish(lambda_a)
        bucket_s.grant_write(lambda_a)

        # EventBridge rule: run A every 10 minutes
        events.Rule(
            self,
            "NewsFetcherScheduleRule",
            schedule=events.Schedule.rate(Duration.minutes(10)),
            targets=[targets.LambdaFunction(lambda_a)],
        )

        # Lambda F – News Writer (consumes Q2 NEW_NEWS messages)
        lambda_f = _lambda.Function(
            self,
            "NewsWriterLambda",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="main.handler",
            code=_lambda.Code.from_asset(f"../lambda/news_writer"),
            timeout=Duration.seconds(300),
            environment={
                "BUCKET_NAME": bucket_s.bucket_name,
                "SYSTEM_OPS_TOPIC_ARN": system_ops_topic.topic_arn,
            },
        )
        bucket_s.grant_write(lambda_f)
        system_ops_topic.grant_publish(lambda_f)

        lambda_f.add_event_source(
            lambda_event_sources.SqsEventSource(system_ops_queue)
        )

        # Lambda B – Sentiment Scoring (kept for compatibility; not wired to S3 when using external FinBERT)
        lambda_b = _lambda.Function(
            self,
            "SentimentScorerLambda",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="main.handler",
            code=_lambda.Code.from_asset(f"../lambda/sentiment_scorer"),
            timeout=Duration.seconds(300),
            memory_size=1024,
            environment={
                "TABLE_NAME": table_t.table_name,
                "SYSTEM_OPS_TOPIC_ARN": system_ops_topic.topic_arn,
            },
        )
        table_t.grant_read_write_data(lambda_b)
        system_ops_topic.grant_publish(lambda_b)
        bucket_s.grant_read(lambda_b)

        # Lambda E – Soft Delete / Cleanup (system ops on Q2)
        lambda_e = _lambda.Function(
            self,
            "SoftDeleteLambda",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="main.handler",
            code=_lambda.Code.from_asset(f"../lambda/soft_delete"),
            timeout=Duration.seconds(300),
            environment={
                "TABLE_NAME": table_t.table_name,
                "BUCKET_NAME": bucket_s.bucket_name,
            },
        )
        table_t.grant_read_write_data(lambda_e)
        bucket_s.grant_read_write(lambda_e)
        lambda_e.add_event_source(
            lambda_event_sources.SqsEventSource(system_ops_queue)
        )

        # Periodic news retention cleaner (ensures news are removed after 6 weeks,
        # even for deleted stocks).
        lambda_retention = _lambda.Function(
            self,
            "NewsRetentionCleanerLambda",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="main.handler",
            code=_lambda.Code.from_asset(f"../lambda/news_retention_cleaner"),
            timeout=Duration.seconds(300),
            environment={
                "TABLE_NAME": table_t.table_name,
                "BUCKET_NAME": bucket_s.bucket_name,
            },
        )
        table_t.grant_read_write_data(lambda_retention)
        bucket_s.grant_read_write(lambda_retention)

        events.Rule(
            self,
            "NewsRetentionCleanupRule",
            schedule=events.Schedule.rate(Duration.days(1)),
            targets=[targets.LambdaFunction(lambda_retention)],
        )

        # Lambda C – Net-Worth Plotter (behind API Gateway)
        lambda_c = _lambda.Function(
            self,
            "NetWorthPlotterLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="main.handler",
            code=_lambda.Code.from_asset(f"../lambda/networth_plotter"),
            timeout=Duration.seconds(900),
            memory_size=1024,
            layers=[matplotlib_layer],
            environment={
                "TABLE_NAME": table_t.table_name,
                "BUCKET_NAME": bucket_s.bucket_name,
            },
        )
        table_t.grant_read_data(lambda_c)
        bucket_s.grant_read_write(lambda_c)

        api = apigw.LambdaRestApi(
            self,
            "NetWorthApi",
            handler=lambda_c,
            proxy=False,
            rest_api_name="NetWorthService",
        )

        networth_resource = api.root.add_resource("networth")
        networth_resource.add_method("GET")  # expects query params in handler

        # Management API – control-plane operations via HTTP (no generic AWS backend).
        management_lambda = _lambda.Function(
            self,
            "ControlPlaneApiLambda",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="main.handler",
            code=_lambda.Code.from_asset(f"../lambda/control_plane_api"),
            timeout=Duration.seconds(300),
            environment={
                "TABLE_NAME": table_t.table_name,
                "EXTERNAL_OPS_TOPIC_ARN": external_ops_topic.topic_arn,
                "SYSTEM_OPS_TOPIC_ARN": system_ops_topic.topic_arn,
                "NEWS_FETCHER_NAME": lambda_a.function_name,
            },
        )
        table_t.grant_read_write_data(management_lambda)
        external_ops_topic.grant_publish(management_lambda)
        system_ops_topic.grant_publish(management_lambda)
        lambda_a.grant_invoke(management_lambda)

        apigw.LambdaRestApi(
            self,
            "ManagementApi",
            handler=management_lambda,
            proxy=True,
            rest_api_name="ManagementService",
        )

        # Net-worth snapshot writer (periodic writer of NETWORTH#... items)
        lambda_snapshot = _lambda.Function(
            self,
            "NetWorthSnapshotWriterLambda",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="main.handler",
            code=_lambda.Code.from_asset(
                f"../lambda/networth_snapshot_writer"
            ),
            timeout=Duration.seconds(300),
            environment={
                "TABLE_NAME": table_t.table_name,
            },
        )
        table_t.grant_read_write_data(lambda_snapshot)

        events.Rule(
            self,
            "NetWorthSnapshotScheduleRule",
            schedule=events.Schedule.rate(Duration.minutes(15)),
            targets=[targets.LambdaFunction(lambda_snapshot)],
        )
 
 
def _default_removal_policy() -> RemovalPolicy:
        """
        Helper to set dev-friendly removal policy.
        Adjust to RETAIN for production if desired.
        """

        return RemovalPolicy.DESTROY

