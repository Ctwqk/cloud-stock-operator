"""A local stand-in for the AWS deployment.

The whole system - DynamoDB, S3, SNS, SQS - runs on `moto <https://docs.getmoto.org>`_'s server
(or in-process under ``mock_aws`` for tests). This module adds the parts of AWS that moto cannot
run for us:

* :func:`provision` creates the table, bucket, topics, queues, DLQs and SNS filter subscriptions
  from :mod:`common.topology` - the same description the CDK stack is built from.
* :class:`LocalCloud` plays *Lambda*: it polls each consumer queue and calls the real handler
  functions from ``lambda/`` with SQS-shaped events (honouring partial batch failures), runs the
  EventBridge schedules, and fronts the API Gateway-integrated functions with a small HTTP
  server that builds API-Gateway proxy events.

Run it standalone (this is what ``docker compose`` does)::

    python local_app/local_cloud.py            # moto on :5000, API on :8000

Everything else (web app, trade worker) talks to it through the normal AWS SDK by setting
``AWS_ENDPOINT_URL``.
"""

from __future__ import annotations

import argparse
import importlib
import json
import logging
import os
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import boto3
from botocore.exceptions import ClientError

from common import aws, timeutil, topology

logger = logging.getLogger("local_cloud")

TABLE_NAME = "TradingTable"
BUCKET_NAME = "cloud-stock-local"
REGION = "us-east-1"
STAGE = "prod"
LOCAL_RETRY_DELAY_S = (
    2  # failed messages become visible again quickly (real SQS: visibility timeout)
)


# --------------------------------------------------------------------------------------------
# Provisioning
# --------------------------------------------------------------------------------------------
@dataclass
class Resources:
    table_name: str = TABLE_NAME
    bucket_name: str = BUCKET_NAME
    external_topic_arn: str = ""
    system_topic_arn: str = ""
    queue_urls: dict[str, str] = field(default_factory=dict)
    dlq_urls: dict[str, str] = field(default_factory=dict)

    def environment(self) -> dict[str, str]:
        """Environment variables the Lambda handlers expect (mirrors the CDK stack)."""
        return {
            "TABLE_NAME": self.table_name,
            "BUCKET_NAME": self.bucket_name,
            "EXTERNAL_OPS_TOPIC_ARN": self.external_topic_arn,
            "SYSTEM_OPS_TOPIC_ARN": self.system_topic_arn,
            "NEWS_FETCHER_NAME": "news_fetcher",
        }


def _client(service: str) -> Any:
    return boto3.client(service, region_name=os.environ.get("AWS_DEFAULT_REGION", REGION))


def provision(table_name: str = TABLE_NAME, bucket_name: str = BUCKET_NAME) -> Resources:
    """Create (idempotently) every AWS resource the system needs. Returns their identifiers."""
    ddb, s3, sns, sqs = (_client(s) for s in ("dynamodb", "s3", "sns", "sqs"))
    res = Resources(table_name=table_name, bucket_name=bucket_name)

    try:
        ddb.create_table(
            TableName=table_name,
            AttributeDefinitions=[
                {"AttributeName": "PK", "AttributeType": "S"},
                {"AttributeName": "SK", "AttributeType": "S"},
            ],
            KeySchema=[
                {"AttributeName": "PK", "KeyType": "HASH"},
                {"AttributeName": "SK", "KeyType": "RANGE"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        ddb.update_time_to_live(
            TableName=table_name,
            TimeToLiveSpecification={"Enabled": True, "AttributeName": "ttl"},
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ResourceInUseException":
            raise

    try:
        s3.create_bucket(Bucket=bucket_name)
    except ClientError as exc:
        if exc.response["Error"]["Code"] not in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
            raise

    res.external_topic_arn = sns.create_topic(Name="ExternalOpsTopic")["TopicArn"]
    res.system_topic_arn = sns.create_topic(Name="SystemOpsTopic")["TopicArn"]
    topics = {
        topology.EXTERNAL_TOPIC: res.external_topic_arn,
        topology.SYSTEM_TOPIC: res.system_topic_arn,
    }

    for spec in topology.QUEUES:
        dlq_url = sqs.create_queue(QueueName=f"{spec.name}DLQ")["QueueUrl"]
        dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]
        queue_url = sqs.create_queue(
            QueueName=spec.name,
            Attributes={
                "VisibilityTimeout": str(spec.visibility_timeout_s),
                "RedrivePolicy": json.dumps(
                    {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": spec.max_receive_count}
                ),
            },
        )["QueueUrl"]
        queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]
        sns.subscribe(
            TopicArn=topics[spec.topic],
            Protocol="sqs",
            Endpoint=queue_arn,
            Attributes={
                "FilterPolicy": json.dumps({"type": list(spec.message_types)}),
                "FilterPolicyScope": "MessageAttributes",
            },
        )
        res.queue_urls[spec.name] = queue_url
        res.dlq_urls[spec.name] = dlq_url
    return res


def apply_environment(res: Resources) -> None:
    os.environ.update(res.environment())


# --------------------------------------------------------------------------------------------
# "Lambda" runtime
# --------------------------------------------------------------------------------------------
def load_handler(function: str) -> Callable[[dict[str, Any], Any], Any]:
    return importlib.import_module(f"{function}.main").handler


def scheduled_event(moment: str | None = None) -> dict[str, Any]:
    """Shape of an EventBridge scheduled-rule event."""
    return {
        "version": "0",
        "id": str(uuid.uuid4()),
        "detail-type": "Scheduled Event",
        "source": "aws.events",
        "account": "123456789012",
        "time": moment or timeutil.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "region": REGION,
        "resources": ["arn:aws:events:local:rule/local-schedule"],
        "detail": {},
    }


class _LocalLambdaClient:
    """Replaces boto3's ``lambda`` client so ``lambda.invoke`` runs the handler in-process."""

    def __init__(self, cloud: LocalCloud) -> None:
        self._cloud = cloud

    def invoke(
        self,
        FunctionName: str,
        InvocationType: str = "RequestResponse",
        Payload: bytes = b"{}",
        **_: Any,
    ) -> dict[str, Any]:
        event = json.loads(Payload or b"{}")
        if InvocationType == "Event":
            threading.Thread(
                target=self._cloud.invoke, args=(FunctionName, event), daemon=True
            ).start()
            return {"StatusCode": 202}
        result = self._cloud.invoke(FunctionName, event)
        return {"StatusCode": 200, "Payload": _Payload(json.dumps(result, default=str).encode())}


class _Payload:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data


class LocalCloud:
    def __init__(self, resources: Resources, *, install_lambda_client: bool = True) -> None:
        self.res = resources
        self._sqs = _client("sqs")
        self._handlers: dict[str, Callable[[dict[str, Any], Any], Any]] = {}
        self._stop = threading.Event()
        if install_lambda_client:
            aws.register_override("lambda", _LocalLambdaClient(self))

    # -- invocation -------------------------------------------------------------------------
    def handler(self, function: str) -> Callable[[dict[str, Any], Any], Any]:
        if function not in self._handlers:
            self._handlers[function] = load_handler(function)
        return self._handlers[function]

    def invoke(self, function: str, event: dict[str, Any]) -> Any:
        logger.info("invoke %s", function)
        return self.handler(function)(event, None)

    # -- SQS event source mapping -----------------------------------------------------------
    def poll_queue(self, spec: topology.QueueSpec, wait_seconds: int = 0) -> int:
        """Receive one batch for ``spec`` and run its Lambda. Returns messages received."""
        queue_url = self.res.queue_urls[spec.name]
        response = self._sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=spec.batch_size,
            WaitTimeSeconds=wait_seconds,
            MessageSystemAttributeNames=["All"],
            MessageAttributeNames=["All"],
        )
        received = response.get("Messages", [])
        if not received:
            return 0

        event = {"Records": [self._to_record(spec, m) for m in received]}
        try:
            result = self.handler(spec.consumer)(event, None) or {}
            failed = {f["itemIdentifier"] for f in result.get("batchItemFailures", [])}
        except Exception:
            logger.exception("%s raised; whole batch will be retried", spec.consumer)
            failed = {m["MessageId"] for m in received}

        for message in received:
            if message["MessageId"] in failed:
                self._sqs.change_message_visibility(
                    QueueUrl=queue_url,
                    ReceiptHandle=message["ReceiptHandle"],
                    VisibilityTimeout=LOCAL_RETRY_DELAY_S,
                )
            else:
                self._sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"])
        return len(received)

    def _to_record(self, spec: topology.QueueSpec, message: dict[str, Any]) -> dict[str, Any]:
        attrs = message.get("Attributes", {})
        return {
            "messageId": message["MessageId"],
            "receiptHandle": message["ReceiptHandle"],
            "body": message["Body"],
            "attributes": {
                "ApproximateReceiveCount": attrs.get("ApproximateReceiveCount", "1"),
                "SentTimestamp": attrs.get("SentTimestamp", str(int(time.time() * 1000))),
            },
            "messageAttributes": {},
            "md5OfBody": message.get("MD5OfBody", ""),
            "eventSource": "aws:sqs",
            "eventSourceARN": f"arn:aws:sqs:{REGION}:000000000000:{spec.name}",
            "awsRegion": REGION,
        }

    def lambda_queues(self) -> list[topology.QueueSpec]:
        return [q for q in topology.QUEUES if q.consumer != topology.LOCAL_WORKER]

    def drain(self, max_rounds: int = 200) -> int:
        """Process every Lambda-consumed queue until all are empty (deterministic, for tests)."""
        total = 0
        for _ in range(max_rounds):
            round_total = sum(self.poll_queue(spec) for spec in self.lambda_queues())
            total += round_total
            if round_total == 0:
                return total
        raise RuntimeError("queues did not drain; possible message loop")

    # -- long-running mode ------------------------------------------------------------------
    def run_forever(
        self,
        fetch_interval: float = 60,
        snapshot_interval: float = 60,
        retention_interval: float = 3600,
    ) -> None:
        threads = [
            threading.Thread(target=self._consume_loop, args=(spec,), daemon=True)
            for spec in self.lambda_queues()
        ]
        schedules = {
            "news_fetcher": fetch_interval,
            "networth_snapshot_writer": snapshot_interval,
            "news_retention_cleaner": retention_interval,
        }
        threads += [
            threading.Thread(target=self._schedule_loop, args=(fn, every), daemon=True)
            for fn, every in schedules.items()
        ]
        for thread in threads:
            thread.start()
        self._stop.wait()

    def stop(self) -> None:
        self._stop.set()

    def _consume_loop(self, spec: topology.QueueSpec) -> None:
        while not self._stop.is_set():
            try:
                if self.poll_queue(spec, wait_seconds=1) == 0:
                    time.sleep(0.2)
            except Exception:
                logger.exception("poller for %s crashed; retrying", spec.name)
                time.sleep(2)

    def _schedule_loop(self, function: str, every: float) -> None:
        while not self._stop.wait(every):
            try:
                self.invoke(function, scheduled_event())
            except Exception:
                logger.exception("scheduled run of %s failed", function)


# --------------------------------------------------------------------------------------------
# API Gateway stand-in
# --------------------------------------------------------------------------------------------
def create_api(cloud: LocalCloud, api_key: str | None = None) -> Any:
    """Flask app that turns HTTP requests into API Gateway proxy events.

    ``/mgmt/<path>``  -> ``control_plane_api``      (ManagementApi)
    ``/networth``     -> ``networth_plotter``       (NetWorthApi, GET /networth)
    ``/_local/invoke/<function>``  local-only helper used by the demo seeder
    """
    from flask import Flask, Response, jsonify, request

    app = Flask("local-api-gateway")

    def check_key() -> Response | None:
        if api_key and request.headers.get("x-api-key") != api_key:
            return jsonify({"message": "Forbidden"}), 403  # same body as API Gateway
        return None

    def proxy(function: str, path: str) -> Response:
        denied = check_key()
        if denied:
            return denied
        event = {
            "httpMethod": request.method,
            "path": path,
            "resource": "/{proxy+}",
            "headers": dict(request.headers),
            "queryStringParameters": request.args.to_dict() or None,
            "body": request.get_data(as_text=True) or None,
            "isBase64Encoded": False,
            "requestContext": {"stage": STAGE},
        }
        result = cloud.invoke(function, event)
        return Response(
            result["body"],
            status=result["statusCode"],
            headers=result.get("headers") or {},
        )

    @app.route("/mgmt/<path:path>", methods=["GET", "POST", "PUT", "DELETE"])
    def management(path: str) -> Response:
        return proxy("control_plane_api", "/" + path)

    @app.get("/networth")
    def networth() -> Response:
        return proxy("networth_plotter", "/networth")

    @app.post("/_local/invoke/<function>")
    def invoke(function: str) -> Response:
        if function not in topology.ALL_FUNCTIONS:
            return jsonify({"error": "unknown function"}), 404
        result = cloud.invoke(function, request.get_json(silent=True) or scheduled_event())
        return jsonify(result)

    @app.post("/_local/drain")
    def drain() -> Response:
        return jsonify({"processed": cloud.drain()})

    @app.get("/_local/health")
    def health() -> Response:
        return jsonify({"status": "ok", "table": cloud.res.table_name})

    return app


# --------------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------------
def configure_local_environment() -> None:
    for name, value in {
        "AWS_ACCESS_KEY_ID": "local",
        "AWS_SECRET_ACCESS_KEY": "local",
        "AWS_DEFAULT_REGION": REGION,
        "AWS_REGION": REGION,
    }.items():
        os.environ.setdefault(name, value)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--moto-port", type=int, default=int(os.environ.get("MOTO_PORT", "5000")))
    parser.add_argument("--api-port", type=int, default=int(os.environ.get("API_PORT", "8000")))
    parser.add_argument("--host", default=os.environ.get("BIND_HOST", "0.0.0.0"))
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    for noisy in ("werkzeug", "botocore", "boto3", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)  # one line per HTTP call otherwise
    configure_local_environment()

    if "AWS_ENDPOINT_URL" not in os.environ:
        from moto.server import ThreadedMotoServer

        server = ThreadedMotoServer(ip_address=args.host, port=args.moto_port)
        server.start()
        os.environ["AWS_ENDPOINT_URL"] = f"http://127.0.0.1:{args.moto_port}"
        logger.info("moto server listening on %s:%s", args.host, args.moto_port)

    res = provision()
    apply_environment(res)
    os.environ.setdefault("NEWS_PROVIDER", "fixture")
    os.environ.setdefault(
        "NEWS_FIXTURE_PATH", os.path.join(os.path.dirname(__file__), "fixtures", "market.json")
    )

    cloud = LocalCloud(res)
    api = create_api(cloud, api_key=os.environ.get("LOCAL_API_KEY") or None)
    threading.Thread(
        target=lambda: api.run(host=args.host, port=args.api_port, threaded=True), daemon=True
    ).start()
    logger.info("local API gateway on %s:%s (stage /%s)", args.host, args.api_port, STAGE)
    try:
        cloud.run_forever(
            fetch_interval=float(os.environ.get("FETCH_INTERVAL_S", "60")),
            snapshot_interval=float(os.environ.get("SNAPSHOT_INTERVAL_S", "60")),
        )
    except KeyboardInterrupt:
        cloud.stop()


if __name__ == "__main__":
    main()
