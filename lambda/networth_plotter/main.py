import io
import json
import os
import uuid
from datetime import datetime
from typing import Any, Dict, List

import boto3
import matplotlib.pyplot as plt


TABLE_NAME = os.environ.get("TABLE_NAME", "")
BUCKET_NAME = os.environ.get("BUCKET_NAME", "")

dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")
sts = boto3.client("sts")

_ACCOUNT_ID_CACHE: str | None = None


def _get_account_id() -> str:
    """
    Resolve AWS account id from STS once per container.
    """
    global _ACCOUNT_ID_CACHE
    if _ACCOUNT_ID_CACHE is None:
        identity = sts.get_caller_identity()
        _ACCOUNT_ID_CACHE = identity["Account"]
    return _ACCOUNT_ID_CACHE


def _query_networth_points(account_id: str, start_time: str, end_time: str) -> List[Dict[str, Any]]:
    """
    Query net-worth time-series:
      PK = ACCOUNT#<account_id>
      SK between NETWORTH#start_time and NETWORTH#end_time
    """
    table = dynamodb.Table(TABLE_NAME)
    sk_start = f"NETWORTH#{start_time}"
    sk_end = f"NETWORTH#{end_time}"
    resp = table.query(
        KeyConditionExpression="PK = :pk AND SK BETWEEN :start AND :end",
        ExpressionAttributeValues={
            ":pk": f"ACCOUNT#{account_id}",
            ":start": sk_start,
            ":end": sk_end,
        },
    )
    return sorted(resp.get("Items", []), key=lambda x: x.get("SK", ""))


def _make_plot(points: List[Dict[str, Any]]) -> bytes:
    """
    Generate a simple time-series PNG plot from points.
    """
    if not points:
        fig, ax = plt.subplots()
        ax.text(0.5, 0.5, "No data", ha="center", va="center")
    else:
        times = [p["SK"].split("NETWORTH#")[1] for p in points]
        net_worths = [float(p.get("net_worth", 0.0)) for p in points]

        fig, ax = plt.subplots()
        ax.plot(times, net_worths, marker="o")
        ax.set_xlabel("time")
        ax.set_ylabel("net worth")
        ax.set_title("Net Worth Over Time")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda C – Net-Worth Plotter.

    Triggered by API Gateway (proxy=False):
      - expects queryStringParameters: account_id, start_time, end_time.
    """
    params = (event or {}).get("queryStringParameters") or {}
    account_id = params.get("account_id") or _get_account_id()
    start_time = params.get("start_time", "1970-01-01T00:00:00Z")
    end_time = params.get("end_time", datetime.utcnow().isoformat() + "Z")

    points = _query_networth_points(account_id, start_time, end_time)
    png_bytes = _make_plot(points)

    request_id = str(uuid.uuid4())
    key = f"plots/networth/{account_id}/{request_id}.png"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=png_bytes,
        ContentType="image/png",
    )

    # Generate a presigned URL so the local web app can display the plot directly.
    presigned_url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": BUCKET_NAME, "Key": key},
        ExpiresIn=3600,
    )

    body = {
        "account_id": account_id,
        "start_time": start_time,
        "end_time": end_time,
        "s3_key": key,
        "url": presigned_url,
    }
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }



