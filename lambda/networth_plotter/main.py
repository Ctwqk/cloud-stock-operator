"""Lambda C - Net-Worth Plotter (API Gateway ``GET /networth``).

Query string: ``account_id`` (optional), ``start_time`` / ``end_time`` (ISO-8601, optional).
Renders the ``NETWORTH#`` series as a PNG into S3 (``plots/networth/<account>/<id>.png``) and
returns its key plus a presigned URL. Bad timestamps return ``400``.
"""

from __future__ import annotations

import io
import logging
import uuid
from typing import Any

from common import aws, ddb, jsonutil, keys, timeutil

logger = logging.getLogger(__name__)

PRESIGN_SECONDS = 3600
MAX_POINTS = 5000


def _response(status: int, body: dict[str, Any]) -> dict[str, Any]:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": jsonutil.dumps(body),
    }


def query_points(account_id: str, start: str, end: str) -> list[dict[str, Any]]:
    """Net-worth items with ``start <= timestamp <= end`` in chronological order."""
    items = ddb.query_all(
        aws.table(),
        KeyConditionExpression="PK = :pk AND SK BETWEEN :start AND :end",
        ExpressionAttributeValues={
            ":pk": keys.account_pk(account_id),
            ":start": keys.networth_sk(start),
            ":end": keys.networth_sk(end),
        },
    )
    return sorted(items, key=lambda item: item["SK"])[-MAX_POINTS:]


def render_png(points: list[dict[str, Any]]) -> bytes:
    import matplotlib

    matplotlib.use("Agg")  # no display in Lambda
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter

    fig, ax = plt.subplots(figsize=(8, 4), dpi=100)
    try:
        if not points:
            ax.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax.transAxes)
            ax.set_axis_off()
        else:
            times = [timeutil.parse_iso(p["SK"][len(keys.NETWORTH_PREFIX) :]) for p in points]
            values = [float(p.get("net_worth", 0)) for p in points]
            ax.plot(times, values, marker="o", markersize=3, linewidth=1.5)
            ax.set_xlabel("time (UTC)")
            ax.set_ylabel("net worth")
            ax.set_title("Managed net worth")
            ax.grid(alpha=0.3)
            ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:,.2f}"))
            locator = mdates.AutoDateLocator()
            ax.xaxis.set_major_locator(locator)
            ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))
        fig.tight_layout()
        buffer = io.BytesIO()
        fig.savefig(buffer, format="png")
        return buffer.getvalue()
    finally:
        plt.close(fig)


def handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    params = (event or {}).get("queryStringParameters") or {}
    account_id = params.get("account_id") or aws.get_account_id()

    start = timeutil.normalize_iso(params.get("start_time")) if params.get("start_time") else None
    end = timeutil.normalize_iso(params.get("end_time")) if params.get("end_time") else None
    if params.get("start_time") and start is None:
        return _response(400, {"error": "start_time must be an ISO-8601 timestamp"})
    if params.get("end_time") and end is None:
        return _response(400, {"error": "end_time must be an ISO-8601 timestamp"})
    start = start or "1970-01-01T00:00:00.000000Z"
    end = end or timeutil.iso_utc()
    if start > end:
        return _response(400, {"error": "start_time must not be after end_time"})

    points = query_points(account_id, start, end)
    png = render_png(points)

    bucket = aws.require_env("BUCKET_NAME")
    key = f"plots/networth/{account_id}/{uuid.uuid4()}.png"
    s3 = aws.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=png, ContentType="image/png")
    url = s3.generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=PRESIGN_SECONDS
    )
    return _response(
        200,
        {
            "account_id": account_id,
            "start_time": start,
            "end_time": end,
            "points": len(points),
            "s3_key": key,
            "url": url,
        },
    )
