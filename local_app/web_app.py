"""Trading control panel (Flask).

A thin UI over the deployed (or locally emulated) system:

* **writes** go through the Management API (``MGMT_API_BASE_URL``); balance-changing commands are
  asynchronous, so the app polls ``GET /operations/{op_id}`` and shows the real outcome;
* **reads** (watchlist, summary, recent trades) query DynamoDB directly;
* the net-worth PNG is rendered by the plotter Lambda (``NETWORTH_API_BASE_URL``) and proxied
  from S3 by this app, so the browser never needs to reach S3 itself.

Environment::

    MGMT_API_BASE_URL       e.g. https://abc.execute-api.us-west-1.amazonaws.com/prod  (no trailing /)
    NETWORTH_API_BASE_URL   base URL of the net-worth API
    MGMT_API_KEY            API Gateway API key (sent as x-api-key), if the APIs require one
    TABLE_NAME, BUCKET_NAME DynamoDB table / S3 bucket (defaults: TradingTable / cloud-stock-local)
    AWS_ENDPOINT_URL        point boto3 at the local emulator
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Any

import requests
from botocore.exceptions import ClientError
from flask import Flask, Response, abort, redirect, render_template_string, request, url_for

from common import aws, ddb, keys

logger = logging.getLogger("web_app")

REQUEST_TIMEOUT_S = 15
OPERATION_POLL_S = 8  # how long to wait for an asynchronous command to be applied


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


app = Flask(__name__)

INDEX_TEMPLATE = """
<!DOCTYPE html>
<html>
  <head>
    <meta charset="utf-8" />
    <title>Trading Control Panel</title>
    <style>
      body { font-family: system-ui, sans-serif; margin: 2rem; background:#0f172a; color:#e5e7eb; }
      h1 { margin-bottom: 0.5rem; }
      h2 { margin-top: 2rem; }
      form { margin-bottom: 1.5rem; padding:1rem; border-radius:0.5rem; background:#111827; border:1px solid #1f2937; }
      label { display:block; margin-top:0.5rem; }
      input { padding:0.35rem 0.5rem; margin-top:0.25rem; width: 16rem; border-radius:0.25rem; border:1px solid #374151; background:#020617; color:#e5e7eb; }
      button { margin-top:0.75rem; padding:0.35rem 0.9rem; border-radius:0.375rem; border:none; background:#2563eb; color:white; cursor:pointer; }
      button:hover { background:#1d4ed8; }
      .layout { display:flex; flex-wrap:wrap; gap:2rem; align-items:flex-start; }
      .section { max-width: 42rem; flex: 1 1 28rem; }
      .section.wide { max-width: 56rem; flex-basis: 100%; }
      .note { font-size:0.85rem; color:#9ca3af; margin-top:0.25rem; }
      .msg-ok { color:#4ade80; margin:0.75rem 0; white-space:pre-wrap; }
      .msg-err { color:#fca5a5; margin:0.75rem 0; white-space:pre-wrap; }
      code { background:#020617; padding:0.15rem 0.25rem; border-radius:0.25rem; }
      table { border-collapse: collapse; width: 100%; margin-top: 1rem; font-size: 0.9rem; }
      th, td { border: 1px solid #1f2937; padding: 0.35rem 0.5rem; text-align: left; }
      th { background:#020617; }
      .num { text-align: right; font-variant-numeric: tabular-nums; }
      .pill { display:inline-block; padding:0.1rem 0.4rem; border-radius:999px; font-size:0.75rem; }
      .pill-ok { background:#065f46; color:#bbf7d0; }
      .pill-warn { background:#7c2d12; color:#fed7aa; }
      .pos { color:#4ade80; } .neg { color:#fca5a5; }
      .img-frame { margin-top:1rem; padding:0.5rem; border-radius:0.5rem; border:1px solid #1f2937; background:#fff; }
    </style>
  </head>
  <body>
    <h1>Trading Control Panel</h1>
    <p class="note">
      Management API: <code>{{ mgmt or "NOT SET" }}</code><br/>
      Net-worth API: <code>{{ networth or "NOT SET" }}</code>
    </p>

    {% if message %}
      <div class="{{ 'msg-err' if error else 'msg-ok' }}">{{ message }}</div>
    {% endif %}

    <div class="layout">
    <div class="section">
      <h2>Manage watchlist</h2>

      <form method="post" action="{{ url_for('add_watchlist') }}">
        <strong>Add stock</strong>
        <label>Symbol
          <input name="symbol" required />
        </label>
        <label>Initial shares managed
          <input name="shares_managed" type="number" min="0" value="0" />
        </label>
        <button type="submit">Add to watchlist</button>
      </form>

      <form method="post" action="{{ url_for('delete_watchlist') }}">
        <strong>Delete / soft-delete stock</strong>
        <label>Symbol
          <input name="symbol" required />
        </label>
        <button type="submit">Delete</button>
        <p class="note">Calls <code>DELETE /watchlist/{symbol}</code>; the soft-delete Lambda cleans up.</p>
      </form>

      <form method="post" action="{{ url_for('set_threshold') }}">
        <strong>Set per-stock threshold</strong>
        <label>Symbol
          <input name="symbol" required />
        </label>
        <label>Threshold (abs)
          <input name="threshold_abs" type="number" min="1" value="1" />
        </label>
        <button type="submit">Update threshold</button>
      </form>

      <form method="post" action="{{ url_for('refresh_watchlist') }}">
        <strong>Refresh prices &amp; news</strong>
        <p class="note">
          Triggers the NewsFetcher Lambda for every symbol on the watchlist.
        </p>
        <button type="submit">Refresh now</button>
      </form>
    </div>

    <div class="section">
      <h2>Account</h2>

      <form method="post" action="{{ url_for('adjust_cash') }}">
        <strong>Adjust managed cash</strong>
        <label>Delta cash
          <input name="delta_cash" type="number" step="0.01" value="1000" />
        </label>
        <label>Max allowed cash
          <input name="max_allowed_cash" type="number" step="0.01" value="15000" />
        </label>
        <button type="submit">Adjust cash</button>
      </form>

      <form method="post" action="{{ url_for('adjust_shares') }}">
        <strong>Adjust managed shares</strong>
        <label>Symbol
          <input name="symbol" required />
        </label>
        <label>Delta shares
          <input name="delta_shares" type="number" value="1" />
        </label>
        <label>Max shares allowed
          <input name="max_shares_allowed" type="number" value="100" />
        </label>
        <button type="submit">Adjust shares</button>
      </form>
    </div>

    <div class="section wide">
      <h2>Overview</h2>
      <table>
        <thead><tr><th>Metric</th><th class="num">Value</th></tr></thead>
        <tbody>
          <tr><td>Managed cash</td><td class="num">{{ money(summary.get('current_cash_managed')) }}</td></tr>
          <tr><td>Positions value</td><td class="num">{{ money(summary.get('positions_value')) }}</td></tr>
          <tr><td>Managed net worth</td><td class="num">{{ money(summary.get('current_net_worth')) }}</td></tr>
        </tbody>
      </table>

      <h2>Watchlist</h2>
      {% if watchlist %}
        <table>
          <thead>
            <tr>
              <th>Symbol</th><th class="num">Managed shares</th><th class="num">Score</th>
              <th class="num">Threshold</th><th class="num">Last price</th>
              <th>Latest headline</th><th>Status</th>
            </tr>
          </thead>
          <tbody>
            {% for s in watchlist %}
              <tr>
                <td><strong>{{ s.symbol }}</strong></td>
                <td class="num">{{ s.shares_managed or 0 }}</td>
                <td class="num">{{ s.current_level_score or 0 }}</td>
                <td class="num">{{ s.threshold_abs or 1 }}</td>
                <td class="num">{{ money(s.get('last_fetched_price')) }}</td>
                <td>
                  {% if s.get('last_headline') %}
                    <span class="{{ 'pos' if s.get('last_sentiment', 0) > 0 else ('neg' if s.get('last_sentiment', 0) < 0 else '') }}">
                      {{ '+' if s.get('last_sentiment', 0) > 0 else '' }}{{ s.get('last_sentiment', 0) }}
                    </span>
                    {{ s.last_headline }}
                  {% else %}-{% endif %}
                </td>
                <td>
                  {% if s.is_deleted %}<span class="pill pill-warn">deleted</span>
                  {% else %}<span class="pill pill-ok">active</span>{% endif %}
                </td>
              </tr>
            {% endfor %}
          </tbody>
        </table>
      {% else %}
        <p class="note">No watchlist entries yet.</p>
      {% endif %}

      <h2>Recent virtual trades</h2>
      {% if trades %}
        <table>
          <thead>
            <tr><th>Time (UTC)</th><th>Side</th><th>Symbol</th><th class="num">Qty</th>
                <th class="num">Price</th><th>Status</th><th class="num">Score</th></tr>
          </thead>
          <tbody>
            {% for t in trades %}
              <tr>
                <td>{{ t.created_at[:19] }}</td>
                <td class="{{ 'pos' if t.side == 'BUY' else 'neg' }}">{{ t.side }}</td>
                <td>{{ t.symbol }}</td>
                <td class="num">{{ t.quantity }}</td>
                <td class="num">{{ money(t.get('last_price')) }}</td>
                <td>
                  <span class="pill {{ 'pill-ok' if t.status == 'EXECUTED_VIRTUAL' else 'pill-warn' }}">
                    {{ t.status.replace('_', ' ').lower() }}
                  </span>
                </td>
                <td class="num">{{ t.get('score', '-') }}</td>
              </tr>
            {% endfor %}
          </tbody>
        </table>
      {% else %}
        <p class="note">No trades yet. A trade happens when a stock's sentiment score reaches its threshold.</p>
      {% endif %}
    </div>

    <div class="section wide">
      <h2>Net-worth plot</h2>
      <form method="post" action="{{ url_for('request_networth_plot') }}">
        <label>Start time (ISO)
          <input name="start_time" placeholder="2025-01-01T00:00:00Z" />
        </label>
        <label>End time (ISO)
          <input name="end_time" placeholder="leave empty for now" />
        </label>
        <button type="submit">Generate plot</button>
        <p class="note">Calls <code>GET /networth</code>; the PNG is rendered by Lambda C and stored in S3.</p>
      </form>
      {% if plot_key %}
        <div class="img-frame">
          <img src="{{ url_for('plot', key=plot_key) }}" alt="Net-worth plot" style="max-width:100%;" />
        </div>
      {% endif %}
    </div>
    </div>
  </body>
</html>
"""


# --------------------------------------------------------------------------------------------
# Data access
# --------------------------------------------------------------------------------------------
def money(value: Any) -> str:
    try:
        return f"{float(value):,.2f}"
    except (TypeError, ValueError):
        return "-"


def load_account_state(
    limit_trades: int = 10,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    """Summary, watchlist and the most recent trades of the current account."""
    table = aws.table()
    pk = keys.account_pk(aws.get_account_id())

    summary = table.get_item(Key={"PK": pk, "SK": keys.SUMMARY_SK}).get("Item") or {}
    watchlist = ddb.query_all(
        table,
        KeyConditionExpression="PK = :pk AND begins_with(SK, :prefix)",
        ExpressionAttributeValues={":pk": pk, ":prefix": keys.STOCK_PREFIX},
    )
    watchlist.sort(key=lambda item: item.get("symbol", ""))
    trades = table.query(
        KeyConditionExpression="PK = :pk AND begins_with(SK, :prefix)",
        ExpressionAttributeValues={":pk": pk, ":prefix": keys.TRADE_PREFIX},
        ScanIndexForward=False,
        Limit=limit_trades,
    ).get("Items", [])
    return summary, watchlist, trades


# --------------------------------------------------------------------------------------------
# API client
# --------------------------------------------------------------------------------------------
@dataclass
class ApiResult:
    status: int
    data: dict[str, Any]
    ok: bool
    message: str


def _headers() -> dict[str, str]:
    key = _env("MGMT_API_KEY")
    return {"x-api-key": key} if key else {}


def _mgmt_base() -> str:
    return _env("MGMT_API_BASE_URL").rstrip("/")


def call_api(
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
    base: str | None = None,
    params: dict[str, str] | None = None,
) -> ApiResult:
    base = (base if base is not None else _mgmt_base()).rstrip("/")
    if not base:
        return ApiResult(0, {}, False, "API base URL is not configured")
    url = f"{base}{path}"
    try:
        response = requests.request(
            method, url, json=body, params=params, headers=_headers(), timeout=REQUEST_TIMEOUT_S
        )
    except requests.RequestException as exc:
        return ApiResult(0, {}, False, f"Error calling {url}: {exc}")
    try:
        data = response.json()
    except ValueError:
        data = {"raw": response.text}
    ok = 200 <= response.status_code < 300
    detail = data.get("error") or data.get("message") or data.get("raw") or ""
    return ApiResult(response.status_code, data, ok, f"{response.status_code}: {detail}")


def await_operation(op_id: str, timeout_s: float | None = None) -> ApiResult:
    """Poll ``GET /operations/{id}`` until the asynchronous command was applied or rejected."""
    deadline = time.monotonic() + (OPERATION_POLL_S if timeout_s is None else timeout_s)
    result = call_api("GET", f"/operations/{op_id}")
    while result.ok and result.data.get("status") == "PENDING" and time.monotonic() < deadline:
        time.sleep(0.5)
        result = call_api("GET", f"/operations/{op_id}")
    return result


def run_command(label: str, method: str, path: str, body: dict[str, Any] | None = None) -> Response:
    """Send a command, wait for asynchronous ones to finish and redirect with the outcome."""
    result = call_api(method, path, body)
    if not result.ok:
        return _redir(f"{label}: {result.message}", error=True)
    op_id = result.data.get("op_id")
    if not op_id:
        return _redir(f"{label}: {result.message}")
    outcome = await_operation(op_id)
    status = outcome.data.get("status", "UNKNOWN")
    if status == "APPLIED":
        return _redir(f"{label}: applied")
    if status == "REJECTED":
        return _redir(
            f"{label}: rejected - {outcome.data.get('reason', 'no reason given')}", error=True
        )
    return _redir(f"{label}: accepted, still processing (operation {op_id})")


def _redir(message: str, error: bool = False, **extra: str) -> Response:
    return redirect(url_for("index", m=message, e="1" if error else "0", **extra))


def _form_int(name: str, default: int = 0) -> int:
    raw = (request.form.get(name) or "").strip()
    return int(raw) if raw else default


def _form_float(name: str, default: float = 0.0) -> float:
    raw = (request.form.get(name) or "").strip()
    return float(raw) if raw else default


# --------------------------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------------------------
@app.get("/")
def index() -> str:
    message = request.args.get("m") or ""
    error = request.args.get("e") == "1"
    summary: dict[str, Any] = {}
    watchlist: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    try:
        summary, watchlist, trades = load_account_state()
    except Exception as exc:
        logger.exception("could not load account state")
        message = f"{message} (could not load data from DynamoDB: {exc})".strip()
        error = True
    return render_template_string(
        INDEX_TEMPLATE,
        mgmt=_mgmt_base(),
        networth=_env("NETWORTH_API_BASE_URL"),
        message=message,
        error=error,
        summary=summary,
        watchlist=watchlist,
        trades=trades,
        plot_key=request.args.get("plot_key", ""),
        money=money,
    )


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/watchlist/add")
def add_watchlist() -> Response:
    try:
        body = {
            "symbol": request.form.get("symbol", "").strip().upper(),
            "shares_managed": _form_int("shares_managed"),
        }
    except ValueError:
        return _redir("shares_managed must be a whole number", error=True)
    return run_command(f"Add {body['symbol']}", "POST", "/watchlist", body)


@app.post("/watchlist/delete")
def delete_watchlist() -> Response:
    symbol = request.form.get("symbol", "").strip().upper()
    result = call_api("DELETE", f"/watchlist/{symbol}")
    return _redir(f"Delete {symbol}: {result.message}", error=not result.ok)


@app.post("/watchlist/threshold")
def set_threshold() -> Response:
    symbol = request.form.get("symbol", "").strip().upper()
    try:
        body = {"threshold_abs": _form_int("threshold_abs", 1)}
    except ValueError:
        return _redir("threshold must be a whole number", error=True)
    result = call_api("POST", f"/watchlist/{symbol}/threshold", body)
    return _redir(f"Threshold {symbol}: {result.message}", error=not result.ok)


@app.post("/watchlist/refresh")
def refresh_watchlist() -> Response:
    result = call_api("POST", "/watchlist/refresh", {})
    return _redir(f"Refresh: {result.message}", error=not result.ok)


@app.post("/account/cash/adjust")
def adjust_cash() -> Response:
    try:
        body = {
            "delta_cash": _form_float("delta_cash"),
            "max_allowed_cash": _form_float("max_allowed_cash"),
        }
    except ValueError:
        return _redir("cash values must be numbers", error=True)
    return run_command("Adjust cash", "POST", "/account/cash/adjust", body)


@app.post("/account/shares/adjust")
def adjust_shares() -> Response:
    symbol = request.form.get("symbol", "").strip().upper()
    try:
        body = {
            "symbol": symbol,
            "delta_shares": _form_int("delta_shares"),
            "max_shares_allowed": _form_int("max_shares_allowed"),
        }
    except ValueError:
        return _redir("share values must be whole numbers", error=True)
    return run_command(f"Adjust shares {symbol}", "POST", "/account/shares/adjust", body)


@app.post("/networth/plot")
def request_networth_plot() -> Response:
    base = _env("NETWORTH_API_BASE_URL")
    if not base:
        return _redir("NETWORTH_API_BASE_URL is not set", error=True)
    params = {k: v for k in ("start_time", "end_time") if (v := request.form.get(k, "").strip())}
    result = call_api("GET", "/networth", base=base, params=params)
    if not result.ok:
        return _redir(f"Net-worth plot: {result.message}", error=True)
    points = result.data.get("points", 0)
    return _redir(
        f"Net-worth plot rendered from {points} snapshot(s)", plot_key=result.data.get("s3_key", "")
    )


@app.get("/plot")
def plot() -> Response:
    """Serve a rendered plot from S3 (only keys under ``plots/networth/``)."""
    key = request.args.get("key", "")
    if not key.startswith("plots/networth/") or ".." in key:
        abort(400)
    try:
        obj = aws.client("s3").get_object(Bucket=_env("BUCKET_NAME", "cloud-stock-local"), Key=key)
    except ClientError:
        abort(404)
    return Response(obj["Body"].read(), mimetype="image/png")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app.run(host=_env("BIND_HOST", "0.0.0.0"), port=int(_env("PORT", "9090")))
