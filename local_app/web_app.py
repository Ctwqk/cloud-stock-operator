from __future__ import annotations

import os
from decimal import Decimal
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import boto3
import requests
from flask import Flask, redirect, render_template_string, request, url_for


MGMT_API_BASE_URL = os.environ.get("MGMT_API_BASE_URL", "").rstrip("/")
NETWORTH_API_BASE_URL = os.environ.get("NETWORTH_API_BASE_URL", "").rstrip("/")
TABLE_NAME = os.environ.get("TABLE_NAME", "TradingTable")

dynamodb = boto3.resource("dynamodb")
sts = boto3.client("sts")

_ACCOUNT_ID_CACHE: str | None = None


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
      .section { max-width: 42rem; }
      .note { font-size:0.85rem; color:#9ca3af; margin-top:0.25rem; }
      .msg-ok { color:#4ade80; margin-top:0.5rem; white-space:pre-wrap; }
      .msg-err { color:#fca5a5; margin-top:0.5rem; white-space:pre-wrap; }
      code { background:#020617; padding:0.15rem 0.25rem; border-radius:0.25rem; }
      table { border-collapse: collapse; width: 100%; margin-top: 1rem; font-size: 0.9rem; }
      th, td { border: 1px solid #1f2937; padding: 0.35rem 0.5rem; text-align: left; }
      th { background:#020617; }
      .pill { display:inline-block; padding:0.1rem 0.4rem; border-radius:999px; font-size:0.75rem; }
      .pill-ok { background:#065f46; color:#bbf7d0; }
      .pill-warn { background:#7c2d12; color:#fed7aa; }
      .img-frame { margin-top:1rem; padding:0.5rem; border-radius:0.5rem; border:1px solid #1f2937; background:#020617; }
    </style>
  </head>
  <body>
    <h1>Trading Control Panel</h1>
    <p class="note">
      Management API base: <code>{{ mgmt or "NOT SET" }}</code><br/>
      Networth API base: <code>{{ networth or "NOT SET" }}</code>
    </p>

    {% if message %}
      <div class="{{ 'msg-ok' if not error else 'msg-err' }}">{{ message }}</div>
    {% endif %}

    <div class="section">
      <h2>Watchlist</h2>

      <form method="post" action="{{ url_for('add_watchlist') }}">
        <strong>Add stock</strong>
        <label>Symbol
          <input name="symbol" required />
        </label>
        <label>Initial shares managed
          <input name="shares_managed" type="number" value="0" />
        </label>
        <button type="submit">Add to watchlist</button>
      </form>

      <form method="post" action="{{ url_for('delete_watchlist') }}">
        <strong>Delete / soft-delete stock</strong>
        <label>Symbol
          <input name="symbol" required />
        </label>
        <button type="submit">Delete</button>
        <p class="note">This calls DELETE /watchlist/{symbol} &amp; uses the soft-delete Lambda.</p>
      </form>

      <form method="post" action="{{ url_for('set_threshold') }}">
        <strong>Set per-stock threshold</strong>
        <label>Symbol
          <input name="symbol" required />
        </label>
        <label>Threshold (abs)
          <input name="threshold_abs" type="number" value="1" />
        </label>
        <button type="submit">Update threshold</button>
      </form>

      <form method="post" action="{{ url_for('refresh_watchlist') }}">
        <strong>Refresh prices &amp; news</strong>
        <p class="note">
          This triggers the NewsFetcher Lambda to update last prices and pull fresh news
          for all symbols in the watchlist.
        </p>
        <button type="submit">Refresh now</button>
      </form>
      {% if watchlist %}
        <table>
          <thead>
            <tr>
              <th>Symbol</th>
              <th>Managed shares</th>
              <th>Score</th>
              <th>Threshold</th>
              <th>Last price</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {% for s in watchlist %}
              <tr>
                <td>{{ s.symbol }}</td>
                <td>{{ s.shares_managed or 0 }}</td>
                <td>{{ s.current_level_score or 0 }}</td>
                <td>{{ s.threshold_abs or 1 }}</td>
                <td>
                  {% set price = s.last_fetched_price|default(None) %}
                  {{ "%.2f"|format(price) if price is not none else "-" }}
                </td>
                <td>
                  {% if s.is_deleted %}
                    <span class="pill pill-warn">deleted</span>
                  {% else %}
                    <span class="pill pill-ok">active</span>
                  {% endif %}
                </td>
              </tr>
            {% endfor %}
          </tbody>
        </table>
      {% else %}
        <p class="note">No watchlist entries yet.</p>
      {% endif %}
    </div>

    <div class="section">
      <h2>Account</h2>

      <form method="post" action="{{ url_for('adjust_cash') }}">
        <strong>Adjust managed cash</strong>
        <label>Δ cash
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
        <label>Δ shares
          <input name="delta_shares" type="number" value="1" />
        </label>
        <label>Max shares allowed
          <input name="max_shares_allowed" type="number" value="100" />
        </label>
        <button type="submit">Adjust shares</button>
      </form>

      {% if summary %}
        <table>
          <thead>
            <tr>
              <th>Metric</th>
              <th>Value</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>Managed money (cash)</td>
              <td>
                {% set cash = summary.current_cash_managed|default(0) %}
                {{ "%.2f"|format(cash) }}
              </td>
            </tr>
            <tr>
              <td>Managed net worth</td>
              <td>
                {% if 'current_net_worth' in summary and summary.current_net_worth is not none %}
                  {% set nw = summary.current_net_worth %}
                  {{ "%.2f"|format(nw) }}
                {% else %}
                  N/A
                {% endif %}
              </td>
            </tr>
          </tbody>
        </table>
      {% endif %}
    </div>

    <div class="section">
      <h2>Net-worth Plot</h2>

      <form method="post" action="{{ url_for('request_networth_plot') }}">
        <label>Start time (ISO)
          <input name="start_time" placeholder="2025-01-01T00:00:00Z" />
        </label>
        <label>End time (ISO)
          <input name="end_time" placeholder="leave empty for now()" />
        </label>
        <button type="submit">Generate plot</button>
        <p class="note">This calls GET /networth on the NetWorthApi. It returns the S3 key of the PNG.</p>
      </form>

      {% if plot_url %}
        <div class="img-frame">
          <img src="{{ plot_url }}" alt="Net-worth plot"
               style="max-width:100%; border-radius:0.5rem;" />
        </div>
      {% endif %}
    </div>
  </body>
  </html>
"""


def _get_account_id() -> str:
    global _ACCOUNT_ID_CACHE
    if _ACCOUNT_ID_CACHE is None:
        ident = sts.get_caller_identity()
        _ACCOUNT_ID_CACHE = ident["Account"]
    return _ACCOUNT_ID_CACHE


def _load_account_state() -> Tuple[Dict[str, Any] | None, List[Dict[str, Any]]]:
    """
    Read summary and watchlist entries for the current account directly from DynamoDB.
    """
    table = dynamodb.Table(TABLE_NAME)
    account_id = _get_account_id()
    pk = f"ACCOUNT#{account_id}"

    resp = table.query(
        KeyConditionExpression="PK = :pk",
        ExpressionAttributeValues={":pk": pk},
    )
    items = resp.get("Items", [])

    summary = None
    watchlist: List[Dict[str, Any]] = []
    for item in items:
        sk = item.get("SK", "")
        if sk == "SUMMARY":
            summary = item
        elif sk.startswith("STOCK#"):
            if item.get("is_deleted"):
                # still show but flagged; template handles styling
                watchlist.append(item)
            else:
                watchlist.append(item)

    # If there is no SUMMARY row yet, synthesize a default so the UI
    # can still display "Managed money" as 0.
    if summary is None:
        summary = {
            "current_cash_managed": 0,
            "current_net_worth": None,
        }

    # Sort watchlist by symbol
    watchlist.sort(key=lambda x: x.get("symbol", ""))
    return summary, watchlist


def _get_ib() -> Optional["IB"]:
    """
    IBKR is no longer used for trading or position checks in this app.
    This helper is kept only for backwards compatibility; it always returns None.
    """
    return None


def _get_managed_cash(account_id: str) -> float:
    table = dynamodb.Table(TABLE_NAME)
    resp = table.get_item(Key={"PK": f"ACCOUNT#{account_id}", "SK": "SUMMARY"})
    item = resp.get("Item") or {}
    val = item.get("current_cash_managed", 0)
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


def _update_managed_cash(account_id: str, new_cash: float) -> None:
    table = dynamodb.Table(TABLE_NAME)
    table.update_item(
        Key={"PK": f"ACCOUNT#{account_id}", "SK": "SUMMARY"},
        UpdateExpression="SET current_cash_managed = :c, updated_at = :u",
        ExpressionAttributeValues={
            ":c": Decimal(str(new_cash)),
            ":u": datetime.now(timezone.utc).isoformat(),
        },
    )


def _get_last_fetched_price(account_id: str, symbol: str) -> Optional[float]:
    table = dynamodb.Table(TABLE_NAME)
    resp = table.get_item(
        Key={"PK": f"ACCOUNT#{account_id}", "SK": f"STOCK#{symbol}"}
    )
    item = resp.get("Item") or {}
    val = item.get("last_fetched_price")
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _get_managed_shares(account_id: str, symbol: str) -> int:
    table = dynamodb.Table(TABLE_NAME)
    resp = table.get_item(
        Key={"PK": f"ACCOUNT#{account_id}", "SK": f"STOCK#{symbol}"}
    )
    item = resp.get("Item") or {}
    try:
        return int(item.get("shares_managed", 0))
    except (TypeError, ValueError):
        return 0


def _get_ib_position_shares(symbol: str) -> Optional[int]:
    """
    IBKR positions are no longer consulted; trading is fully virtual.
    """
    return None


def _ensure_ib_shares_for_delta(symbol: str, delta_shares: int) -> int:
    """
    Ensure IBKR has enough shares for the requested managed-share delta.

    If increasing managed shares and IBKR has fewer shares than the target,
    try to buy ONE additional share using managed cash (if price and cash
    allow). Returns the max_shares_allowed value that should be passed to
    the management API.
    """
    account_id = _get_account_id()
    symbol = symbol.upper()

    # With pseudo-trading, we no longer consult IBKR positions or auto-buy.
    # Just return the current managed shares to allow the API to enforce any
    # additional limits (if provided).
    account_id = _get_account_id()
    symbol = symbol.upper()
    return _get_managed_shares(account_id, symbol)


def _call_mgmt_api(method: str, path: str, json_body: Dict[str, Any] | None = None) -> str:
    if not MGMT_API_BASE_URL:
        return "MGMT_API_BASE_URL is not set"

    url = f"{MGMT_API_BASE_URL}{path}"
    try:
        resp = requests.request(method, url, json=json_body)
        return f"{resp.status_code}: {resp.text}"
    except Exception as exc:  # best-effort
        return f"Error calling {url}: {exc}"


@app.route("/")
def index() -> str:
    message = request.args.get("m") or ""
    error = request.args.get("e") == "1"
    plot_url = request.args.get("plot_url") or ""

    summary = None
    watchlist: List[Dict[str, Any]] = []
    try:
        summary, watchlist = _load_account_state()
    except Exception as exc:  # noqa: BLE001
        # Best-effort; include error inline with any API message.
        extra = f" (watchlist load error: {exc})"
        message = f"{message}{extra}" if message else extra

    return render_template_string(
        INDEX_TEMPLATE,
        mgmt=MGMT_API_BASE_URL,
        networth=NETWORTH_API_BASE_URL,
        message=message,
        error=error,
        summary=summary,
        watchlist=watchlist,
        plot_url=plot_url,
    )


def _redir(msg: str, error: bool = False):
    return redirect(url_for("index", m=msg, e="1" if error else "0"))


@app.post("/watchlist/add")
def add_watchlist():
    symbol = request.form.get("symbol", "").upper()
    shares_managed = request.form.get("shares_managed", "0")
    body = {
        "symbol": symbol,
        "shares_managed": int(shares_managed or "0"),
    }
    msg = _call_mgmt_api("POST", "/watchlist", body)
    return _redir(msg, error=not msg.startswith("2"))


@app.post("/watchlist/delete")
def delete_watchlist():
    symbol = request.form.get("symbol", "").upper()
    msg = _call_mgmt_api("DELETE", f"/watchlist/{symbol}")
    return _redir(msg, error=not msg.startswith("2"))


@app.post("/watchlist/threshold")
def set_threshold():
    symbol = request.form.get("symbol", "").upper()
    threshold_abs = int(request.form.get("threshold_abs", "3") or "3")
    body = {"threshold_abs": threshold_abs}
    msg = _call_mgmt_api("POST", f"/watchlist/{symbol}/threshold", body)
    return _redir(msg, error=not msg.startswith("2"))


@app.post("/watchlist/refresh")
def refresh_watchlist():
    msg = _call_mgmt_api("POST", "/watchlist/refresh", {})
    return _redir(msg, error=not msg.startswith("2"))


@app.post("/account/cash/adjust")
def adjust_cash():
    delta_cash = float(request.form.get("delta_cash", "0") or "0")
    max_allowed_cash = float(request.form.get("max_allowed_cash", "0") or "0")
    body = {
        "delta_cash": delta_cash,
        "max_allowed_cash": max_allowed_cash,
    }
    msg = _call_mgmt_api("POST", "/account/cash/adjust", body)
    return _redir(msg, error=not msg.startswith("2"))


@app.post("/account/shares/adjust")
def adjust_shares():
    symbol = request.form.get("symbol", "").upper()
    delta_shares = int(request.form.get("delta_shares", "0") or "0")
    max_shares_allowed = int(request.form.get("max_shares_allowed", "0") or "0")

    # With pseudo-trading, we don't talk to IBKR here. Optionally clamp by
    # current managed shares to avoid accidental huge jumps if max_shares_allowed
    # is left at 0.
    try:
        current_managed = _get_managed_shares(_get_account_id(), symbol)
        if max_shares_allowed <= 0:
            max_shares_allowed = current_managed + max(delta_shares, 0)
    except Exception as exc:  # noqa: BLE001
        print(f"[web_app] adjust_shares: pre-check failed: {exc}")

    body = {
        "symbol": symbol,
        "delta_shares": delta_shares,
        "max_shares_allowed": max_shares_allowed,
    }
    msg = _call_mgmt_api("POST", "/account/shares/adjust", body)
    return _redir(msg, error=not msg.startswith("2"))


@app.post("/networth/plot")
def request_networth_plot():
    if not NETWORTH_API_BASE_URL:
        return _redir("NETWORTH_API_BASE_URL is not set", error=True)

    start_time = request.form.get("start_time") or ""
    end_time = request.form.get("end_time") or ""

    params: Dict[str, Any] = {}
    if start_time:
        params["start_time"] = start_time
    if end_time:
        params["end_time"] = end_time

    url = f"{NETWORTH_API_BASE_URL}/networth"
    try:
        resp = requests.get(url, params=params)
        msg = f"{resp.status_code}: {resp.text}"
        plot_url = ""
        if resp.status_code == 200:
            try:
                data = resp.json()
                plot_url = data.get("url", "") or ""
            except Exception:
                plot_url = ""
    except Exception as exc:
        msg = f"Error calling {url}: {exc}"
        return _redir(msg, error=True)

    # Redirect back to index, showing the message and embedding the plot if we have a URL.
    return redirect(url_for("index", m=msg, e="1" if not msg.startswith("2") else "0", plot_url=plot_url))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9090)





