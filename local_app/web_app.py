from __future__ import annotations

import os
from typing import Any, Dict

import requests
from flask import Flask, redirect, render_template_string, request, url_for


MGMT_API_BASE_URL = os.environ.get("MGMT_API_BASE_URL", "").rstrip("/")
NETWORTH_API_BASE_URL = os.environ.get("NETWORTH_API_BASE_URL", "").rstrip("/")

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
      .section { max-width: 32rem; }
      .note { font-size:0.85rem; color:#9ca3af; margin-top:0.25rem; }
      .msg-ok { color:#4ade80; margin-top:0.5rem; white-space:pre-wrap; }
      .msg-err { color:#fca5a5; margin-top:0.5rem; white-space:pre-wrap; }
      code { background:#020617; padding:0.15rem 0.25rem; border-radius:0.25rem; }
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
          <input name="threshold_abs" type="number" value="3" />
        </label>
        <button type="submit">Update threshold</button>
      </form>
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
    </div>
  </body>
  </html>
"""


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
    return render_template_string(
        INDEX_TEMPLATE,
        mgmt=MGMT_API_BASE_URL,
        networth=NETWORTH_API_BASE_URL,
        message=message,
        error=error,
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
    except Exception as exc:
        msg = f"Error calling {url}: {exc}"
    return _redir(msg, error=not msg.startswith("2"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9090)





