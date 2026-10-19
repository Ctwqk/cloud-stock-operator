"""Seed the local demo through the public API (the same calls the web UI makes).

Adds managed cash and a few stocks, waits for every asynchronous command to be applied and
then exits. Safe to re-run: stocks that exist are kept, cash adjustments are bounded by
``max_allowed_cash``.

    python local_app/demo_seed.py            # uses MGMT_API_BASE_URL / MGMT_API_KEY
"""

from __future__ import annotations

import os
import sys
import time

import requests

BASE = os.environ.get("MGMT_API_BASE_URL", "http://127.0.0.1:8000/mgmt").rstrip("/")
HEADERS = {"x-api-key": os.environ["MGMT_API_KEY"]} if os.environ.get("MGMT_API_KEY") else {}

STOCKS = [("AAPL", 5), ("MSFT", 0), ("TSLA", 2), ("NVDA", 0), ("AMZN", 0)]
STARTING_CASH = 10_000
MAX_CASH = 50_000


def call(method: str, path: str, body: dict | None = None) -> dict:
    response = requests.request(method, BASE + path, json=body, headers=HEADERS, timeout=15)
    data = response.json()
    if response.status_code >= 400:
        sys.exit(f"{method} {path} -> {response.status_code}: {data}")
    return data


def wait_for(op_id: str, what: str, timeout: float = 30) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        status = call("GET", f"/operations/{op_id}")
        if status["status"] != "PENDING":
            print(
                f"  {what}: {status['status']}"
                + (f" ({status['reason']})" if status.get("reason") else "")
            )
            return
        time.sleep(0.5)
    sys.exit(f"timed out waiting for {what}")


def wait_for_api(timeout: float = 60) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            requests.get(BASE.rsplit("/", 1)[0] + "/_local/health", timeout=3).raise_for_status()
            return
        except requests.RequestException:
            time.sleep(1)
    sys.exit("API never became reachable")


def main() -> None:
    print(f"Seeding via {BASE}")
    wait_for_api()
    op = call(
        "POST", "/account/cash/adjust", {"delta_cash": STARTING_CASH, "max_allowed_cash": MAX_CASH}
    )
    wait_for(op["op_id"], f"add ${STARTING_CASH:,} managed cash")
    for symbol, shares in STOCKS:
        op = call("POST", "/watchlist", {"symbol": symbol, "shares_managed": shares})
        wait_for(op["op_id"], f"add {symbol} ({shares} shares)")
    print("Done. News, prices, scores and trades follow asynchronously (watch the web UI).")


if __name__ == "__main__":
    main()
