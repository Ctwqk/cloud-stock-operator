#!/usr/bin/env bash
# Smoke test of the docker compose demo: build, start, wait until news has been scored and
# virtual trades were executed, check the web UI shows them, then tear everything down.
#
#   ./scripts/smoke_local_demo.sh
#   COMPOSE_PROJECT=my-test WEB_PORT=41090 LOCAL_IMAGE=my-image ./scripts/smoke_local_demo.sh
set -euo pipefail

cd "$(dirname "$0")/.."
PROJECT="${COMPOSE_PROJECT:-cloud-stock-smoke}"
export WEB_PORT="${WEB_PORT:-9090}"
COMPOSE=(docker compose -p "${PROJECT}" -f local_app/docker-compose.yml)

cleanup() {
  status=$?
  if [ "${status}" -ne 0 ]; then
    "${COMPOSE[@]}" logs --tail=60 || true
  fi
  "${COMPOSE[@]}" down --volumes --remove-orphans || true
  exit "${status}"
}
trap cleanup EXIT

if [ "${SKIP_BUILD:-0}" = 1 ]; then
  "${COMPOSE[@]}" up --no-build -d
else
  "${COMPOSE[@]}" up --build -d
fi

echo "waiting for trades on http://localhost:${WEB_PORT} ..."
for _ in $(seq 1 90); do
  page="$(curl -fsS "http://localhost:${WEB_PORT}/" 2>/dev/null || true)"
  if grep -q "executed virtual" <<<"${page}"; then
    break
  fi
  sleep 2
done

grep -q "executed virtual" <<<"${page}" || { echo "no executed trades appeared" >&2; exit 1; }
grep -q "AAPL" <<<"${page}" || { echo "watchlist missing AAPL" >&2; exit 1; }
grep -q "Managed net worth" <<<"${page}" || { echo "summary missing" >&2; exit 1; }
echo "local demo OK: news was fetched, scored and traded."

if [ "${CAPTURE_SCREENSHOT:-0}" = 1 ]; then
  export WEB_URL="http://127.0.0.1:${WEB_PORT}"
  npx --yes --package=playwright@1.57.0 -c \
    'NODE_PATH="$(dirname "$(dirname "$(command -v playwright)")")" node scripts/capture_screenshot.js'
fi
