# Verification record

Verified on 2026-10-19 from a separate clean source directory, without an existing virtual
environment, built layers, or CDK output. No AWS account credentials or deployment were used.

| Command / check | Result |
|---|---|
| `make install` | Locked environment installed on Python 3.12.7 |
| `make lint` | Ruff lint and format checks passed (63 Python files) |
| `uv run --no-sync pytest --cov --cov-report=term:skip-covered --cov-fail-under=85` | **308 passed**, 91.68% coverage, 90.14 seconds on this host |
| `UV_OFFLINE=1 npm_config_offline=true make synth` | All three stacks synthesized using cached tools, with missing-layer warnings as documented |
| `./scripts/build_layers.sh` | Linux x86_64 / Python 3.12 wheels built: matplotlib 125 MiB, sentiment 3 MiB, yfinance 142 MiB; each below the size gate |
| `docker build -f local_app/Dockerfile ...` | Image built from the clean source; build context 727 kB |
| `cdk synth --quiet --no-notices --no-lookups` inside `docker run --network none` | All three stacks synthesized **with networking disabled** and real built layers |
| `CAPTURE_SCREENSHOT=1 SKIP_BUILD=1 WEB_PORT=41090 ./scripts/smoke_local_demo.sh` | Compose pipeline fetched fixture news, scored it and executed trades; Playwright generated the plot and captured `docs/screenshot.png` |

The screenshot is 285,803 bytes and shows five watchlist entries, nine virtual trades and
three scheduled net-worth snapshots. The browser reported no page errors. The smoke script
removed its containers and network after capture.

New regression cases cover failed/ambiguous SNS delivery after subsequent news, concurrent
score updates, invalid trade quantities, missing decision IDs, and partial S3 cleanup failures
in both queued and scheduled handlers. The inherited suite also covers all nine handlers,
CDK wiring and IAM assertions, and the local HTTP pipeline.

On the shared verification host, heavy work ran under `flock` and a user systemd scope with
`MemoryMax=6G` / `MemorySwapMax=0`, with at least 8 GiB available before each heavy step.
Docker build and offline-synthesis containers had 1.5 GiB caps; the demo used the committed
per-service memory limits. Stack startup, browser capture and teardown shared one lock.

The [CI workflow](../.github/workflows/ci.yml) independently runs lint, the coverage-gated
suite, credential-free synthesis, layer builds and the Compose smoke test on pushes and PRs.
See [branch runs](https://github.com/Ctwqk/cloud-stock-operator/actions/workflows/ci.yml?query=branch%3Aportfolio-upgrade)
for the latest result.

Real AWS deployment/IAM behavior and live yfinance availability remain unverified.
