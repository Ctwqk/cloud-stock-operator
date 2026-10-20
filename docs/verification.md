# Verification record

Verified on 2026-10-19 from a separate clean source directory, without an existing virtual
environment, built layers, or CDK output. No AWS account credentials or deployment were used.

| Command / check | Result |
|---|---|
| `make install` | Locked environment installed on Python 3.12.7 |
| `make lint` | Ruff lint and format checks passed |
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

## Independent review — 2026-10-19 (America/Los_Angeles)

Compared the upgrade at `37452c1` with default-branch commit `171ce0e`. A fresh source
checkout passed the documented `make install`, `make lint`, `make test` (308 tests) and
`make synth`. The original CI test log independently confirms 308 tests and 91.68% coverage.

Findings fixed during review:

* **P1:** S3 cleanup failures left stocks active, old delete deliveries deactivated re-added
  stocks, and share adjustments could commit after a concurrent deletion. Deletion now commits
  with an operation marker before cleanup; retries preserve reactivation, unknown stocks leave
  S3 untouched, and share transactions guard the deleted flag (`a93de87`).
* **P2:** The sentiment API generated new deduplication IDs for requests without `event_id`,
  allowing retries to apply the delta again. It now rejects missing/invalid IDs (`c5036a7`).
* **P2:** Broad CDK grants allowed unnecessary DynamoDB deletes and S3 object writes/listing;
  the old wildcard assertion missed patterns such as `GetObject*`. Application policies now
  list the required actions, and cleanup listing is restricted to `news/*` (`5826865`).
* **P2/P3:** The AWS launcher swallowed failed API-key lookups and started containers anyway;
  the deploy instructions also left the shell in the wrong directory. The launcher now stops
  on CLI failures and empty keys, with isolated CLI-stub regressions (`a5ba788`).

| Review verification | Result |
|---|---|
| Full suite with `--cov --cov-report=term-missing:skip-covered --cov-fail-under=85` | **322 passed**, **91.52%** coverage |
| Ruff lint and formatting | Passed, 65 Python files |
| `make layers` and isolated Python 3.12 container imports | matplotlib 125 MiB, sentiment 3 MiB, yfinance 142 MiB; all three imported with networking disabled |
| `UV_OFFLINE=1 npm_config_offline=true make synth` | All three stacks synthesized with built layers and credential files disabled |
| CDK CLI synthesis in a container with `--network=none` | All three stacks synthesized with built layers |
| `CAPTURE_SCREENSHOT=1 SKIP_BUILD=1 WEB_PORT=41783 make smoke` | Five stocks, nine executed virtual trades, three plotted snapshots; no page errors; 285,445-byte review capture |

The original committed screenshot is retained because the UI did not change. The first review
capture hit a Chromium startup failure with a long nested temporary path; shortening the
review `TMPDIR` allowed the same cached browser to start and complete the capture. Both smoke
runs tore down their stacks. All heavy review steps held the shared lock and ran in a user
systemd scope with `MemoryMax=6G` / `MemorySwapMax=0`, after checking for at least 8 GiB available.
Docker builds/offline synthesis used 1.5 GiB caps, layer imports 512 MiB, and the demo used its
committed per-service caps. No production containers were modified.

The deletion regressions failed before their fixes; the sentiment validation, synthesized
IAM policy and launcher failure paths also had independent failing reproductions. The review
found no AWS access-key/private-key signatures in the current or default-branch trees and no
tracked files over 1 MiB. The 99-term sentiment extension is deterministic; these tests do not
establish predictive accuracy or profitability.

IAM action selection follows the AWS documentation for
[DynamoDB transaction permissions](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis-iam.html)
and [S3 listing prefix conditions](https://docs.aws.amazon.com/AmazonS3/latest/userguide/amazon-s3-policy-keys.html).
No AWS resources were deployed. Emulator tests do not validate real IAM enforcement,
service-side transaction conflicts, TTL expiry or Lambda event-source delivery timing.
