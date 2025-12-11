## cloud-stock-operator

Trading/watchlist system infrastructure using **AWS CDK (Python)** and several Lambda functions.

### What this repo contains

- **CDK app** in `cdk/`:
  - `cdk/app.py`: CDK entry point that creates `DataStack`, `MessagingStack`, and `AppStack`.
  - `cdk/infra_stack.py`: defines DynamoDB, S3, SNS, SQS, Lambda functions, EventBridge rules, and API Gateway.
- **Lambda functions** in `lambda/`:
  - `news_fetcher/main.py` (Lambda A): scheduled by EventBridge, fetches news and publishes `NEW_NEWS` messages.
  - `news_writer/main.py` (Lambda F): consumes `NEW_NEWS` from `SystemOpsQueue` and writes news JSON to S3.
  - `sentiment_scorer/main.py` (Lambda B): reads news from S3, assigns a random sentiment score, updates DynamoDB, and may emit `AUTO_TRADE_DECISION`.
  - `stock_info_writer/main.py` (Lambda D): handles external ops (`ADD_STOCK`, `ADJUST_MANAGED_CASH`, `ADJUST_MANAGED_SHARES`) from `ExternalOpsQueue`.
  - `soft_delete/main.py` (Lambda E): processes `MARK_STOCK_DELETED` and related cleanup from `SystemOpsQueue`.
  - `networth_plotter/main.py` (Lambda C): behind API Gateway, reads net-worth series from DynamoDB and (eventually) writes plots to S3.
  - Additional helpers like `control_plane_api` and `networth_snapshot_writer` are also wired in `AppStack`.

### Core AWS resources

- **DynamoDB**: single-table design `TradingTable` with `PK`/`SK` for account summary, watchlist, net-worth series, trade history, and helper items.
- **S3**: bucket for:
  - News objects under `news/<symbol>/...`.
  - Net-worth plots under `plots/networth/<account_id>/...`.
- **SNS topics**:
  - `ExternalOpsTopic` for user-initiated operations.
  - `SystemOpsTopic` for system-generated operations.
- **SQS queues**:
  - `ExternalOpsQueue` (Q1) subscribed to `ExternalOpsTopic`.
  - `SystemOpsQueue` (Q2) subscribed to `SystemOpsTopic`.
- **EventBridge**:
  - Triggers `NewsFetcherLambda` on a schedule.
  - Triggers `NetWorthSnapshotWriterLambda` periodically.

### Getting started

From the project root:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cd cdk
npm install -g aws-cdk  # if not already installed
cdk bootstrap -c account=<ACCOUNT_ID> -c region=<REGION>   # once per account/region
cdk synth -c account=<ACCOUNT_ID> -c region=<REGION>
cdk deploy AppStack -c account=<ACCOUNT_ID> -c region=<REGION>
```

After deployment, CDK will output the API Gateway URL(s) and ARNs for the core resources. You can then fill in any remaining TODOs in the Lambda handlers and plug in your local IBKR/backend logic.
