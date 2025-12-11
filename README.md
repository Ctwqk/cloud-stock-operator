<<<<<<< HEAD
## Trading Infra CDK Skeleton (Python)

This repo contains a minimal AWS CDK (Python) skeleton for the trading/watchlist system you described.

### What it creates

- **DynamoDB table `TradingTable`**: single-table design with `PK` and `SK`.
- **S3 bucket**: stores news JSON under `news/...` and net-worth plots under `plots/networth/...`.
- **SNS topics**:
  - `ExternalOpsTopic` for external/user-initiated operations.
  - `SystemOpsTopic` for system-generated operations.
- **SQS queues**:
  - `ExternalOpsQueue` (Q1) subscribed to `ExternalOpsTopic`.
  - `SystemOpsQueue` (Q2) subscribed to `SystemOpsTopic`.
- **Lambdas** (all have stub handlers under `lambda/`):
  - `news_fetcher` (Lambda A) – scheduled news fetcher.
  - `news_writer` (Lambda F) – writes NEW_NEWS messages to S3.
  - `sentiment_scorer` (Lambda B) – S3-triggered sentiment scoring and AUTO_TRADE_DECISION.
  - `stock_info_writer` (Lambda D) – handles Q1 external operations and updates DynamoDB.
  - `soft_delete` (Lambda E) – handles MARK_STOCK_DELETED and related cleanup on Q2.
  - `networth_plotter` (Lambda C) – behind API Gateway `GET /networth` for plotting net worth.

### CDK app layout

- `cdk/app.py`: entrypoint that instantiates `TradingInfraStack`.
- `cdk/infra_stack.py`: defines all AWS resources and wiring.
- `lambda/*/main.py`: stub handlers with TODOs for each Lambda.
- `requirements.txt`: Python dependencies for the CDK app (install into a virtualenv).

### How to use

1. **Create and activate a virtualenv** (optional but recommended):

   ```bash
   cd /home/taiwei/code/cloudComputing/final
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Install AWS CDK CLI** (once per machine):

   ```bash
   npm install -g aws-cdk
   ```

3. **Bootstrap (per account/region, if not already done)**:

   ```bash
   cdk bootstrap aws://<ACCOUNT_ID>/<REGION>
   ```

4. **Set context for account and region (or pass via env)**:

   You can either:

   - Edit `cdk.json` to add `"account": "<ACCOUNT_ID>"` and `"region": "<REGION>"` under `"context"`, or
   - Pass them on the command line:

     ```bash
     cdk synth -c account=<ACCOUNT_ID> -c region=<REGION>
     ```

5. **Synthesize and deploy**:

   ```bash
   cdk synth
   cdk deploy
   ```

After deployment, CDK will output resource identifiers (e.g., the API Gateway URL for `GET /networth`). You can then start filling in the Lambda handler TODOs with your actual business logic and integrate your local backend / IBKR gateway.

## Trading System – AWS CDK Skeleton

This project contains a Python AWS CDK app plus Lambda function stubs for a small
stock-trading helper system backed by DynamoDB, S3, SNS, SQS, and API Gateway.

### Components

- **DynamoDB table `T`**: single-table design with keys `PK` and `SK` for account
  summary, watchlist entries, net-worth time series and trade history.
- **S3 bucket `S`**: stores news JSON under `news/<symbol>/...` and net-worth
  plots under `plots/networth/<account_id>/...`.
- **SNS topics**: `ExternalOpsTopic` and `SystemOpsTopic` for external vs.
  system-generated events.
- **SQS queues**: `ExternalOpsQueue` (`Q1`) and `SystemOpsQueue` (`Q2`) subscribed
  to the above topics.
- **Lambdas (Python)**:
  - `lambda_news_fetcher` (A): scans watchlist and publishes `NEW_NEWS` messages.
  - `lambda_news_writer` (F): consumes `NEW_NEWS` from `Q2` and writes S3 objects.
  - `lambda_sentiment_scorer` (B): triggered by S3 `news/` object creation, runs
    a placeholder sentiment model and updates DynamoDB; emits `AUTO_TRADE_DECISION`.
  - `lambda_stock_info_writer` (D): handles `ADD_STOCK`, `ADJUST_MANAGED_CASH`,
    `ADJUST_MANAGED_SHARES` from `Q1`.
  - `lambda_cleanup` (E): handles `MARK_STOCK_DELETED` from `Q2`.
  - `lambda_networth_plotter` (C): reads net-worth series and writes a PNG plot.
- **API Gateway**: exposes `GET /plots/networth` backed by Lambda C.
- **EventBridge rule**: triggers Lambda A every 10 minutes.
- **S3 notifications**: trigger Lambda B on `ObjectCreated` events under `news/`.

### Layout

- `cdk/`
  - `app.py`: CDK entry point.
  - `infra_stack.py`: defines all AWS resources and wiring.
- `lambda/`
  - `news_fetcher/main.py`
  - `news_writer/main.py`
  - `sentiment_scorer/main.py`
  - `stock_info_writer/main.py`
  - `soft_delete/main.py`
  - `networth_plotter/main.py`

### Setup

1. **Install dependencies**

   ```bash
   cd /home/taiwei/code/cloudComputing/final
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Bootstrap CDK (once per account/region)**

   ```bash
   cdk bootstrap
   ```

3. **Synthesize and deploy**

   ```bash
   cdk synth
   cdk deploy TradingInfraStack
   ```

### Next steps

- Replace placeholder logic in the Lambdas with real integrations:
  - Use a real news API/yfinance in `lambda_news_fetcher`.
  - Package a FinBERT or other sentiment model in `lambda_sentiment_scorer`.
  - Implement proper S3 tagging/cleanup in `lambda_cleanup`.
- Implement the **local backend + IBKR integration** that:
  - Publishes messages to `ExternalOpsTopic`.
  - Optionally polls `SystemOpsQueue` for `AUTO_TRADE_DECISION` messages.

## Trading Infra CDK Skeleton

This repo contains an AWS CDK (v2) TypeScript app that implements the skeleton of the architecture you described:

- **DynamoDB table `T`** (single-table design with `PK`/`SK`).
- **S3 bucket `S`** for news (`news/...`) and net-worth plots (`plots/networth/...`).
- **SNS topics** `ExternalOpsTopic` and `SystemOpsTopic`.
- **SQS queues** `ExternalOpsQueue` (Q1) and `SystemOpsQueue` (Q2) subscribed to their respective topics.
- **Lambdas A–F** wired to the appropriate event sources:
  - **Lambda A – News Fetcher** (`lambda/news_fetcher`): scheduled by EventBridge every 10 minutes.
  - **Lambda F – News Writer** (`lambda/news_writer`): SQS event source from `SystemOpsQueue` (Q2).
  - **Lambda B – Sentiment Scoring** (`lambda/sentiment_scorer`): S3 `ObjectCreated` notifications on `news/` prefix.
  - **Lambda D – Stock Info Writer** (`lambda/stock_info_writer`): SQS event source from `ExternalOpsQueue` (Q1).
  - **Lambda E – Soft Delete / Cleanup** (`lambda/soft_delete_cleanup`): SQS event source from `SystemOpsQueue` (Q2).
  - **Lambda C – Net-Worth Plotter** (`lambda/networth_plotter`): fronted by API Gateway (`/networth` GET).

All lambda code is currently placeholder Python; you can replace the handler logic with your actual implementations.

### Prerequisites

- Node.js and npm installed.
- AWS CDK v2 CLI installed (`npm install -g aws-cdk`).
- AWS credentials configured (e.g., via `aws configure`) for the target account.

### Install and Build

```bash
cd /home/taiwei/code/cloudComputing/final
npm install
npm run build
```

### Synthesize and Deploy

```bash
# View the generated CloudFormation template
npm run synth

# Deploy the stack
npm run deploy
```

By default the stack name is `TradingInfraStack`. You can adjust account/region via environment variables (`CDK_DEFAULT_ACCOUNT`, `CDK_DEFAULT_REGION`) or by editing `bin/app.ts`.


=======
# cloud-stock-operator
>>>>>>> 217542561e6093452452b84a19cf874ea8efa433
