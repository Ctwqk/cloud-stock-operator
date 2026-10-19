#!/usr/bin/env bash
# Start the web UI + trade worker against a deployed cloud-stock-operator stack.
#
# Requires: AWS CLI configured for the account/region the stack lives in, Docker.
# Reads the CloudFormation outputs, so nothing is hard-coded:
#   AWS_REGION=us-west-1 ./local_app/run_against_aws.sh
set -euo pipefail

cd "$(dirname "$0")"

REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-$(aws configure get region || true)}}"
if [ -z "${REGION}" ]; then
  echo "Set AWS_REGION (or configure a default region)." >&2
  exit 1
fi

output() { # <stack> <output key>
  aws cloudformation describe-stacks --stack-name "$1" --region "${REGION}" \
    --query "Stacks[0].Outputs[?OutputKey=='$2'].OutputValue | [0]" --output text
}

echo "Reading stack outputs from ${REGION}..."
export AWS_DEFAULT_REGION="${REGION}"
export TABLE_NAME="$(output DataStack TableName)"
export BUCKET_NAME="$(output DataStack BucketName)"
export TRADE_DECISION_QUEUE_URL="$(output MessagingStack TradeDecisionQueueUrl)"
export MGMT_API_BASE_URL="$(output AppStack ManagementApiUrl)"
export NETWORTH_API_BASE_URL="$(output AppStack NetWorthApiUrl)"
API_KEY_ID="$(output AppStack ApiKeyId)"
export MGMT_API_KEY="$(aws apigateway get-api-key --api-key "${API_KEY_ID}" --include-value \
  --region "${REGION}" --query value --output text)"

for name in TABLE_NAME BUCKET_NAME TRADE_DECISION_QUEUE_URL MGMT_API_BASE_URL NETWORTH_API_BASE_URL; do
  if [ -z "${!name}" ] || [ "${!name}" = "None" ]; then
    echo "Could not read ${name} from the stacks; are they deployed in ${REGION}?" >&2
    exit 1
  fi
done

echo "Table:      ${TABLE_NAME}"
echo "Queue:      ${TRADE_DECISION_QUEUE_URL}"
echo "Mgmt API:   ${MGMT_API_BASE_URL}"
echo "Plots API:  ${NETWORTH_API_BASE_URL}"

docker compose -f docker-compose.aws.yml up --build -d

echo
echo "Control panel:  http://localhost:${WEB_PORT:-9090}"
echo "Logs:           docker compose -f local_app/docker-compose.aws.yml logs -f"
echo "Stop:           docker compose -f local_app/docker-compose.aws.yml down"
