#!/bin/bash
set -e

REGION="us-west-1"

echo "Fetching stack outputs from region $REGION..."

QUEUE_URL=$(aws cloudformation describe-stack-resources \
  --stack-name MessagingStack \
  --region $REGION \
  --query "StackResources[?ResourceType=='AWS::SQS::Queue' && contains(LogicalResourceId, 'SystemOpsQueue')].PhysicalResourceId" \
  --output text)

if [[ "$QUEUE_URL" != https* ]]; then
  QUEUE_URL=$(aws sqs get-queue-url --queue-name "$QUEUE_URL" --region $REGION --query QueueUrl --output text)
fi

if [ -z "$QUEUE_URL" ] || [ "$QUEUE_URL" == "None" ]; then
  echo "Error: Could not determine SystemOpsQueue URL."
  exit 1
fi

MGMT_URL=$(aws cloudformation describe-stacks \
  --stack-name AppStack \
  --region $REGION \
  --query "Stacks[0].Outputs[?contains(OutputKey, 'ManagementApiEndpoint')].OutputValue" \
  --output text)

NETWORTH_URL=$(aws cloudformation describe-stacks \
  --stack-name AppStack \
  --region $REGION \
  --query "Stacks[0].Outputs[?contains(OutputKey, 'NetWorthApiEndpoint')].OutputValue" \
  --output text)

echo "Found Queue:    $QUEUE_URL"
echo "Found Mgmt API: $MGMT_URL"
echo "Found Plot API: $NETWORTH_URL"

export SYSTEM_OPS_QUEUE_URL="$QUEUE_URL"
export MGMT_API_BASE_URL="$MGMT_URL"
export NETWORTH_API_BASE_URL="$NETWORTH_URL"
export AWS_REGION="$REGION"
export AWS_DEFAULT_REGION="$REGION"
export TABLE_NAME="TradingTable"

# Run detached
docker compose up --build -d

echo ""
echo "✅ Local app is running in background!"
echo "➡️  Frontend: http://localhost:8080"
echo "➡️  Logs:     docker compose logs -f"
echo "➡️  Stop:     docker compose down"
