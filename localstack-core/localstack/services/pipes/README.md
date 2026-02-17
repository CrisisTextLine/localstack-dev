# EventBridge Pipes

EventBridge Pipes connects event sources to targets with optional filtering and input transformation.

## Supported Sources & Targets

| Sources            | Targets            |
|--------------------|--------------------|
| SQS                | SQS                |
| Kinesis            | Kinesis            |
| DynamoDB Streams   | API Destinations   |

## Quick Start

### SQS → SQS

```bash
# Create queues
aws --endpoint-url=http://localhost:4566 sqs create-queue --queue-name orders
aws --endpoint-url=http://localhost:4566 sqs create-queue --queue-name processed-orders

# Create pipe
aws --endpoint-url=http://localhost:4566 pipes create-pipe \
  --name order-pipe \
  --source arn:aws:sqs:us-east-1:000000000000:orders \
  --target arn:aws:sqs:us-east-1:000000000000:processed-orders \
  --role-arn arn:aws:iam::000000000000:role/pipe-role

# Send a message
aws --endpoint-url=http://localhost:4566 sqs send-message \
  --queue-url http://localhost:4566/000000000000/orders \
  --message-body '{"orderId": "123", "amount": 49.99}'

# Receive from target
aws --endpoint-url=http://localhost:4566 sqs receive-message \
  --queue-url http://localhost:4566/000000000000/processed-orders
```

### SQS → Kinesis

```bash
aws --endpoint-url=http://localhost:4566 kinesis create-stream \
  --stream-name events --shard-count 1

aws --endpoint-url=http://localhost:4566 pipes create-pipe \
  --name sqs-to-kinesis \
  --source arn:aws:sqs:us-east-1:000000000000:orders \
  --target arn:aws:kinesis:us-east-1:000000000000:stream/events \
  --target-parameters '{"KinesisStreamParameters": {"PartitionKey": "orderId"}}' \
  --role-arn arn:aws:iam::000000000000:role/pipe-role
```

### DynamoDB Streams → SQS

```bash
aws --endpoint-url=http://localhost:4566 dynamodb create-table \
  --table-name users \
  --attribute-definitions AttributeName=id,AttributeType=S \
  --key-schema AttributeName=id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES

STREAM_ARN=$(aws --endpoint-url=http://localhost:4566 dynamodb describe-table \
  --table-name users --query 'Table.LatestStreamArn' --output text)

aws --endpoint-url=http://localhost:4566 pipes create-pipe \
  --name dynamo-to-sqs \
  --source "$STREAM_ARN" \
  --target arn:aws:sqs:us-east-1:000000000000:processed-orders \
  --source-parameters '{"DynamoDBStreamParameters": {"StartingPosition": "LATEST", "BatchSize": 10}}' \
  --role-arn arn:aws:iam::000000000000:role/pipe-role
```

### Kinesis → Webhook (API Destination)

Stream events from Kinesis directly to your application's HTTP endpoint.

```bash
# 1. Create the Kinesis stream
aws --endpoint-url=http://localhost:4566 kinesis create-stream \
  --stream-name app-events --shard-count 1

# 2. Create a connection (auth sent with every webhook call)
aws --endpoint-url=http://localhost:4566 events create-connection \
  --name app-webhook-conn \
  --authorization-type API_KEY \
  --auth-parameters '{"ApiKeyAuthParameters": {"ApiKeyName": "x-api-key", "ApiKeyValue": "my-secret-key"}}'

# 3. Create an API destination pointing to your PHP app
aws --endpoint-url=http://localhost:4566 events create-api-destination \
  --name php-webhook \
  --connection-arn arn:aws:events:us-east-1:000000000000:connection/app-webhook-conn \
  --invocation-endpoint http://host.docker.internal:8080/api/webhook/events \
  --http-method POST

# 4. Create the pipe: Kinesis → your webhook
aws --endpoint-url=http://localhost:4566 pipes create-pipe \
  --name kinesis-to-webhook \
  --source arn:aws:kinesis:us-east-1:000000000000:stream/app-events \
  --target arn:aws:events:us-east-1:000000000000:api-destination/php-webhook \
  --source-parameters '{"KinesisStreamParameters": {"StartingPosition": "LATEST", "BatchSize": 10}}' \
  --role-arn arn:aws:iam::000000000000:role/pipe-role

# 5. Publish an event — your PHP endpoint receives it as a POST
aws --endpoint-url=http://localhost:4566 kinesis put-record \
  --stream-name app-events \
  --partition-key user-123 \
  --data '{"event": "order.created", "orderId": "789", "amount": 49.99}'
```

Your PHP app receives a POST to `/api/webhook/events` with the event payload. The `x-api-key: my-secret-key` header is included on every request.

> **Note:** Use `host.docker.internal` to reach your host machine from inside the LocalStack container. On Linux you may need `--add-host=host.docker.internal:host-gateway` when running Docker.

### With Input Transformation

```bash
aws --endpoint-url=http://localhost:4566 pipes create-pipe \
  --name transformed-pipe \
  --source arn:aws:sqs:us-east-1:000000000000:orders \
  --target arn:aws:sqs:us-east-1:000000000000:processed-orders \
  --target-parameters '{"InputTemplate": "{\"source\": \"<aws.pipes.source-arn>\", \"body\": <$.body>}"}' \
  --role-arn arn:aws:iam::000000000000:role/pipe-role
```

## Managing Pipes

```bash
# List pipes
aws --endpoint-url=http://localhost:4566 pipes list-pipes

# Describe
aws --endpoint-url=http://localhost:4566 pipes describe-pipe --name order-pipe

# Stop
aws --endpoint-url=http://localhost:4566 pipes stop-pipe --name order-pipe

# Start
aws --endpoint-url=http://localhost:4566 pipes start-pipe --name order-pipe

# Delete
aws --endpoint-url=http://localhost:4566 pipes delete-pipe --name order-pipe
```

## Testing

```bash
./test-pipes.sh http://localhost:4566
```
