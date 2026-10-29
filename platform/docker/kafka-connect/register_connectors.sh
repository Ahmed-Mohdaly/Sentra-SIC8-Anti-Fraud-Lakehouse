#!/usr/bin/env bash
set -e

CONNECT_URL="http://localhost:8083"

echo "[INFO] Waiting for Kafka Connect REST API at ${CONNECT_URL} to become ready..."
until curl -s -f "${CONNECT_URL}/connectors" > /dev/null 2>&1; do
    echo "[INFO] Kafka Connect is starting up, retrying in 5 seconds..."
    sleep 5
done

echo "[INFO] Kafka Connect REST API is ready!"

# Verify installed plugins
echo "[INFO] Available connector plugins:"
curl -s "${CONNECT_URL}/connector-plugins" | jq -r '.[].class'

# 1. Register / Update Debezium PostgreSQL CDC Source Connector
echo "[INFO] Configuring Debezium PostgreSQL Source Connector..."

DB_HOST="${DB_HOST:-containerdb.cjm6iag8qrwi.eu-west-3.rds.amazonaws.com}"
DB_PORT="${DB_PORT:-5432}"
DB_USER="${DB_USER:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-}"
DB_NAME="${DB_NAME:-postgres}"

DEBEZIUM_CONFIG=$(cat <<EOF
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "tasks.max": "1",
  "plugin.name": "pgoutput",
  "database.hostname": "${DB_HOST}",
  "database.port": "${DB_PORT}",
  "database.user": "${DB_USER}",
  "database.password": "${DB_PASSWORD}",
  "database.dbname": "${DB_NAME}",
  "database.server.name": "sic",
  "topic.prefix": "sic",
  "table.include.list": "public.users,public.cards",
  "publication.name": "debezium_publication",
  "slot.name": "debezium_cdc_slot",
  "snapshot.mode": "initial",
  "tombstones.on.delete": "false",
  "decimal.handling.mode": "double",
  "time.precision.mode": "connect",
  "transforms": "reroute",
  "transforms.reroute.type": "io.debezium.transforms.ByLogicalTableRouter",
  "transforms.reroute.topic.regex": ".*\\\\.public\\\\.(.*)",
  "transforms.reroute.topic.replacement": "\$1"
}
EOF
)

curl -s -X PUT \
  -H "Content-Type: application/json" \
  --data "${DEBEZIUM_CONFIG}" \
  "${CONNECT_URL}/connectors/debezium-postgres-source/config" | jq .

echo "[INFO] Debezium PostgreSQL Source Connector configured successfully."

# 2. Register / Update Aiven S3 Sink Connector
echo "[INFO] Configuring Aiven S3 Sink Connector..."

S3_BUCKET="${S3_BUCKET:-sic-pipeline-storage}"
AWS_REGION="${AWS_REGION:-eu-west-3}"

S3_SINK_CONFIG=$(cat <<EOF
{
  "connector.class": "io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector",
  "tasks.max": "1",
  "topics": "transactions,users,cards",
  "aws.s3.bucket.name": "${S3_BUCKET}",
  "aws.s3.region": "${AWS_REGION}",
  "format.output.type": "jsonl",
  "format.output.fields": "key,value,offset,timestamp",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false",
  "file.name.template": "bronze/{{topic}}/partition={{partition}}/{{start_offset:padding=true}}.json.gz",
  "file.compression.type": "gzip",
  "file.max.records": "50",
  "flush.timeout.ms": "5000",
  "errors.tolerance": "all",
  "errors.deadletterqueue.topic.name": "sic_sink_dlq",
  "errors.deadletterqueue.topic.replication.factor": "2",
  "errors.deadletterqueue.context.headers.enable": "true",
  "errors.log.enable": "true",
  "errors.log.include.messages": "true"
}
EOF
)

curl -s -X PUT \
  -H "Content-Type: application/json" \
  --data "${S3_SINK_CONFIG}" \
  "${CONNECT_URL}/connectors/aiven-s3-sink/config" | jq .

echo "[INFO] Aiven S3 Sink Connector configured successfully."

echo "[INFO] Initial connector status:"
curl -s "${CONNECT_URL}/connectors?expand=status" | jq .

echo "[INFO] Connector registration process complete."
