#!/usr/bin/env bash
set -e

# Run the connector registration in the background
/kafka/register_connectors.sh &

# Start Kafka Connect worker via upstream entrypoint
exec /docker-entrypoint.sh start
