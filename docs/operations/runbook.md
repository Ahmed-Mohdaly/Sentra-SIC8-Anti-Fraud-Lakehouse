# Operational Runbook & Incident Response Guide
## SIC — Synthetic Financial Transaction Data Platform

**System:** SIC Data Platform (Streaming, CDC, Batch Ingestion)  
**Target Environment:** AWS `eu-west-3` (Paris) | **Account ID:** `138257835434`  
**Classification:** Internal Production Operations Runbook  
**Last Updated:** October 26, 2026  

---

## 1. System Overview & Architecture Summary

The SIC platform delivers synthetic financial transactions and CDC-captured entities to an Amazon S3 Bronze data lakehouse through three ingestion pipelines:
1. **Streaming Pipeline:** Generator/API (`synthetic-data-service`) produces directly to Amazon MSK topic `transactions`.
2. **CDC Pipeline:** Relational changes to `users` and `cards` in PostgreSQL RDS (`containerdb`) are captured by Debezium Source Connector via `pgoutput` logical replication into MSK topics `users` and `cards`.
3. **S3 Delivery:** Aiven S3 Sink Connector consumes `transactions`, `users`, and `cards` from MSK and delivers compressed JSONL files into `s3://sic-pipeline-storage/bronze/{topic}/`.
4. **Batch Reference Ingestion:** EventBridge rule `scheduled-ingestion-extractor` launches ECS task `synthetic-ingestion-extractor:5` to take daily Snappy Parquet snapshots of `merchant_category_codes` and `city_reference`.

---

## 2. Health Check Matrix & Routine Verification

| Component | Check Type | Command / Endpoint | Expected Response / Healthy State |
|---|---|---|---|
| **FastAPI Generator** | HTTP GET | `http://<API_IP>:8000/health` | `{"status": "ok", "database": "connected"}` (HTTP 200) |
| **Kafka Connect Cluster** | HTTP GET | `http://172.31.10.236:8083/connectors` | `["debezium-postgres-source", "aiven-s3-sink"]` (HTTP 200) |
| **Debezium Connector** | HTTP GET | `http://172.31.10.236:8083/connectors/debezium-postgres-source/status` | `"state": "RUNNING"`, tasks[0] `"state": "RUNNING"` |
| **Aiven S3 Sink** | HTTP GET | `http://172.31.10.236:8083/connectors/aiven-s3-sink/status` | `"state": "RUNNING"`, tasks[0] `"state": "RUNNING"` |
| **PostgreSQL Replication** | SQL Query | `SELECT slot_name, active FROM pg_replication_slots;` | `debezium_cdc_slot` has `active = true` |
| **Amazon MSK Cluster** | AWS CLI | `aws kafka describe-cluster --cluster-arn <ARN>` | `"State": "ACTIVE"` |
| **ECS Fargate Services** | AWS CLI | `aws ecs describe-services --cluster synthetic-data-cluster --services synthetic-data-service sic-kafka-connect-service` | Both services show `runningCount == desiredCount` |

---

## 3. Standard Operating Procedures (SOP)

### SOP-01: Restarting a Failed Kafka Connector or Task
If a connector or task enters a `FAILED` state (visible in `/status`):

```bash
# Option A: Restart entire connector and all tasks
curl -X POST http://172.31.10.236:8083/connectors/aiven-s3-sink/restart?includeTasks=true

# Option B: Restart specific task (e.g. Task 0)
curl -X POST http://172.31.10.236:8083/connectors/aiven-s3-sink/tasks/0/restart

# Option C: Restart Debezium source connector
curl -X POST http://172.31.10.236:8083/connectors/debezium-postgres-source/restart?includeTasks=true
```

### SOP-02: Updating Connector Configuration
To update connector parameters without deleting the connector:

```bash
curl -X PUT http://172.31.10.236:8083/connectors/aiven-s3-sink/config \
  -H "Content-Type: application/json" \
  -d '{
    "connector.class": "io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector",
    "tasks.max": "1",
    "topics": "transactions,users,cards",
    "aws.s3.bucket.name": "sic-pipeline-storage",
    "aws.s3.region": "eu-west-3",
    "aws.sts.config.endpoint": "https://sts.eu-west-3.amazonaws.com",
    "format.output.type": "jsonl",
    "format.output.compression": "gzip",
    "file.name.template": "bronze/{{topic}}/partition={{partition}}/{{start_offset:padding=true}}.json.gz",
    "file.max.records": "1000",
    "flush.timeout.ms": "10000",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "sic_sink_dlq",
    "errors.deadletterqueue.topic.replication.factor": "2",
    "errors.deadletterqueue.context.headers.enable": "true",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true"
  }'
```

### SOP-03: Triggering Ad-Hoc Batch Reference Ingestion
To manually run the batch reference extractor outside of the EventBridge schedule:

```bash
aws ecs run-task \
  --region eu-west-3 \
  --cluster synthetic-data-cluster \
  --task-definition synthetic-ingestion-extractor:5 \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[subnet-003d3891dab36b354,subnet-0b18944bde732d1fd],securityGroups=[sg-0dd0933f4da2b5ea7],assignPublicIp=ENABLED}" \
  --overrides '{"containerOverrides":[{"name":"extractor","command":["batch-reference"]}]}'
```

### SOP-04: Inspecting Topic Watermarks & Offsets
To inspect current offset positions on MSK:

```bash
# List consumer group lag for S3 sink
kafka-consumer-groups.sh \
  --bootstrap-server b-1.sicstreamingmsk.4io9um.c2.kafka.eu-west-3.amazonaws.com:9092 \
  --describe --group connect-aiven-s3-sink
```

---

## 4. Incident Response Playbooks

### Playbook 1: S3 Sink Connector Fails with STS Region Mismatch
- **Symptoms:** CloudWatch logs show `AWSSecurityTokenServiceException: Credential should be scoped to a valid region (SignatureDoesNotMatch)`.
- **Root Cause:** Aiven S3 connector defaults to the global STS endpoint `sts.amazonaws.com` rather than the regional endpoint `sts.eu-west-3.amazonaws.com`.
- **Remediation:**
  1. Inspect connector config via `GET /connectors/aiven-s3-sink/config`.
  2. Verify `"aws.sts.config.endpoint": "https://sts.eu-west-3.amazonaws.com"` is present.
  3. Re-apply configuration via SOP-02 and restart the connector via SOP-01.

### Playbook 2: Inactive PostgreSQL Replication Slot & Disk Space Alert
- **Symptoms:** RDS CloudWatch alarm `FreeStorageSpace` decreasing rapidly.
- **Root Cause:** If Debezium is stopped or fails to connect, PostgreSQL retains all WAL segments since `confirmed_flush_lsn`, consuming storage.
- **Remediation:**
  1. Query active slots:
     ```sql
     SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_bytes
     FROM pg_replication_slots;
     ```
  2. If `active` is `false`, check `sic-kafka-connect-service` logs in CloudWatch (`/ecs/sic-kafka-connect`).
  3. If Kafka Connect is down, restart the ECS service:
     ```bash
     aws ecs update-service --cluster synthetic-data-cluster --service sic-kafka-connect-service --force-new-deployment
     ```
  4. If the connector is permanently decommissioned, drop the slot to release WAL disk space:
     ```sql
     SELECT pg_drop_replication_slot('debezium_cdc_slot');
     ```

### Playbook 3: Fargate Regional vCPU Limit Exceeded
- **Symptoms:** ECS task launch fails with `ResourceInitializationError: Fargate vCPU limit exceeded`.
- **Root Cause:** Region `eu-west-3` has a quota of **2.0 vCPUs** on Fargate.
  - Active: `sic-kafka-connect-service` (1.0 vCPU) + `synthetic-data-service` (0.5 vCPU) = 1.5 vCPUs.
  - Remaining headroom is only **0.5 vCPU**. Launching a 0.5 vCPU task while another task is running will trigger the quota failure.
- **Remediation:**
  1. Always configure batch extractor tasks to use `256` CPU (0.25 vCPU) and `512` memory.
  2. Never launch parallel batch tasks simultaneously.
  3. If additional capacity is needed, submit an AWS Service Quota increase for `Fargate vCPU count`.

### Playbook 4: Transactions Erroneously Persisting in PostgreSQL
- **Symptoms:** `SELECT count(*) FROM transactions;` increases during synthetic generation.
- **Root Cause:** `generator_service.py` was invoked with legacy database insertion mode.
- **Remediation:**
  1. Ensure `generator_service.py` runs with `--sink msk` or `SINK_TARGET=msk`.
  2. Confirm transaction generator uses `KafkaPublisher` targeting MSK topic `transactions`.
  3. Verify `generator_service.py` line where `db.bulk_insert_transactions()` is bypassed.

---

## 5. Escalation Protocol & Contact Information

| Tier | Role | Responsibilities | SLA |
|---|---|---|---|
| **Tier 1** | On-Call Data Ops | Health checks, connector restarts, EventBridge verification | 15 mins |
| **Tier 2** | Lead Data Engineer | Schema evolution, Debezium offset reset, Fargate scaling | 30 mins |
| **Tier 3** | Cloud Security / AWS Architect | IAM policies, VPC routing, RDS parameter group maintenance | 1 hour |
