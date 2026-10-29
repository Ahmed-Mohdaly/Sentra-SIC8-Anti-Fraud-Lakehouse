# Technical Audit Report: Executive Summary
## Synthetic Financial Data Ingestion Pipeline (SIC Platform)

**Audit Date:** October 26, 2026  
**Auditor:** Principal Cloud Architect & Independent Data Engineering Reviewer  
**Audit Scope:** End-to-end Ingestion Pipeline (Streaming, CDC, Batch, Lakehouse)  
**Target AWS Environment:** Account `138257835434` | Region `eu-west-3` (Paris)  
**Audit Mode:** Read-Only Source Code, AWS Configuration, Runtime Evidence & Test Analysis

---

## 1. System Overview: What the Pipeline Actually Does

The Synthetic Financial Data Ingestion Platform (SIC) is deployed across Amazon MSK, Amazon RDS PostgreSQL, AWS ECS Fargate, Amazon EventBridge, and Amazon S3. The intended business objective is to ingest synthetic financial transactions, customer reference entities, and static dimension tables into an Amazon S3 Bronze data lakehouse (`sic-pipeline-storage`).

Based on direct inspection of active AWS resources, running task definitions, S3 object hierarchies, CloudWatch logs, and application source code, the system actually operates as follows:

1. **Transaction Event Generation & Streaming:**
   - Synthetic transactions are generated in memory by `generator_service.py` and published directly via `confluent-kafka` (`KafkaEventPublisher`) to Amazon MSK topic `transactions`.
   - Transactions are **not** written to PostgreSQL `transactions` table during generator loop execution.
2. **User & Card Entity Persistence & Debezium CDC:**
   - Generated users and credit cards are persisted to Amazon RDS PostgreSQL 18.3 (`containerdb`) to maintain referential integrity.
   - RDS logical replication (`pgoutput`) streams WAL changes through a replication slot (`debezium_cdc_slot`) to a Debezium PostgreSQL Source Connector running in ECS Fargate (`sic-kafka-connect-service`).
   - Debezium routes table events from `public.users` and `public.cards` to MSK topics `users` and `cards`.
3. **Data Lakehouse Delivery (Kafka Connect S3 Sink):**
   - An Aiven S3 Sink connector consumes from `transactions`, `users`, and `cards` topics and writes gzip-compressed JSONL files to S3.
   - **Critical Audit Finding:** S3 Sink output files are landing at the **bucket root** (`s3://.../transactions/`, `s3://.../users/`, `s3://.../cards/`) rather than the expected `bronze/` directory because `file.name.template` in the active connector configuration omitted `{{file_name_prefix}}`.
4. **Reference Data Batch Extraction:**
   - EventBridge rule `scheduled-ingestion-extractor` triggers ECS Fargate task `synthetic-ingestion-extractor:5` (`BatchReferenceExtractor`).
   - The extractor pulls `merchant_category_codes` (914 rows) and `city_reference` (1,433 rows) from RDS and writes strongly typed Snappy Parquet snapshots to `s3://.../bronze/mcc/` and `s3://.../bronze/cities/`.
   - **Critical Audit Finding:** The EventBridge rule is configured to execute every **5 minutes** (`rate(5 minutes)`) instead of daily, resulting in 288 redundant Parquet snapshots per day, unnecessary Fargate compute cost, and severe small-file accumulation on S3.
5. **Real-Time ML Fraud Inference & Shared Online Feature Store (Path 4):**
   - A dedicated ECS Fargate consumer (`sic-fraud-consumer:2`) streams transactions directly from MSK topic `transactions`.
   - Shared online feature store **AWS ElastiCache for Redis** (`cache.t4g.micro`, Redis 7.1 with In-Transit TLS & KMS encryption) hydrates 1h/24h sliding windows, velocity, and demographic profiles across 8,346 live keys.
   - Evaluates fraud risk using LightGBM (<1ms scoring, 6-8ms E2E) and publishes enriched alerts to MSK topic `fraud_alerts`.
   - Consumer offsets are committed manually to Kafka only after confirmed alert delivery, preventing missed alerts.

---

## 2. Component Verification Summary

| Component / Subsystem | Verification Level | Actual Status | Primary Evidence |
|---|---|---|---|
| **Amazon MSK Cluster** | **VERIFIED** | `ACTIVE` (Kafka 3.8.x, 2 brokers, 10 GB EBS/broker) | AWS CLI `describe-cluster`: ARN `.../sic-streaming-msk/ff954d9e-...` |
| **RDS PostgreSQL (containerdb)** | **VERIFIED** | `available` (PostgreSQL 18.3, `db.t4g.micro`, `wal_level=logical`) | AWS CLI `describe-db-instances`, `describe-db-parameters` |
| **ECS Cluster & Services** | **VERIFIED** | `ACTIVE` (2 services: `synthetic-data-service` 1/1, `sic-kafka-connect-service` 1/1) | AWS CLI `describe-services` |
| **Transaction Direct Streaming** | **VERIFIED** | Operational (produces to MSK `transactions`, offset ~220) | Source `generator_service.py:265-273`, live S3 object payload |
| **PostgreSQL 0-Insert for Stream** | **PARTIALLY VERIFIED** | Verified during generator loop; legacy API still queries DB table | `generator_service.py:302`, `database.py:711-738`, `api.py:262` |
| **Debezium CDC Source Connector** | **VERIFIED** | `RUNNING` (Task 0 `RUNNING`, captures users & cards) | S3 JSONL records containing Debezium 2.7.3 envelopes |
| **Aiven S3 Sink Connector** | **PARTIALLY VERIFIED** | `RUNNING`, but **misconfigured path** (writes to root, not `bronze/`) | S3 listing: `users/0/*.json.gz`, `cards/0/*.json.gz`, `transactions/0/*.json.gz` |
| **Batch Reference Extractor** | **VERIFIED** | Operational (extracts 914 MCC & 1,433 Cities to Parquet) | CloudWatch logs `scheduled/ingestion-extractor/ea93c1abb...` |
| **EventBridge Orchestration** | **VERIFIED** | Enabled, but **over-scheduled** (`rate(5 minutes)`) | AWS CLI `events list-rules`: `rate(5 minutes)` |
| **Secrets Manager Integration** | **VERIFIED** | Active (`synthetic-data-provider/database`) | Dynamic ARN injection in Task Definitions |
| **AWS ElastiCache Redis Feature Store** | **VERIFIED** | `available` (Redis 7.1, `cache.t4g.micro`, In-Transit TLS & KMS) | Boto3 SDK: 8,346 active keys, 1.1ms read, 1.9ms write |
| **Real-Time Fraud Detection Consumer** | **VERIFIED** | `RUNNING` (`sic-fraud-consumer:2`, LightGBM Core, Alert Topic) | CloudWatch Logs: 6-8ms E2E latency, alert delivery & offset commit |

---

## 3. Pillar Health & Readiness Ratings

| Dimension | Rating (1–5) | Justification & Supporting Evidence |
|---|:---:|---|
| **Architecture Correctness** | **3.5 / 5** | Core decoupled topology (MSK + CDC + S3 Sink + Batch Parquet) is sound. However, S3 Sink pathing defect routes files outside `bronze/`, and REST API `/transactions` serves stale DB data rather than the streaming lakehouse. |
| **Data Correctness** | **4.0 / 5** | Schemas are strongly typed. Foreign key integrity between users and cards is enforced in RDS. Debezium preserves primary keys (`{"user_id": 888803}`). Parquet schemas for MCC and Cities strictly match relational types. |
| **Reliability & Resilience** | **3.0 / 5** | Producer in `generator_service.py` lacks asynchronous delivery error recovery (messages dropped if broker ACK fails after enqueue). Fargate regional quota of 2.0 vCPUs leaves only 0.5 vCPU headroom, risking task launch failures. |
| **Security Posture** | **4.5 / 5** | Database credentials properly rotated and stored in AWS Secrets Manager. RDS is `PubliclyAccessible: False`. S3 Block Public Access enabled. Ingress on port 8000 locked to internal VPC CIDR + authorized IP. Passwords sanitized in logs. |
| **Observability** | **3.0 / 5** | CloudWatch log groups exist and capture events. However, no CloudWatch Alarms are configured for consumer lag, replication slot WAL growth, Fargate vCPU throttling, or connector task failures. |
| **Operational Readiness** | **3.5 / 5** | Runbooks and health check commands are documented. Connectors survive reboots via registrar sidecar. However, EventBridge 5-minute schedule will rapidly inflate S3 file counts and API costs if left unthrottled. |

---

## 4. Top Five Critical Findings & Architectural Weaknesses

1. **S3 Lakehouse Pathing Defect (Aiven S3 Sink Configuration):**
   - In `scripts/fix_s3_sink_sts.py`, `file.name.template` was configured as `{{topic}}/{{partition}}/{{start_offset:padding=true}}.json.gz`. This overrides `"file.name.prefix": "bronze/"`.
   - As a result, streaming transactions and CDC entities land at `s3://.../transactions/`, `s3://.../users/`, and `s3://.../cards/` at the bucket root, while batch dimensions land at `s3://.../bronze/mcc/` and `s3://.../bronze/cities/`. Downstream analytics expecting all raw data in `bronze/` will miss all streaming and CDC data.
2. **EventBridge Batch Extractor Cadence Over-Execution (`rate(5 minutes)`):**
   - The batch reference extractor is triggered every 5 minutes by EventBridge rule `scheduled-ingestion-extractor`. Because reference data is static (914 MCC records, 1,433 Cities), this generates 288 Parquet files per dataset per day (~17,280 files/month), inflating S3 PUT request costs and creating small-file query degradation.
3. **Producer Asynchronous Delivery Silent Drop Risk:**
   - `generator_service.py:265-273` invokes `self.publisher.publish(...)` without an `on_delivery` callback. If librdkafka encounters a delivery failure after enqueuing (e.g. partition leader failover timeout), the failure is incremented in an internal counter and logged, but the transaction event is permanently lost without a retry queue or dead-letter storage.
4. **Fargate Regional vCPU Quota Bottleneck (2.0 vCPU Limit):**
   - The AWS account has a hard quota of **2.0 vCPUs** on Fargate in `eu-west-3`.
   - Steady-state consumption: `sic-kafka-connect-service` (1.0 vCPU) + `synthetic-data-service` (0.5 vCPU) = 1.5 vCPUs.
   - Only 0.5 vCPU remains. When EventBridge runs the batch extractor (0.25 vCPU), headroom drops to 0.25 vCPU. Any ECS rolling deployment or ad-hoc task triggers `ResourceInitializationError: You've reached the limit on the number of vCPUs you can run concurrently`.
5. **API & Database Data Disconnect:**
   - `api.py` exposes `GET /api/v1/transactions` by querying SQLAlchemy model `TransactionModel` against the PostgreSQL database. Since new streaming transactions bypass PostgreSQL, this API endpoint only returns pre-existing baseline data (32,353 records) and never serves newly generated streaming transactions.
