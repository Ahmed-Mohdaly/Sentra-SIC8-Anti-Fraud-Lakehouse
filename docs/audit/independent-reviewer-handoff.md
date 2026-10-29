# Independent Architecture Reviewer Handoff Document
## Synthetic Financial Data Ingestion Pipeline (SIC Platform)

**Audit Completion Date:** October 26, 2026  
**Auditor:** Principal Cloud Solutions Architect & Data Engineering Specialist  
**Target Environment:** AWS `eu-west-3` (Paris) | **Account ID:** `138257835434`  
**Purpose:** Self-contained, evidence-based architectural dossier for independent peer review.

---

## 1. Compact Summary of the Verified Architecture

The SIC platform on AWS (`eu-west-3`) is a hybrid streaming, CDC, and batch ingestion lakehouse:

```
[generator_service.py] ──(Direct Stream: MSK acks=1)──> [MSK: transactions] ──> [Aiven S3 Sink] ──> [S3 Root: transactions/]
        │
        └──(SQL INSERT)──> [RDS PostgreSQL: users & cards]
                                  │
                          (WAL / pgoutput)
                                  │
                                  ▼
                   [Debezium: debezium_cdc_slot]
                                  │
                          (RegexRouter $1)
                                  │
                                  ▼
                        [MSK: users & cards] ──> [Aiven S3 Sink] ──> [S3 Root: users/ & cards/]

[EventBridge (rate: 5m)] ──> [ECS Extractor:5] ──> [RDS: mcc & cities] ──(Parquet)──> [S3: bronze/mcc/ & bronze/cities/]
```

1. **Transactions:** Bypasses PostgreSQL entirely during streaming. Ingested directly into MSK topic `transactions` and flushed to S3 via Aiven S3 Sink in gzip JSONL format.
2. **Users & Cards:** Persisted in Amazon RDS PostgreSQL 18.3 (`containerdb`). Captured via PostgreSQL logical replication (`pgoutput`) and Debezium Source Connector, streamed to MSK topics `users` and `cards`, and flushed to S3 via Aiven S3 Sink.
3. **Reference Data:** `merchant_category_codes` (914 rows) and `city_reference` (1,433 rows) are extracted from RDS by `BatchReferenceExtractor` and uploaded as Snappy Parquet to `s3://.../bronze/mcc/` and `s3://.../bronze/cities/`.
4. **Lakehouse Storage:** Amazon S3 bucket `sic-pipeline-storage`.

---

## 2. Most Relevant Source Files & Line References

| File Path | Critical Line References | Core Architectural Responsibility |
|---|---|---|
| [`src/synthetic_streaming/generator_service.py`](file:///home/syd/Downloads/archive/src/synthetic_streaming/generator_service.py) | `L250-274` (`_generation_loop`), `L286-320` (`generate_batch`) | Generates transactions, persists prerequisites to DB, publishes transactions to MSK topic. Lacks async `on_delivery` callback. |
| [`src/synthetic_streaming/database.py`](file:///home/syd/Downloads/archive/src/synthetic_streaming/database.py) | `L635-742` (`persist_transaction`), `L615-633` (`persist_prerequisite_entities`) | Instantiates `TransactionModel` in-memory while explicitly skipping `session.add(tx_record)` (enforces 0-insert rule). |
| [`src/synthetic_streaming/kafka_publisher.py`](file:///home/syd/Downloads/archive/src/synthetic_streaming/kafka_publisher.py) | `L88-100` (librdkafka conf), `L155-175` (`_delivery_callback`), `L180-198` (`produce`) | Confluent-Kafka producer adapter with queue backpressure polling and error logging throttled to 5s. |
| [`src/synthetic_streaming/api.py`](file:///home/syd/Downloads/archive/src/synthetic_streaming/api.py) | `L262-340` (`list_transactions`), `L392-423` (`generate_batch_transactions`) | FastAPI control-plane. `/transactions` queries PostgreSQL table rather than MSK/S3 streaming data. |
| [`src/ingestion/batch_reference_extractor.py`](file:///home/syd/Downloads/archive/src/ingestion/batch_reference_extractor.py) | `L52-73` (Arrow schemas), `L120-150` (`extract_mcc`), `L154-188` (`extract_cities`) | Strongly typed PyArrow reference extractor persisting Snappy Parquet to S3. |
| [`scripts/fix_s3_sink_sts.py`](file:///home/syd/Downloads/archive/scripts/fix_s3_sink_sts.py) | `L10-29` (`s3_sink_config`) | Active Aiven S3 Sink connector config with regional STS endpoint and path template defect (`{{topic}}/{{partition}}...`). |
| [`scripts/fix_debezium_config.py`](file:///home/syd/Downloads/archive/scripts/fix_debezium_config.py) | `L26-53` (`debezium_config`) | Active Debezium connector configuration with `RegexRouter` remapping `(?:.*\.public\.)?(.*)` to `$1`. |
| [`scripts/run_e2e_live_validation.py`](file:///home/syd/Downloads/archive/scripts/run_e2e_live_validation.py) | `L114-199` (Test A), `L201-266` (Tests B & C), `L270-288` (Test D), `L292-345` (S3 assertions) | Live validation script executed in Fargate runner (`sic-admin-runner:1`). |

---

## 3. Important AWS Resource Identifiers (Secrets Redacted)

- **AWS Region:** `eu-west-3` (Paris) | **Account ID:** `138257835434`
- **Amazon MSK Cluster:**
  - Cluster ARN: `arn:aws:kafka:eu-west-3:138257835434:cluster/sic-streaming-msk/ff954d9e-7cba-437e-b180-83ad115d0996-2`
  - Apache Kafka: `3.8.x` | 2x `kafka.t3.small` | 10 GB EBS gp3 per broker
  - Bootstrap: `b-1.sicstreamingmsk.4io9um.c2.kafka.eu-west-3.amazonaws.com:9092,b-2...:9092`
- **Amazon RDS PostgreSQL:**
  - DB Identifier: `containerdb` | `db.t4g.micro` | 20 GB gp3 | PostgreSQL `18.3`
  - Endpoint: `containerdb.cjm6iag8qrwi.eu-west-3.rds.amazonaws.com:5432`
  - Parameter Group: `synthetic-pg18-cdc` (`rds.logical_replication=1`, `wal_level=logical`)
  - Publication: `debezium_publication` | Slot: `debezium_cdc_slot` (plugin: `pgoutput`)
- **Amazon ECS Cluster:** `synthetic-data-cluster`
  - Service 1: `synthetic-data-service` (Task Def: `synthetic-data-provider:4`, 0.5 vCPU, 1 GB)
  - Service 2: `sic-kafka-connect-service` (Task Def: `sic-kafka-connect:4`, 1.0 vCPU, 2 GB)
  - Scheduled Task Def: `synthetic-ingestion-extractor:5` (0.25 vCPU, 512 MB)
- **Amazon EventBridge Rule:** `scheduled-ingestion-extractor` (`rate(5 minutes)`)
- **Amazon S3 Bronze Bucket:** `sic-pipeline-storage` (SSE-S3 AES-256, Block Public Access active, No Lifecycle configuration)
- **AWS Secrets Manager:** `arn:aws:secretsmanager:eu-west-3:138257835434:secret:synthetic-data-provider/database-w5PpeT`

---

## 4. Confirmed Test Results & Evidence Limitations

1. **Test A (Direct Transaction Streaming & PostgreSQL Bypass):**
   - **Confirmed:** 55 transactions generated; published to MSK `transactions` (partition 0, offset 220). Query `SELECT count(*) FROM transactions` confirmed row count was exactly 32,353 before and after test.
   - **Limitation:** Does not prove zero inserts if an external consumer calls legacy API endpoints or if someone triggers historical batch scripts.
2. **Tests B & C (Users & Cards CDC via Debezium):**
   - **Confirmed:** Inserted 52 users and 52 cards into PostgreSQL. Debezium streamed changes to MSK. Consumed User `888803` on `users-1:1126` and Card `888821` on `cards-2:3337`.
   - **Limitation:** Only tested single-row `INSERT` operations. Did not test bulk batch updates, `DELETE` cascades, or schema migrations.
3. **Test D (Batch Reference Ingestion to Parquet):**
   - **Confirmed:** `BatchReferenceExtractor` queried 914 MCC rows and 1,433 Cities rows, writing valid Snappy Parquet files to `bronze/mcc/` and `bronze/cities/`.
   - **Limitation:** Over-execution cadence: EventBridge runs this every 5 minutes instead of daily.

---

## 5. Unresolved Questions for the Next Engineering Phase

1. **What is the authoritative source for REST API `/transactions` queries?**
   - Currently, `/api/v1/transactions` queries PostgreSQL (which only contains pre-existing baseline records). Should this endpoint be refactored to query AWS Athena / S3 Lakehouse, or should it be deprecated?
2. **How should late-arriving and update events be handled in S3 Bronze?**
   - Aiven S3 Sink writes raw append-only JSONL files containing Debezium CDC envelopes (`op: c, u, d`). Will the Silver layer utilize Apache Iceberg or dbt snapshots to reconstruct the current state of Users and Cards?
3. **What is the long-term data retention policy for MSK and S3?**
   - MSK has 10 GB EBS per broker and 7-day retention. At higher transaction rates, this will run out of space. When will EBS volume expansion or tiered storage be provisioned?

---

## 6. Top Five Architecture Risks

1. **S3 Lakehouse Pathing Defect (`RSK-01`):**
   Aiven S3 Sink config `file.name.template` writes to `s3://.../transactions/`, `users/`, and `cards/` at the bucket root, missing the `bronze/` directory.
2. **EventBridge Batch Cadence Over-Execution (`RSK-02`):**
   Rule `scheduled-ingestion-extractor` runs every 5 minutes (`rate(5 minutes)`), creating 288 duplicate Parquet files per table per day for static reference data.
3. **Silent Transaction Loss on Producer Failure (`RSK-03`):**
   `generator_service.py` publishes transactions without an `on_delivery` callback or dead-letter queue, dropping messages if broker delivery fails asynchronously.
4. **Fargate Regional vCPU Quota Bottleneck (`RSK-04`):**
   Hard regional limit of 2.0 vCPUs in `eu-west-3`. Services consume 1.5 vCPUs, leaving only 0.5 vCPU headroom. Rolling deployments trigger ECS task placement failures.
5. **PostgreSQL WAL Disk Exhaustion Risk (`RSK-05`):**
   If Kafka Connect stops for an extended period, PostgreSQL replication slot `debezium_cdc_slot` retains WAL logs on the 20 GB RDS disk, risking database outage.

---

## 7. Critical Assumptions That Must NOT Be Treated as Facts

1. **DO NOT assume all files in S3 Bronze are in `bronze/`:** Direct inspection proved that streaming transactions and CDC events are currently landing at the bucket root (`s3://.../users/`, `s3://.../cards/`, `s3://.../transactions/`).
2. **DO NOT assume the pipeline is "Exactly-Once":** Producer uses `acks: 1` and `enable.idempotence: False`. Network retries can produce duplicate messages on MSK. Consumer sinks commit offsets in micro-batches; crash recovery will re-process messages (At-Least-Once delivery).
3. **DO NOT assume EventBridge runs daily:** EventBridge rule `scheduled-ingestion-extractor` is actively running every 5 minutes (`rate(5 minutes)`).
4. **DO NOT assume MSK brokers have 50 GB storage:** AWS CLI inspection confirmed brokers have **10 GB EBS storage**, which limits maximum sustainable retention to ~15 TPS at 7 days.
5. **DO NOT assume `/api/v1/transactions` serves live streaming data:** It queries the PostgreSQL `transactions` table, which is deliberately bypassed by the live generator and contains only 32,353 static baseline rows.
