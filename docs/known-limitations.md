# SIC Platform — Known Limitations & Operational Considerations

## 1. At-Least-Once Delivery Semantics & Potential Duplicates

The streaming ingestion pipeline guarantees **at-least-once** delivery across its components:
- **Kafka Publisher:** Configured with bounded retries on transient `BufferError` and connection interruptions. In the event of network partition or broker failover, retried messages may produce duplicate entries in the `transactions` topic.
- **Aiven S3 Sink Connector:** Flushes batches of records to Amazon S3 Bronze based on record thresholds (`file.max.records = 50`) and time intervals (`flush.timeout.ms = 5000`). If a connector worker restarts abruptly before committing Kafka consumer offsets, records from the uncommitted batch will be reprocessed and written to a subsequent S3 object.
- **Deduplication Strategy:** The platform intentionally does not incorporate complex distributed deduplication or transactional state stores. Downstream Lakehouse transformation layers (Silver/Gold ETL) are expected to deduplicate events using `event_id` or `data.transaction_id`.

---

## 2. Legacy PostgreSQL Transactions Table vs. Streaming Stream

A critical architectural distinction exists between historical baseline transactions and live synthetic transactions:
- **Historical Baseline Records:** The PostgreSQL RDS database (`containerdb`) contains ~10,000 historical baseline transaction records imported during initial project bootstrapping.
- **Live Streaming Transactions:** Newly generated synthetic transactions are **never inserted into PostgreSQL**. They stream directly from the generator service to the MSK `transactions` topic and land in S3 Bronze.
- **REST API Endpoint (`GET /api/v1/transactions`):** This endpoint queries the legacy PostgreSQL table and returns historical baseline records only. It does not query MSK or the S3 Lakehouse. Callers seeking real-time streaming transactions must consume the MSK `transactions` topic or query the `bronze/transactions/` Lakehouse partition.

---

## 3. PostgreSQL Logical Replication & WAL Retention Risk

The Debezium PostgreSQL Source Connector relies on PostgreSQL logical decoding:
- **Replication Slot:** Configured using logical replication slot `debezium_slot` and publication `dbz_publication`.
- **WAL Accumulation Risk:** When a replication slot is active, PostgreSQL will not recycle WAL (Write-Ahead Log) segments that have not yet been consumed and acknowledged by Debezium.
- **Storage Impact:** If the Kafka Connect service or the `debezium-postgres-source` connector experiences extended downtime or task failure while new user/card records continue to be written to PostgreSQL, WAL segments will accumulate in `pg_wal`. On a standard RDS instance with 20 GB of storage, this can exhaust available disk space and transition RDS into a `STORAGE_FULL` state.
- **Recommended Monitoring:** Set CloudWatch alarms on `FreeStorageSpace` (< 5 GB) and RDS PostgreSQL replication slot lag (`OldestReplicationSlotLagInBytes`). If connector downtime is planned or prolonged, drop the replication slot or pause upstream writes.

---

## 4. Fargate vCPU Regional Quotas & Task Scheduling

In AWS Region `eu-west-3`, the account's Fargate vCPU service quota (`L-720C7AEB`) is set to 2.0 vCPUs:
- Two long-running ECS services run in `sic-streaming-cluster`:
  - `synthetic-data-service`: 1 task × 1.0 vCPU = 1.0 vCPU
  - `sic-kafka-connect-service`: 1 task × 1.0 vCPU = 1.0 vCPU
- **Capacity Constraint:** The two long-running tasks fully consume the 2.0 vCPU quota. Launching additional ad-hoc tasks (such as `sic-admin-runner` or the EventBridge scheduled batch extractor task) will fail with a Fargate quota limit error unless:
  - An AWS Service Quota increase is granted for Fargate vCPUs, OR
  - Task CPU allocations are resized (e.g. 0.5 vCPU per service task), OR
  - The `synthetic-data-service` is scaled down to 0 tasks prior to executing ad-hoc validation or extraction tasks.

---

## 5. Live Verification Constraints & AWS Session Expiry

- **AWS Credentials:** Running live validation against MSK and Kafka Connect requires active AWS credentials with access to the `138257835434` account. If the AWS STS session token expires, live API operations (`aws events`, `aws s3`, `aws ecs`) cannot proceed until refreshed.
- **Network Boundaries:** MSK plaintext brokers (`9092`), Kafka Connect REST API (`8083`), and PostgreSQL RDS (`5432`) reside inside VPC private subnets. Live validation and connector configuration updates must be executed from an ECS container or runner located within the VPC (`subnet-0a6237f02d0cf3fdf`).
