# Architectural Analysis: Streaming vs. Batch Ingestion
## SIC — Synthetic Financial Transaction Data Platform

**Classification:** Architectural Trade-off Evaluation & Decision Record  
**Target Environment:** AWS `eu-west-3` (Paris)  

---

## 1. Executive Summary

A core requirement of modern financial data architectures is matching ingestion mechanics to the velocity, volatility, and query patterns of distinct data domains. The SIC platform deliberately rejects a one-size-fits-all paradigm, adopting a **dual-pattern architectural strategy**:
- **Continuous Event Streaming & CDC** for high-velocity transactional events and operational entity lifecycle mutations.
- **Micro-Batch Snapshot Ingestion** for stable, low-volatility reference datasets (merchant categories and geographic centroids).

---

## 2. Ingestion Pattern Trade-off Matrix

| Evaluation Dimension | Transaction Streaming (MSK + S3 Sink) | Entity Change Data Capture (Debezium + MSK) | Reference Data Batch (EventBridge + ECS) |
|---|---|---|---|
| **Primary Domain** | Synthetic Transactions (`transactions`) | User & Card Registrations (`users`, `cards`) | Merchant Categories (`mcc`), Cities (`cities`) |
| **Ingestion Latency** | Sub-second (Producer to Broker: <50ms; S3 buffer: 5–15s) | Low-latency asynchronous (<1s from RDS commit) | Periodic scheduled snapshots (5 min to 24 hrs) |
| **Source System Impact** | **Zero RDS write or read overhead** | Minimal WAL read overhead via native `pgoutput` | Periodic lightweight indexed query scan |
| **Failure Recovery** | Consumer offset replay from MSK partitions | Replication slot LSN position tracking | Idempotent whole-snapshot retry on next run |
| **Backpressure Handling** | Kafka distributed log buffer absorbs producer spikes | Postgres WAL retains unconsumed segments safely | Isolated ephemeral task execution |
| **Storage Format & Layout** | Partitioned Gzip JSONL (`bronze/transactions/`) | Partitioned Gzip JSONL (`bronze/users/`, `cards/`) | Columnar Snappy Parquet (`bronze/mcc/`, `cities/`) |
| **Resource Cost Profile** | Fixed compute (MSK + Kafka Connect Fargate task) | Shared with Kafka Connect distributed cluster | Ephemeral compute (runs for ~10 seconds and terminates) |

---

## 3. Deep Dive: Streaming Ingestion Rationale

### 3.1. Why Direct-to-Kafka for Transactions?
In traditional architectures, transactions are initially inserted into an operational database (OLTP) and later extracted via batch ETL or CDC. For synthetic financial platforms generating hundreds or thousands of transactions per second, this introduces severe anti-patterns:
1. **Relational IOPS Saturation:** Inserting millions of synthetic transaction rows rapidly exhausts RDS storage burst IOPS and triggers database autoscaling costs.
2. **Table Bloat & VACUUM Overhead:** Constantly inserting and archiving high-volume transactions severely stresses PostgreSQL's autovacuum subsystem, degrading operational query latency.
3. **Decoupled Failure Isolation:** A spike in synthetic transaction generation cannot degrade operational CRUD operations for users and cards.

**Architectural Decision:** Synthetic transactions bypass PostgreSQL completely. The application directly writes to MSK with `acks=all` or `acks=1`, achieving sub-10ms producer latencies while MSK seamlessly scales across broker partitions.

### 3.2. Why CDC for Users and Cards?
Users and credit cards represent stateful entities where transactions must reference persistent identifiers (`user_id`, `card_index`).
1. **Source of Truth Integrity:** The relational database enforces foreign key relationships, uniqueness constraints, and data validation.
2. **Auditability & Historic Lineage:** Capturing changes via Debezium CDC preserves the full history of entity state transitions (creates, credit limit updates, card expirations) with precise timestamps (`ts_ms`) and transaction IDs (`txId`), enabling event-sourced downstream analytics.
3. **Non-Invasive Extensibility:** Downstream microservices can subscribe to the `users` and `cards` topics independently of the application database.

---

## 4. Deep Dive: Batch Ingestion Rationale

### 4.1. Why Batch for Reference Datasets (MCC and Cities)?
Merchant Category Codes (914 standard ISO codes) and City Geographic Centroids (1,433 reference locations) exhibit zero or negligible intraday mutation.
1. **Avoid Over-Engineering:** Streaming or CDC on static lookup tables adds operational overhead without analytical value.
2. **Direct Columnar Optimization:** Downstream analytics (e.g. AWS Athena, Amazon EMR, DuckDB) heavily benefit from reading pre-compressed columnar Parquet files with embedded statistics (min/max dictionaries). Batch extraction directly emits Snappy Parquet tables via `pyarrow`, bypassing intermediate Kafka deserialization.
3. **Ephemeral Cost Optimization:** The extractor executes as an on-demand Fargate container taking ~10 seconds per run, incurring virtually zero AWS cost.

---

## 5. Architectural Verdict & Pattern Coexistence

The coexistence of Streaming, CDC, and Batch forms a harmonious lakehouse ingestion layer:

```
+-------------------------------------------------------------------------+
|                          AMAZON S3 BRONZE LAYER                         |
+------------------------------------+------------------------------------+
|         STREAMING & CDC            |               BATCH                |
|   (Near-Real-Time Event Streams)   |       (Full State Snapshots)       |
+------------------------------------+------------------------------------+
| - bronze/transactions/             | - bronze/mcc/                      |
|   Format: .json.gz (append-only)   |   Format: .parquet (columnar)      |
|   Partition: topic/partition/      |   Partition: snapshot_date=YYYY-MM |
|                                    |                                    |
| - bronze/users/ & bronze/cards/    | - bronze/cities/                   |
|   Format: .json.gz (Debezium CDC)  |   Format: .parquet (columnar)      |
|   Partition: topic/partition/      |   Partition: snapshot_date=YYYY-MM |
+------------------------------------+------------------------------------+
```
