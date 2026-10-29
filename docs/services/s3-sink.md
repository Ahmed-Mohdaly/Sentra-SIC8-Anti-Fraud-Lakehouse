# S3 Sink Connector Service Specification
## SIC — Synthetic Financial Transaction Data Platform

**Service:** Distributed Apache Kafka Connect — S3 Sink Connector  
**Connector Name:** `aiven-s3-sink`  
**Connector Class:** `io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector`  
**Target Bucket:** `s3://sic-pipeline-storage`  
**Target Prefix:** `bronze/`  
**Status:** `RUNNING` (Active Ingestion)

---

## 1. Architectural Overview & Responsibility

The S3 Sink Connector is the terminal delivery mechanism for all event streams arriving on Amazon MSK. It runs inside the distributed Kafka Connect cluster on ECS Fargate (`sic-kafka-connect-service`) and continuously consumes records from the three core data streams:
1. `transactions` — Direct streaming events from the synthetic transaction generator.
2. `users` — PostgreSQL Change Data Capture (CDC) events via Debezium.
3. `cards` — PostgreSQL CDC events via Debezium.

```mermaid
flowchart LR
    subgraph MSK["Amazon MSK (Cluster: sic-streaming-msk)"]
        T1["Topic: transactions\n(3 Partitions)"]
        T2["Topic: users\n(3 Partitions)"]
        T3["Topic: cards\n(3 Partitions)"]
    end

    subgraph Connect["ECS Fargate: sic-kafka-connect-service"]
        Sink["Aiven S3 Sink Connector\n(Task 0)"]
    end

    subgraph S3["Amazon S3: sic-pipeline-storage"]
        P1["bronze/transactions/partition=X/*.json.gz"]
        P2["bronze/users/partition=X/*.json.gz"]
        P3["bronze/cards/partition=X/*.json.gz"]
    end

    T1 -->|Consume| Sink
    T2 -->|Consume| Sink
    T3 -->|Consume| Sink
    Sink -->|Micro-batch Flush (JSONL.GZ)| P1
    Sink -->|Micro-batch Flush (JSONL.GZ)| P2
    Sink -->|Micro-batch Flush (JSONL.GZ)| P3
```

---

## 2. Production Connector Configuration

The connector is configured dynamically via the Kafka Connect REST API:

```json
{
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
}
```

---

## 3. Critical Configuration Parameters & Engineering Rationale

| Configuration Key | Value | Technical Rationale & Impact |
|---|---|---|
| `topics` | `transactions,users,cards` | Consumes all three streaming and CDC topics within a single managed sink task, optimizing Fargate resource utilization. |
| `aws.s3.region` | `eu-west-3` | Targets the Paris AWS region where all data lakehouse resources reside. |
| `aws.sts.config.endpoint` | `https://sts.eu-west-3.amazonaws.com` | **CRITICAL FIX**: Explicitly directs the AWS SDK v2 within the Aiven connector to use the regional STS endpoint. Avoids `AWSSecurityTokenServiceException: Credential should be scoped to a valid region (SignatureDoesNotMatch)` which occurs when the global STS endpoint issues SigV4 tokens that mismatch the regional endpoint. |
| `format.output.type` | `jsonl` | Writes line-delimited JSON. Optimal for AWS Athena, Amazon EMR, AWS Glue, and Spark SQL query engines. |
| `format.output.compression` | `gzip` | Achieves ~85% storage cost reduction on S3 Bronze while remaining transparently queryable by Athena/Presto. |
| `file.name.template` | `bronze/{{topic}}/partition={{partition}}/{{start_offset:padding=true}}.json.gz` | Enforces Hive-compatible partition hierarchies. Leading-zero padded offsets ensure natural lexicographical and chronological ordering. |
| `file.max.records` | `1000` | Flushes a part file immediately once 1,000 records accumulate, preventing memory pressure in the 2 GB Fargate task. |
| `flush.timeout.ms` | `10000` | Time-based flush trigger (10 seconds). Guarantees near real-time data arrival in S3 even during low-throughput periods. |

---

## 4. Lakehouse Storage Layout

The connector delivers immutable, compressed event batches to S3 Bronze with the following directory structure:

```
s3://sic-pipeline-storage/
└── bronze/
    ├── transactions/
    │   ├── partition=0/
    │   │   ├── 00000000000000000000.json.gz
    │   │   └── 00000000000000000100.json.gz
    │   ├── partition=1/
    │   └── partition=2/
    ├── users/
    │   ├── partition=0/
    │   ├── partition=1/
    │   └── partition=2/
    └── cards/
        ├── partition=0/
        ├── partition=1/
        └── partition=2/
```

---

## 5. Live Operational Verification

The connector was validated under live ingestion loads during end-to-end testing:
- **Connector Health:** Verified via `GET http://172.31.10.236:8083/connectors/aiven-s3-sink/status`. Returned state `RUNNING`, with task 0 in state `RUNNING`.
- **S3 Object Creation:** Successfully flushed and verified objects under `bronze/transactions/`, `bronze/users/`, and `bronze/cards/`.
- **Data Integrity:** Inspected gzip contents using `aws s3 cp` and `zcat`, validating valid JSONL records matching generated transactions and Debezium change payloads.
