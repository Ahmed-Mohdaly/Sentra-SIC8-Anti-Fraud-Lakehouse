# Product Requirements Document (PRD)
## Dead Letter Queue (DLQ) & Poison-Pill Isolation Architecture
### Platform: Synthetic Financial Transaction Data Ingestion Platform (SIC)

---

**Document Title:** Dead Letter Queue (DLQ) & Streaming Fault Tolerance Specification  
**Version:** 1.0.0  
**Target Environment:** AWS `eu-west-3` (Amazon MSK, ECS Fargate, Kafka Connect, Amazon S3)  
**Target Bucket:** `s3://sic-pipeline-storage/`  
**Status:** Architecture Specification & Implementation Baseline  
**Audience:** Platform Engineers, Data Engineers, SREs, Evaluation Panel  

---

## 1. Executive Summary & Problem Statement

### 1.1. Context
The Synthetic Financial Transaction Data Platform (SIC) streams high-velocity financial events (`transactions`) and relational CDC entities (`users`, `cards`) through Amazon MSK into an Amazon S3 Bronze Lakehouse via the **Aiven S3 Sink Connector** running on AWS ECS Fargate.

### 1.2. The Problem: The "Poison-Pill" Vulnerability
In the baseline configuration:
1. **Pipeline Freezing (`errors.tolerance = none`):** If a single corrupted message (e.g. malformed JSON, truncated payload, unexpected schema drift, or non-deserializable byte sequence) enters any topic, Kafka Connect halts the entire worker task (`state: FAILED`). 
2. **Cascading Downtime:** Because all three topics (`transactions`, `users`, `cards`) are consumed within a single sink task, a single corrupted user CDC event immediately halts transaction ingestion into S3 Bronze.
3. **Silent Drop Risk at Producer:** If the generator publishes a message rejected by Kafka brokers during high-load backpressure, the lack of an operational dead-letter mechanism risks permanent message loss.

### 1.3. Objective
Implement an end-to-end, multi-tier **Dead Letter Queue (DLQ)** framework that:
- Guarantees **zero pipeline interruption** upon encountering corrupt records.
- Captures full diagnostic context (original topic, partition, offset, exception class, error message, and stack trace) as standard metadata headers.
- Automatically persists dead-letter events into both a dedicated Kafka DLQ topic (`sic_sink_dlq`) and cold S3 storage (`s3://sic-pipeline-storage/bronze/dlq/`) for auditing, forensics, and reprocessing.

---

## 2. Multi-Tier DLQ Architecture

```mermaid
flowchart TD
    subgraph Producers ["Event Ingress"]
        Gen["Synthetic Generator / API"]
        Debezium["Debezium PostgreSQL CDC"]
    end

    subgraph MSK ["Amazon MSK Streaming Tier (Cluster: sic-streaming-msk)"]
        T1["Topic: transactions"]
        T2["Topic: users"]
        T3["Topic: cards"]
        DLQ_Topic["Topic: sic_sink_dlq<br/>(Partitions: 2, RF: 2, Retention: 14 Days)"]
    end

    subgraph KafkaConnect ["ECS Fargate: sic-kafka-connect-service"]
        SinkTask["Aiven S3 Sink Connector (Task 0)<br/><code>errors.tolerance = all</code><br/><code>errors.deadletterqueue.topic.name = sic_sink_dlq</code>"]
    end

    subgraph S3Storage ["Amazon S3 Lakehouse (sic-pipeline-storage)"]
        S3Bronze["s3://sic-pipeline-storage/bronze/<br/>{transactions, users, cards}"]
        S3DLQ["s3://sic-pipeline-storage/bronze/dlq/<br/>date=YYYY-MM-DD/*.jsonl.gz"]
    end

    subgraph Observability ["Observability & Governance"]
        CW["Amazon CloudWatch Alarms<br/>DLQ Ingress Rate > 0"]
        Ops["Data Ops / SRE Review & Replay"]
    end

    Gen -->|Publish Valid| T1
    Debezium -->|Publish CDC| T2
    Debezium -->|Publish CDC| T3

    T1 -->|Consume| SinkTask
    T2 -->|Consume| SinkTask
    T3 -->|Consume| SinkTask

    SinkTask -->|Happy Path (Flush JSONL.GZ)| S3Bronze
    SinkTask -->|Poison Pill / Deserialization Failure| DLQ_Topic

    DLQ_Topic -->|Sink / Drain Task| S3DLQ
    DLQ_Topic -->|Metrics| CW
    CW -->|Alert| Ops
    S3DLQ -->|Manual / Automated Remediation| Ops
```

---

## 3. Detailed Technical Requirements

### 3.1. Kafka Connect S3 Sink DLQ Specification

The `aiven-s3-sink` connector configuration must be updated via the Kafka Connect REST API (`PUT /connectors/aiven-s3-sink/config`) to enforce the following properties:

| Configuration Property | Target Value | Technical Purpose |
|---|---|---|
| `errors.tolerance` | `all` | Prevents task failure when bad records arrive. Instead of halting, bad records are routed to DLQ and processing continues. |
| `errors.deadletterqueue.topic.name` | `sic_sink_dlq` | Dedicated MSK topic receiving quarantined records. |
| `errors.deadletterqueue.topic.replication.factor` | `2` | Matches MSK broker redundancy (2 brokers across 2 Availability Zones). |
| `errors.deadletterqueue.context.headers.enable` | `true` | Injects diagnostic headers into the DLQ message (error message, exception class, source topic/partition/offset). |
| `errors.log.enable` | `true` | Emits structured error log lines into stdout (captured by CloudWatch Logs). |
| `errors.log.include.messages` | `true` | Includes record keys, values, and stack traces in CloudWatch logs for debugging. |

### 3.2. Diagnostic Context Headers Contract

Every message routed to `sic_sink_dlq` must contain the following Kafka Record Headers:

```text
__connect.errors.topic                = "transactions"
__connect.errors.partition            = 1
__connect.errors.offset               = 14092
__connect.errors.connector.name       = "aiven-s3-sink"
__connect.errors.task.id              = 0
__connect.errors.stage                = "VALUE_CONVERTER" | "KEY_CONVERTER" | "TRANSFORMATION"
__connect.errors.class.name           = "org.apache.kafka.connect.json.JsonConverter"
__connect.errors.exception.class.name = "org.apache.kafka.connect.errors.DataException"
__connect.errors.exception.message    = "Converting byte[] to Kafka Connect data failed due to serialization error"
__connect.errors.exception.stacktrace = "...Full Java Stacktrace..."
```

### 3.3. Amazon S3 Cold DLQ Storage (`bronze/dlq/`)

To guarantee long-term retention and auditability without consuming MSK EBS disk quota:
1. **Path Hierarchy:**  
   `s3://sic-pipeline-storage/bronze/dlq/year={YYYY}/month={MM}/day={DD}/dlq_batch_{timestamp}.jsonl.gz`
2. **Payload Structure:**
   Each line in the compressed JSONL file must preserve:
   - `raw_payload`: Base64-encoded or raw string of the failed message.
   - `source_topic`: Original Kafka topic.
   - `source_partition`: Original partition number.
   - `source_offset`: Original offset.
   - `error_context`: Extracted error message and exception class.
   - `quarantined_at`: ISO 8601 UTC timestamp.

---

## 4. Operational Runbook & Standard Operating Procedures (SOPs)

### SOP-DLQ-01: Detecting & Inspecting DLQ Records

When a CloudWatch Alarm fires indicating messages in `sic_sink_dlq`:

1. **Check DLQ Consumer Group & Offsets:**
   ```bash
   # Inspect message count in DLQ topic
   kafka-run-class kafka.tools.GetOffsetShell \
     --bootstrap-server $MSK_BOOTSTRAP \
     --topic sic_sink_dlq --time -1
   ```

2. **Inspect Diagnostic Headers of Quarantined Records:**
   ```bash
   # Read DLQ record with headers using Python or kafkacat / kcat
   kcat -b $MSK_BOOTSTRAP -C -t sic_sink_dlq -C -e -J -o -5 | jq .
   ```

### SOP-DLQ-02: Replaying Remediated Records

Once the root cause (e.g. invalid date format or schema mismatch) is resolved:
1. Extract the raw messages from `s3://sic-pipeline-storage/bronze/dlq/`.
2. Apply the schema fix script (`scripts/remediate_dlq_records.py`).
3. Re-publish the cleansed records to the target MSK topic (`transactions`, `users`, or `cards`).
4. Verify acknowledgment and downstream Bronze lakehouse landing.

---

## 5. Acceptance Criteria & Test Validation Matrix

| ID | Test Scenario | Execution Method | Expected Result | Pass / Fail |
|---|---|---|---|---|
| **AC-DLQ-01** | **Poison-Pill Ingestion Test** | Publish non-JSON string (`"{bad_json: missing_quotes"`) to `transactions` topic. | S3 Sink task stays in `RUNNING` state (no crash). Message lands in `sic_sink_dlq` within 5 seconds. | [ ] |
| **AC-DLQ-02** | **Context Header Verification** | Inspect record in `sic_sink_dlq`. | Headers include original topic (`transactions`), offset, and `JsonConverter` exception details. | [ ] |
| **AC-DLQ-03** | **Pipeline Non-Blocking Verification** | Publish 10 valid transactions, 1 corrupted transaction, and 10 valid transactions. | 20 valid transactions land in `s3://sic-pipeline-storage/bronze/transactions/`. 1 bad record lands in `sic_sink_dlq`. | [ ] |
| **AC-DLQ-04** | **S3 DLQ Archival** | Run S3 DLQ archival task. | Quarantined records land in `s3://sic-pipeline-storage/bronze/dlq/` with full metadata. | [ ] |

---

## 6. Security, Compliance, and Data Retention

1. **Non-Exposure of Sensitive Tokens:**
   - Stack traces in DLQ headers must not log unmasked PAN (Primary Account Numbers) or plaintext CVVs.
   - S3 DLQ prefix is encrypted with SSE-S3 AES-256 and subject to the bucket's strict `BlockPublicAccess` policy.
2. **Topic Retention:**
   - `sic_sink_dlq` retention policy is configured to **14 days** (`retention.ms=1209600000`).
   - S3 DLQ files follow S3 Standard storage with lifecycle transition to Glacier Flexible Retrieval after 90 days.
