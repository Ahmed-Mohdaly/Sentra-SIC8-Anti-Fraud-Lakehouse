# Recommended Actions & Remediation Plan
## Synthetic Financial Data Ingestion Pipeline (SIC Platform)

**Audit Date:** October 26, 2026  
**AWS Account ID:** `138257835434` | **Region:** `eu-west-3`  
**Classification:** Prioritized Architectural Remediation Roadmap

---

## 1. Action Priority Matrix

```mermaid
quadrantChart
    title Action Prioritization (Effort vs Business Impact)
    x-axis Low Effort --> High Effort
    y-axis Low Impact --> High Impact
    quadrant-1 High Impact / High Effort (Strategic)
    quadrant-2 High Impact / Low Effort (Quick Wins / P0)
    quadrant-3 Low Impact / Low Effort (Housekeeping)
    quadrant-4 Low Impact / High Effort (Reconsider)
    "Fix S3 Sink Template (RSK-01)": [0.15, 0.95]
    "Throttle EventBridge Schedule (RSK-02)": [0.10, 0.85]
    "Add Producer on_delivery Callback (RSK-03)": [0.35, 0.80]
    "Request Fargate vCPU Quota (RSK-04)": [0.10, 0.75]
    "Add CloudWatch Alarms (RSK-07)": [0.30, 0.70]
    "Deploy S3 Lifecycle Policy (RSK-09)": [0.15, 0.50]
    "Deploy AWS VPC Endpoints (RSK-08)": [0.65, 0.65]
    "Adopt Iceberg / Delta Lake Format (RSK-10)": [0.85, 0.75]
    "Refactor REST API /transactions (RSK-06)": [0.50, 0.60]
```

---

## 2. Priority 0 (P0) — Immediate Correctness & Operational Fixes

### Action 1.1: Fix Aiven S3 Sink Output Path Template (`RSK-01`)
- **Status:** **REQUIRED FIX (IMMEDIATE)**
- **Problem:** Streaming and CDC JSONL files are landing at the bucket root (`s3://.../users/0/`, `cards/0/`, `transactions/0/`) rather than within the lakehouse `bronze/` partition hierarchy.
- **Root Cause:** `"file.name.template": "{{topic}}/{{partition}}/{{start_offset:padding=true}}.json.gz"` overrides `"file.name.prefix": "bronze/"`.
- **Concrete Remediation:**
  Update the connector configuration via the Kafka Connect REST API:
  ```json
  PUT http://172.31.10.236:8083/connectors/aiven-s3-sink/config
  {
    ...
    "file.name.template": "bronze/{{topic}}/partition={{partition}}/{{start_offset:padding=true}}.json.gz"
  }
  ```
- **Verification:** Confirm new objects land under `bronze/transactions/partition=X/`, `bronze/users/partition=X/`, and `bronze/cards/partition=X/`.

### Action 1.2: Throttle EventBridge Batch Cadence to Daily (`RSK-02`)
- **Status:** **REQUIRED FIX (IMMEDIATE)**
- **Problem:** EventBridge rule `scheduled-ingestion-extractor` runs every 5 minutes (`rate(5 minutes)`), creating 288 duplicate Parquet files per dataset per day for static dimension tables.
- **Concrete Remediation:**
  Update rule schedule expression via AWS CLI:
  ```bash
  aws events put-rule \
    --name scheduled-ingestion-extractor \
    --schedule-expression "cron(0 2 * * ? *)" \
    --region eu-west-3
  ```
- **Impact:** Reduces extractor executions from 8,640 runs/month to 30 runs/month, eliminating small-file accumulation and saving Fargate compute costs.

---

## 3. Priority 1 (P1) — Reliability & Observability Hardening

### Action 2.1: Implement Producer Delivery Callback & Replay Buffer (`RSK-03`)
- **Status:** **REQUIRED FIX**
- **Problem:** `generator_service.py` publishes transactions without an `on_delivery` callback. Broker delivery failures drop messages without recovery.
- **Concrete Remediation:**
  In `generator_service.py:_generation_loop()`:
  1. Define a thread-safe callback:
     ```python
     def on_tx_delivery(err, msg):
         if err is not None:
             self.dead_letter_queue.append((msg.key(), msg.value(), str(err)))
     ```
  2. Pass `on_delivery=on_tx_delivery` to `self.publisher.publish()`.
  3. Drain `dead_letter_queue` to a dedicated local retry log or S3 quarantine bucket.

### Action 2.2: Increase Fargate Regional vCPU Quota (`RSK-04`)
- **Status:** **REQUIRED FIX (OPERATIONAL)**
- **Problem:** Hard limit of 2.0 vCPUs in `eu-west-3` leaves only 0.5 vCPU headroom with running services (1.5 vCPUs consumed). ECS task launches fail during rolling updates or concurrent batch tasks.
- **Concrete Remediation:**
  Submit an AWS Service Quotas request for `Fargate vCPU count` in `eu-west-3`, requesting an increase from 2 to 8 or 16 vCPUs.

### Action 2.3: Provision Critical CloudWatch Alarms (`RSK-07`, `RSK-05`)
- **Status:** **REQUIRED FIX**
- **Concrete Remediation:**
  Deploy CloudWatch alarms with SNS email/Slack paging for:
  1. **MSK Cluster Health:** `AWS/Kafka UnderReplicatedPartitions > 0` (Period: 1 min, Eval: 1).
  2. **RDS Storage & Slot Lag:** `AWS/RDS FreeStorageSpace < 5000000000` (5 GB threshold).
  3. **ECS Service Stability:** `ECS/ContainerInsights RunningTaskCount < DesiredTaskCount` on `sic-kafka-connect-service` and `synthetic-data-service`.

---

## 4. Priority 2 (P2) — Performance, Security & Architectural Evolution

### Action 3.1: Deploy AWS VPC Endpoints to Eliminate Public IPs (`RSK-08`)
- **Status:** **ARCHITECTURAL IMPROVEMENT**
- **Problem:** ECS Fargate tasks currently require `AssignPublicIp: ENABLED` because the VPC lacks PrivateLink endpoints to reach ECR, Secrets Manager, S3, and CloudWatch.
- **Concrete Remediation:**
  1. Create Gateway VPC Endpoint for `com.amazonaws.eu-west-3.s3` (Free).
  2. Create Interface VPC Endpoints for:
     - `com.amazonaws.eu-west-3.ecr.api`
     - `com.amazonaws.eu-west-3.ecr.dkr`
     - `com.amazonaws.eu-west-3.secretsmanager`
     - `com.amazonaws.eu-west-3.logs`
  3. Update ECS Service network configuration to `AssignPublicIp: DISABLED`.

### Action 3.2: Configure S3 Bronze Lifecycle Policy (`RSK-09`)
- **Status:** **MAINTAINABILITY / COST OPTIMIZATION**
- **Concrete Remediation:**
  Apply S3 Lifecycle configuration:
  - Transition objects under prefix `bronze/` to `STANDARD_IA` after 30 days.
  - Transition objects to `GLACIER_IR` (Instant Retrieval) after 90 days.
  - Expire incomplete multipart uploads after 7 days.

### Action 3.3: Transition Lakehouse Storage to Apache Iceberg (`RSK-10`)
- **Status:** **LONG-TERM STRATEGIC ENHANCEMENT**
- **Rationale:** Replacing raw JSONL/Parquet files with Apache Iceberg or Delta Lake table formats provides ACID transactions, automated file compaction, time travel, and eliminates the small-file problem during high-frequency micro-batch ingestion.
