# Cloud Pipeline Functional Test & Ingestion Verification Report

**Execution Timestamp:** `2026-10-29T14:08:18.764573+00:00`  
**Test Status:** `❌ FAIL`  
**Target Duration:** `300.0s (5.0 mins)` | **Target Rate:** `1000.0 TPS`

---

## 1. Executive Summary & Throughput Performance

| Metric | Target | Observed / Actual | Variance / Health |
| :--- | :--- | :--- | :--- |
| **Pipeline Overall Status** | `PASS` | **`FAIL`** | `100% Assertion Compliance` |
| **Test Duration** | `300.0s` | **`300.0s`** | `Complete window` |
| **Target Rate (Configured)** | `1000.0 TPS` | **`1000.0 TPS`** | `Configured on ECS` |
| **Actual Generated Throughput** | `~700-1000 TPS` | **`700.1 TPS`** | `Sustained Cloud Emission` |
| **Actual Delivered Throughput** | `~700-1000 TPS` | **`700.1 TPS`** | `MSK Ingestion Rate` |
| **Total Transactions Generated** | — | **`210,030`** | `Events Produced` |
| **Total Transactions Delivered** | — | **`210,030`** | `Delivered to MSK` |
| **Delivery Failure / Drops** | `0` | **`0`** | `Zero Loss (100.0% Success)` |
| **Delivery Success Rate** | `100.0%` | **`100.0%`** | `Acks=all Idempotent Delivery` |

---

## 2. Ingestion Pathways End-to-End Verification Matrix

| Pathway # | Pipeline Pathway | Source System | Transport / Broker | Destination | Verification Status |
| :---: | :--- | :--- | :--- | :--- | :---: |
| **1** | **Real-Time Transaction Stream** | FastAPI Generator (ECS) | Amazon MSK (`transactions`) | S3 Bronze (`.json.gz`) via Aiven Sink | **`FAIL`** |
| **2** | **PostgreSQL CDC (Users & Cards)** | Amazon RDS PostgreSQL | Debezium `pgoutput` -> MSK | S3 Bronze (`users/`, `cards/`) | **`PASS`** |
| **3** | **Batch Reference Tables** | PostgreSQL Reference DB | PyArrow Parquet Extractor | S3 Bronze (`mcc/`, `cities/`) | **`PASS`** |
| **4** | **Incremental Keyset Ingestion** | FastAPI REST API Plane | Ingestion Extractor & DynamoDB | S3 Bronze Parquet & Watermarks | **`PASS`** |
| **5** | **Real-Time ML Fraud Inference** | MSK `transactions` topic | LightGBM + ElastiCache Redis | MSK `fraud_alerts` topic | **`FAIL`** |
| **6** | **Lakehouse Orchestration** | S3 Bronze Lakehouse | Databricks Job `815854574980923` | Delta Lake Silver & Gold Marts | **`CONFIGURED_AND_READY`** |

---

## 3. Data Integrity & Quality Assertions ("الداتا داخله صح وطالعه صح")

| Assertion Identifier | Technical Rule / Guarantee | Observed Result | Status |
| :--- | :--- | :--- | :---: |
| **AST-01: Zero PostgreSQL Leakage** | Synthetic transactions strictly bypass PostgreSQL `transactions` table. | Delta DB Tx = `0` rows | **`PASS`** |
| **AST-02: S3 Bronze Schema Validation** | S3 `.json.gz` files parse successfully and match canonical schema. | Sampled `5` files; all valid | **`FAIL`** |
| **AST-03: Zero Message Drops** | MSK Kafka publisher delivers 100% of generated records without failure. | Failed deliveries = `0` | **`PASS`** |
| **AST-04: CDC Prerequisite Generation** | New user/card prerequisites inserted into RDS and captured by CDC. | Users: `+7348`, Cards: `+22143` | **`PASS`** |
| **AST-05: Real-Time ML Inference SLA** | P95 fraud scoring latency must stay strictly below 10ms. | P95 = **`58.38ms`** (< 10.0ms target) | **`FAIL`** |

---

## 4. Real-Time ML Fraud Consumer Performance

- **Observations Sampled:** `100` events
- **P50 Latency:** `6.62ms`
- **P90 Latency:** `7.46ms`
- **P95 Latency:** **`58.38ms`** *(Target SLA: < 10.0ms)*
- **P99 Latency:** `67.59ms`

### Sample Live Fraud Alerts Captured:
```text
🚨 FRAUD DETECTED! User=722, Card=1, Amount=$200.17 | Prob=0.952 | Reasons=['impossible_travel_speed_1089301kmh', 'credit_limit_exhaustion'] | Latency=6.27ms
🚨 FRAUD DETECTED! User=91, Card=1, Amount=$225.68 | Prob=0.361 | Reasons=[] | Latency=6.25ms
🚨 FRAUD DETECTED! User=891692, Card=0, Amount=$44.10 | Prob=0.333 | Reasons=['impossible_travel_speed_1345482kmh'] | Latency=6.51ms
```

---

## 5. Sample Validated S3 Bronze Payload

```json
{
  "schema_version": 1,
  "event_id": "9e935c3f-cc31-4923-b4ba-2d1804c19ae4",
  "event_type": "transaction.created",
  "run_id": "run-0-1793099780",
  "source_system": "POS_TERMINAL",
  "scenario": "normal_spend",
  "payload": {
    "Zip": "10001.0",
    "User": 1203,
    "Errors?": null,
    "Amount": "$109.36",
    "Merchant Name": "554100009876543210",
    "Is Fraud?": "No",
    "Use Chip": "Chip Transaction",
    "Time": "14:06",
    "Merchant Latitude": 40.7128,
    "MCC": 5541,
    "Merchant Longitude": -74.006,
    "Card": 1,
    "Month": 10,
    "Year": 2026,
    "Merchant State": "NY",
    "Merchant City": "New York",
    "Day": 29,
    "Transaction Entry Method": "contactless_chip"
  },
  "event_timestamp": "2026-10-29T14:06:50.840747+00:00"
}
```

---

## 6. Databricks Lakehouse Workflow Status

- **Job ID:** `815854574980923`
- **Last Run State:** `SUCCESS`
- **Monitoring URL:** [https://dbc-d569636e-57c6.cloud.databricks.com/?o=7474655143693032#job/815854574980923/run/652390811526961](https://dbc-d569636e-57c6.cloud.databricks.com/?o=7474655143693032#job/815854574980923/run/652390811526961)

