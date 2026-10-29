# Test Evidence & Verification Matrix
## Synthetic Financial Data Ingestion Pipeline (SIC Platform)

**Audit Date:** October 26, 2026  
**AWS Account ID:** `138257835434` | **Region:** `eu-west-3`  
**Classification:** Empirical Test Audit & Verification Assessment

---

## 1. Major Acceptance Claims Evaluation

| # | Major Acceptance Claim | Verification Status | Evaluated Test / Inspection | Primary Evidence Source | Residual Limitation / Uncovered Failure Mode |
|---|---|:---:|---|---|---|
| **CLM-1** | **Transactions are published to MSK topic `transactions`** | **VERIFIED** | `Test A` in `scripts/run_e2e_live_validation.py` | Confluent-Kafka consumer read transaction `tx-e2e-1792968099-001` at partition 0, offset 220 from `transactions`. | Only tested single-producer burst (55 messages); did not test network partition recovery or prolonged broker unavailability. |
| **CLM-2** | **Transaction records are NOT inserted into PostgreSQL** | **PARTIALLY VERIFIED** | `Test A Assertion` in `scripts/run_e2e_live_validation.py:154-167` | Query `SELECT count(*) FROM transactions` remained exactly `32,353` before and after streaming generation. | Verified for `generator_service.py` stream path. However, legacy `transactions` table remains populated with 32,353 rows, and `GET /api/v1/transactions` still queries it. |
| **CLM-3** | **Generated Users stream to MSK via Debezium CDC** | **VERIFIED** | `Test B` in `scripts/run_e2e_live_validation.py:207-234` | Consumer captured User `888803` on MSK topic `users` (Partition 1, Offset 1126) with full Debezium envelope. | Verified for `INSERT` operations. Schema evolution (adding/dropping columns on `public.users`) was not tested. |
| **CLM-4** | **Generated Cards stream to MSK via Debezium CDC** | **VERIFIED** | `Test C` in `scripts/run_e2e_live_validation.py:236-265` | Consumer captured Card `888821` on MSK topic `cards` (Partition 2, Offset 3337) with composite key preserved. | Tested single batch of 52 cards; did not test high transaction volume concurrent with card updates. |
| **CLM-5** | **Kafka events land in expected S3 prefixes (`bronze/`)** | **FAILED** | S3 object inspection: `aws s3 ls s3://sic-pipeline-storage/` | Streaming files land at `transactions/{p}/`, `users/{p}/`, `cards/{p}/` at **bucket root**, bypassing `bronze/`. | In `run_e2e_live_validation.py:316-318`, assertions checked `len(tx_keys) > 0` (satisfied by legacy Parquet files) but omitted assertions on `users` and `cards`! |
| **CLM-6** | **MCC and Cities snapshots are valid Parquet files** | **VERIFIED** | PyArrow inspection of live S3 Parquet files | MCC: 914 rows, Cities: 1,433 rows. Validated PyArrow schemas, Snappy compression, and metadata. | Idempotency appends new timestamped files rather than overwriting; downstream consumers must implement deduplication or latest-file resolution. |
| **CLM-7** | **Scheduled batch job is configured correctly** | **PARTIALLY VERIFIED** | EventBridge rule `scheduled-ingestion-extractor` | Task `synthetic-ingestion-extractor:5` exits with code 0 and logs success. | **Schedule Cadence Flaw:** Configured with `rate(5 minutes)` instead of daily schedule, causing 288 duplicate snapshots/day. |
| **CLM-8** | **Pipeline recovers from expected transient failures** | **PARTIALLY VERIFIED** | Source code inspection of `kafka_publisher.py` and `database.py` | `pool_pre_ping=True` handles DB reconnects; Kafka producer retries buffer full 3 times. | No DLQ or retry queue for asynchronous delivery failures. If `delivery.timeout.ms` expires, messages are silently dropped. |

---

## 2. Detailed Test-by-Test Audit

### Test A: Transaction Direct Streaming & PostgreSQL Exclusion
- **Script:** `scripts/run_e2e_live_validation.py:114-199`
- **Preconditions:** Amazon MSK cluster active, RDS PostgreSQL `containerdb` reachable.
- **Operation:**
  1. Record baseline count: `SELECT count(*) FROM transactions;` -> 32,353.
  2. Produce 55 synthetic transactions directly to MSK topic `transactions`.
  3. Flush producer (`timeout=15s`).
  4. Query: `SELECT count(*) FROM transactions WHERE transaction_id LIKE 'tx-e2e-%';`
  5. Subscribe consumer to `transactions` partitions `[0, 1, 2]` seeking `high - 60`.
- **Assertions Checked:**
  - `assert db_tx_found == 0`
  - `assert post_tx_count == initial_tx_count`
  - `assert first_tx_found == True`
- **Observed Result:** **PASS** (Zero transactions in PostgreSQL, test transaction `tx-e2e-1792968099-001` read from MSK partition 0 at offset 220).
- **Test Limitations:** Test ran synchronously in a single process. Did not simulate concurrent background database updates or broker failure during production.

---

### Tests B & C: PostgreSQL Insertion & Debezium CDC Capture
- **Script:** `scripts/run_e2e_live_validation.py:80-110, 201-266`
- **Preconditions:** Debezium connector `debezium-postgres-source` in `RUNNING` state, publication `debezium_publication` active.
- **Operation:**
  1. Insert 52 rows into `public.users` (IDs 888801..888852).
  2. Insert 52 rows into `public.cards` (user IDs 888801..888852, card_index 0).
  3. Poll MSK topic `users` across partitions `[0, 1, 2]`.
  4. Poll MSK topic `cards` across partitions `[0, 1, 2]`.
- **Assertions Checked:**
  - `assert user_cdc_captured == True`
  - `assert card_cdc_captured == True`
- **Observed Result:** **PASS** (User 888803 captured on `users-1:1126`, Card 888821 captured on `cards-2:3337`).
- **Test Limitations:** Test used clean synthetic IDs (`888800+`). Did not evaluate updates (`op="u"`) or table constraint violations.

---

### Test D: Scheduled Batch Reference Ingestion
- **Script:** `src/ingestion/batch_reference_extractor.py`, invoked in `run_e2e_live_validation.py:270-288`
- **Preconditions:** PostgreSQL RDS online, S3 bucket accessible via IAM role.
- **Operation:**
  1. Execute `extractor.run_all()`.
  2. Query `merchant_category_codes` and `city_reference`.
  3. Convert to PyArrow tables and persist as Snappy Parquet to S3.
- **Assertions Checked:**
  - `mcc_rows > 0` (Observed: 914 rows)
  - `city_rows > 0` (Observed: 1,433 rows)
  - S3 upload returns valid ETag and status.
- **Observed Result:** **PASS** (`mcc_snapshot_20261025_231056.parquet` and `cities_snapshot_20261025_231057.parquet` generated and verified).
- **Test Limitations:** Test validates single execution. Does not guard against partial-file upload failures (e.g. multipart failure recovery).

---

### Unit Test Suite Audit
- **Command:** `pytest tests/unit/ -v`
- **Execution Result:** **91 Passed, 0 Failed, 1 Skipped** (in 6.38 seconds).
- **Coverage Areas:**
  - API schemas and response structures (`test_api.py`, `test_transaction_api.py`)
  - Classification and geographic validations (`test_classification_and_geo.py`)
  - Configuration parsing and Secrets Manager fallbacks (`test_config.py`, `test_cloud_config.py`)
  - Batch reference extractor schema conformance (`test_batch_reference_extractor.py`)
  - Rate limiting and backpressure logic (`test_rate_controller.py`)
  - Referential integrity and scenario generation (`test_relationships.py`, `test_scenarios.py`)
- **Limitation:** All unit tests execute against mocked S3 clients and in-memory SQLite instances; they do not validate live network latencies or distributed race conditions.
