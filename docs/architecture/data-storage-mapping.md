# Data Storage Mapping & Lineage Inventory

**Author:** Senior Data Engineer  
**Workspace:** `/home/syd/Downloads/archive`  
**Classification:** Enterprise Data Engineering Architecture Specification

---

## 1. Master Dataset Lineage & Storage Matrix

| Dataset | Source | Source Location | Reader | Processing | Destination | Format | Status |
|---|---|---|---|---|---|---|---|
| **Users Reference Dataset** | IBM / Open Demographics Baseline | `/app/data/sd254_users.csv` & root workspace | `BaselineLoader` & `ingest_reference_csvs.py` | Idempotent bulk insertion; demographic normalization | Amazon RDS `users` table & `s3://.../bronze/reference/users/` | PostgreSQL Relational / Snappy Parquet | **VERIFIED LIVE** (2,000 rows in RDS & S3) |
| **Cards Reference Dataset** | Account Portfolio Baseline | `/app/data/sd254_cards.csv` & root workspace | `BaselineLoader` & `ingest_reference_csvs.py` | Foreign key validation; PAN masking; card profile indexing | Amazon RDS `cards` table & `s3://.../bronze/reference/cards/` | PostgreSQL Relational / Snappy Parquet | **VERIFIED LIVE** (6,146 rows in RDS & S3) |
| **Merchant Category Codes** | Financial Payment Taxonomy (ISO 18245) | `/app/data/merchant_category_codes.csv` | `BaselineLoader` & `ingest_reference_csvs.py` | Preserves 4-digit strings; maps categories & payment brands | Amazon RDS `merchant_category_codes` & `s3://.../bronze/reference/merchants/` | PostgreSQL Relational / Snappy Parquet | **VERIFIED LIVE** (914 codes in RDS & S3) |
| **City Centroids Reference** | US Census Bureau & GeoNames | `/app/data/city_reference.csv` | `BaselineLoader` & `CityResolver` | In-memory hash indexing; centroid coordinate resolution | Amazon RDS `city_reference` & `s3://.../bronze/reference/cities/` | PostgreSQL Relational / Snappy Parquet | **VERIFIED LIVE** (1,433 cities in RDS & S3) |
| **Synthetic Financial Transactions** | `ScenarioEngine` + `RateController` | In-process generator runtime | `GeneratorService.generate_batch()` | Monotonic sequence pacing (10.0 TPS); behavioral synthesis (fraud, declines, travel) | Amazon RDS `transactions` table (OLTP) | PostgreSQL Table (`id BIGSERIAL` PK) | **VERIFIED LIVE** (>7,000 txs generated) |
| **Incremental Extract Batches** | Source A REST API | `GET /api/v1/transactions?since_id=X` | `TransactionExtractor` (`src/ingestion/extractor.py`) | Keyset pagination, Data Quality Gate, Snappy Parquet serialization | Amazon S3 Bronze Data Lake `bronze/transactions/year=YYYY/month=MM/day=DD/` | Apache Parquet (Snappy compressed) | **VERIFIED LIVE** (Deterministic keys in S3) |
| **Ingestion Checkpoints / Watermarks** | `TransactionExtractor` runtime state | Execution high-water mark sequence | `IngestionStateStore` | Conditional updates (`attribute_not_exists OR last_since_id <= :sid`) | Amazon DynamoDB `ingestion_watermarks` | Key-Value NoSQL Item | **VERIFIED LIVE** (`last_since_id=300`) |
| **Quarantined Malformed Transactions** | Ingestion Extractor Data Quality Gate | Validation failure stream | `DataQualityGate.validate_batch()` | Anomaly isolation: negative amounts on non-refunds, missing fields | Amazon S3 Quarantine `bronze/quarantine/transactions/year=YYYY/month=MM/day=DD/` | JSON Lines (`.jsonl`) | **VERIFIED LIVE** (Isolated bad records in S3) |
| **Historical IBM Baseline Transactions** | Kaggle IBM Dataset (Reference Only) | `credit_card_transactions-ibm_v2.csv` | `scripts/profile_data.py` | Offline demographic & behavioral distribution modeling | Local raw archive; target: `s3://.../bronze/historical_raw/ibm_v2/` | Raw CSV (2.24 GB, 24,386,900 rows) | **ARCHIVED** (Excluded from container build) |
| **Single-User Extract (User0)** | Subset extract of User 0 transactions | `User0_credit_card_transactions.csv` | Development testing | Verifies exact distribution matching against User 0 history | Local workspace | Raw CSV (1.81 MB, 19,963 rows) | **VERIFIED** (Duplicate subset archived) |
