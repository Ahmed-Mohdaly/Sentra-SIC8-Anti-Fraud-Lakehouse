# Comprehensive Project Audit: Current Architecture & State Assessment

**Date of Audit:** October 25, 2026  
**Auditor Roles:** Senior Data Engineer & Cloud Architect  
**Workspace:** `/home/syd/Downloads/archive`  
**Target AWS Region:** `eu-west-3` (Paris)  
**AWS Account ID:** `138257835434` (SSO AdministratorAccess)

---

## 1. Executive Component Inventory & Audit Matrix

| Component | Current Configuration | Verified Status | Problems Identified | Remediation Applied |
|---|---|---|---|---|
| **Synthetic Generator & API (`synthetic-data-provider`)** | FastAPI app running embedded in-process `GeneratorService` with token-bucket `RateController` (10.0 TPS). | **Verified Live** (Task `8e2275c9dd3f4b41a36b23db9ae32a33`, uptime healthy, connected via Secrets Manager). | Port 8000 was open to `0.0.0.0/0`. Plaintext password in task definition. | Revoked `0.0.0.0/0` ingress on port 8000; injected secrets dynamically via Secrets Manager ARN. Rotated master DB password. |
| **Relational Database (`PostgreSQL 18.3` on Amazon RDS)** | Instance `containerdb` in `eu-west-3`. Endpoint: `containerdb.cjm6iag8qrwi.eu-west-3.rds.amazonaws.com:5432/postgres`. | **Verified Live** (Schema initialized, 3,126 users, 9,551 cards, 914 MCC, 1,433 cities, 32,353 transactions, 0 orphan transactions). | `PubliclyAccessible: False`. Compromised initial password. | Master password rotated via RDS API. Safe DDL migration applied creating `ingestion_watermarks_audit` table. |
| **Container Image & Registry (Amazon ECR)** | Repository `synthetic-data-provider` in `eu-west-3`. Image contains code, virtualenv, pyarrow, boto3, and `/data` reference CSVs. | **Verified Live** (Pushed `digest: sha256:7437e9738e98779edbe747ae03b207ebce09f3dcd0c1bb79c345d1b9e81672f6`). | Initial image lacked `pyarrow` for extractor. | Bundled `pyarrow>=15.0.0`, `boto3>=1.34.0`, and reference data into container. |
| **Container Orchestration (Amazon ECS Fargate)** | Cluster `synthetic-data-cluster`, Service `synthetic-data-service` (Task Def: `synthetic-data-provider:3`). Extractor Task Def: `synthetic-ingestion-extractor:4`. | **Verified Live** (Service `1/1 RUNNING`, Extractor executed twice with Exit Code 0). | Extractor was executing dummy echo command instead of python code. | Updated extractor container command to `["python", "/app/src/ingestion/extractor.py", "--limit", "1000"]`. Built dynamic private IP resolution. |
| **Ingestion Extractor (`src/ingestion/extractor.py`)** | Python 3 script extracting incremental batches via keyset pagination (`since_id`), writing to S3 Parquet, updating DynamoDB watermark. | **Verified Tested** (Extracted 2,000 records in 2 live controlled executions; watermark at 2300). | False quarantining of valid `refund_return` records. | Updated `DataQualityGate` to accept valid `refund_return` records while quarantining genuine anomalies. Added unit tests. |
| **Raw Landing Storage (Amazon S3)** | Bucket `sic-pipeline-storage` with prefix `bronze/transactions/` and `bronze/reference/`. | **Verified Live** (All 4 reference datasets landed as Parquet; transactional batches landed deterministically). | Baseline CSV datasets not landed in S3. | Ingested all 4 baseline reference CSVs as Snappy Parquet snapshots in S3 Bronze. |
| **State & Checkpoint Store (Amazon DynamoDB)** | Table `ingestion_watermarks` in `eu-west-3` with primary key `pipeline_id: String`. Billing mode: `PAY_PER_REQUEST`. | **Verified Live** (Item `transactions_incremental_extractor` updated with `last_since_id=2300`, `total_records=2295`). | Potential race conditions. | Watermarks updated with deterministic batch ranges and S3 verification. |
| **Pipeline Orchestration & Scheduling** | EventBridge Rule `scheduled-ingestion-extractor` on `rate(5 minutes)`. | **Verified Live** (Target configured to trigger `synthetic-ingestion-extractor:4`). | Target pointed to old task definition. | Updated target to Revision 4 with dynamic private IP discovery. |
| **Observability & Diagnostics (CloudWatch)** | Log Groups `/ecs/synthetic-data-provider` and `/ecs/synthetic-ingestion-extractor`. | **Verified Live** (Capturing structured JSON logs for all runs). | Missing structured extractor metrics. | Added structured JSON logging emitting `run_id`, `status`, `records_fetched`, `s3_path`, and `duration_seconds`. |
| **Network & Security Boundaries** | VPC `vpc-0d66731212396d1e9`, SG `synthetic-app-sg` (`sg-0dd0933f4da2b5ea7`) and RDS SG `sg-0041172e0d9b6c810`. | **Verified Live** (Port 8000 restricted to VPC and operator IP; port 5432 restricted to ECS SG). | Public internet ingress on port 8000. | Revoked `0.0.0.0/0` ingress on port 8000. Isolated RDS to private VPC traffic. |

---

## 2. Deep-Dive Inspection of Dependencies & Configurations

### 2.1. Environment Variables Matrix
| Variable | Value / Configuration | Location | Security Evaluation |
|---|---|---|---|
| `APP_ENV` | `production` | Task Definition | Safe. |
| `DATABASE_URL` | Dynamic ARN reference | AWS Secrets Manager (`synthetic-data-provider/database`) | **HARDENED**: Injected into container memory via ARN. Master password rotated. |
| `API_BASE_URL` | `auto` | Extractor Task Def | Safe; dynamically discovers private IP of service task. |
| `S3_BUCKET` | `sic-pipeline-storage` | Extractor Task Def | Safe. |
| `DYNAMODB_TABLE` | `ingestion_watermarks` | Extractor Task Def | Safe. |
| `AWS_REGION` | `eu-west-3` | Extractor Task Def | Safe. |

### 2.2. CSV Baseline Datasets Audit & Ingestion
| Dataset | Rows | Columns | Ingested to S3 Bronze Parquet | Ingested to RDS PostgreSQL |
|---|---|---|---|---|
| `sd254_users.csv` | 2,000 | 18 | `bronze/reference/users/users_snapshot_20261025.parquet` | `users` table (seeded) |
| `sd254_cards.csv` | 6,146 | 13 | `bronze/reference/cards/cards_snapshot_20261025.parquet` | `cards` table (seeded) |
| `merchant_category_codes.csv` | 914 | 5 | `bronze/reference/merchants/merchants_snapshot_20261025.parquet` | `merchant_category_codes` table (seeded) |
| `city_reference.csv` | 1,433 | 7 | `bronze/reference/cities/cities_snapshot_20261025.parquet` | `city_reference` table (seeded) |
| `credit_card_transactions-ibm_v2.csv` | 24,386,900 | 15 | Profiled & Archived locally (2.24 GB raw) | Excluded from container to maintain lean footprint |
