# Batch Reference Ingestion Specification
## SIC — Synthetic Financial Transaction Data Platform

**Service:** Scheduled Batch Reference Data Extractor  
**Orchestrator:** Amazon EventBridge (`scheduled-ingestion-extractor`)  
**Runtime:** AWS ECS Fargate (`synthetic-ingestion-extractor:5`)  
**Source Database:** Amazon RDS PostgreSQL 18.3 (`containerdb`)  
**Target Storage:** Amazon S3 Bronze (`s3://sic-pipeline-storage`)  
**Output Format:** Columnar Apache Parquet with Snappy Compression  
**Status:** `PRODUCTION OPERATIONAL`

---

## 1. Architectural Role & Ingestion Pattern

While dynamic financial transactions stream directly to MSK and users/cards are captured via CDC, static and slowly changing reference dimensions are ingested via **scheduled batch snapshots**.

Reference datasets provide essential context for downstream Silver and Gold dimensional models (e.g., star schemas, merchant fraud scoring, geographic enrichment). Ingesting them as daily partitioned Parquet snapshots guarantees temporal consistency and eliminates CDC overhead for low-churn reference tables.

```mermaid
flowchart TD
    subgraph Scheduling["Amazon EventBridge"]
        EB["Rule: scheduled-ingestion-extractor\nSchedule: cron(0 2 * * ? *)"]
    end

    subgraph Fargate["AWS ECS Fargate Task"]
        Task["Task Def: synthetic-ingestion-extractor:5\nCommand: ['batch-reference']\nScript: src/ingestion/batch_reference_extractor.py"]
    end

    subgraph RDS["Amazon RDS PostgreSQL"]
        T1[("public.merchant_category_codes\n(914 rows)")]
        T2[("public.city_reference\n(1,433 rows)")]
    end

    subgraph S3["Amazon S3: sic-pipeline-storage"]
        S1["bronze/mcc/snapshot_date=YYYY-MM-DD/\nreference_mcc_YYYYMMDD_HHMMSS.parquet"]
        S2["bronze/cities/snapshot_date=YYYY-MM-DD/\nreference_cities_YYYYMMDD_HHMMSS.parquet"]
    end

    EB -->|Trigger RunTask| Task
    Task -->|SQL Query (Secrets Manager Auth)| T1
    Task -->|SQL Query (Secrets Manager Auth)| T2
    Task -->|PyArrow Snappy Parquet| S1
    Task -->|PyArrow Snappy Parquet| S2
```

---

## 2. Dataset Schemas & Columnar Typing

### 2.1. Merchant Category Codes (`bronze/mcc/`)
Extracted from `public.merchant_category_codes` (914 records). Defines standard ISO/card network merchant categorizations.

| Column Name | SQL Type | Arrow / Parquet Type | Nullable | Description |
|---|---|---|---|---|
| `mcc` | `VARCHAR` | `pa.string()` | No (PK) | 4-digit standard MCC identifier |
| `description` | `VARCHAR` | `pa.string()` | Yes | Merchant business description |
| `valid_payment_brands` | `VARCHAR` | `pa.string()` | Yes | Supported card networks (e.g. Visa, Mastercard) |
| `category` | `VARCHAR` | `pa.string()` | Yes | Broad industry classification |
| `required_abbreviation`| `VARCHAR` | `pa.string()` | Yes | Short ticker/abbreviation |
| `snapshot_timestamp` | Derived | `pa.timestamp("us", tz="UTC")`| No | Extractor execution timestamp |

### 2.2. City Reference (`bronze/cities/`)
Extracted from `public.city_reference` (1,433 records). Provides geographic enrichment for user locations and physical merchant locations.

| Column Name | SQL Type | Arrow / Parquet Type | Nullable | Description |
|---|---|---|---|---|
| `city_id` | `BIGINT` | `pa.int64()` | No (PK) | Unique surrogate city identifier |
| `city_name` | `VARCHAR` | `pa.string()` | No | Full name of the municipality |
| `state_code` | `VARCHAR` | `pa.string()` | Yes | 2-letter state/province abbreviation |
| `country_code` | `VARCHAR` | `pa.string()` | Yes | ISO 3166-1 alpha-2 country code |
| `latitude` | `DOUBLE PRECISION`| `pa.float64()` | Yes | Decimal latitude coordinates |
| `longitude` | `DOUBLE PRECISION`| `pa.float64()` | Yes | Decimal longitude coordinates |
| `coordinate_source` | `VARCHAR` | `pa.string()` | Yes | Geocoding authority or dataset source |
| `coordinate_precision`| `VARCHAR` | `pa.string()` | Yes | Positional precision rating |
| `snapshot_timestamp` | Derived | `pa.timestamp("us", tz="UTC")`| No | Extractor execution timestamp |

---

## 3. Extractor Architecture (`BatchReferenceExtractor`)

The extractor is implemented in `src/ingestion/batch_reference_extractor.py` using a stateless, resilient design:

1. **Database Connection:** Connects securely using SQLAlchemy with driver normalization (`postgresql+psycopg2://`). Credentials are dynamically resolved from AWS Secrets Manager (`synthetic-data-provider/database`) with automatic error sanitization.
2. **Deterministic Typing:** Converts database result sets into strongly typed `pyarrow.Table` objects enforcing explicit PyArrow schemas. Null values in float and string columns are safely handled.
3. **Partitioned Parquet Generation:** Generates Apache Parquet buffers compressed with **Snappy**.
4. **Idempotent S3 Upload:** Uploads to date-partitioned Hive prefixes (`snapshot_date=YYYY-MM-DD/`) using `boto3`. Multiple executions on the same day append distinct timestamped files without corrupting previous snapshots.

---

## 4. ECS Fargate & EventBridge Orchestration

### Task Definition Configuration
- **Task Definition:** `arn:aws:ecs:eu-west-3:138257835434:task-definition/synthetic-ingestion-extractor:5`
- **Container Image:** `138257835434.dkr.ecr.eu-west-3.amazonaws.com/synthetic-data-provider:latest`
- **Command:** `["batch-reference"]` (mapped in `entrypoint.sh` to `python /app/src/ingestion/batch_reference_extractor.py`)
- **CPU / Memory:** 256 CPU (0.25 vCPU) / 512 MiB RAM
- **IAM Execution Role:** `arn:aws:iam::138257835434:role/ecsTaskExecutionRole`
- **IAM Task Role:** `arn:aws:iam::138257835434:role/syntheticTaskRole` (Grants `s3:PutObject`, `s3:ListBucket` on Bronze bucket)

### EventBridge Scheduled Rule
- **Rule Name:** `scheduled-ingestion-extractor`
- **Target:** ECS Fargate Cluster `synthetic-data-cluster` with Task Definition `synthetic-ingestion-extractor:5`
- **Network Configuration:** Security group `sg-0dd0933f4da2b5ea7`, subnets `subnet-003d3891dab36b354`, `subnet-0b18944bde732d1fd` with `AssignPublicIp: ENABLED`.

---

## 5. Live Verification Evidence

During Phase 8 Live Pipeline Validation, the batch extractor was executed end-to-end inside the VPC:

```
[INFO] Starting BatchReferenceExtractor execution...
[INFO] Extracting table: merchant_category_codes
[INFO] Query returned 914 rows for merchant_category_codes.
[INFO] Converting 914 records to PyArrow Table...
[INFO] Writing Parquet buffer (Snappy compressed, size: 27,414 bytes)...
[INFO] Successfully uploaded s3://sic-pipeline-storage/bronze/mcc/snapshot_date=2026-10-25/reference_mcc_20261025_214512.parquet
[INFO] Extracting table: city_reference
[INFO] Query returned 1,433 rows for city_reference.
[INFO] Converting 1,433 records to PyArrow Table...
[INFO] Writing Parquet buffer (Snappy compressed, size: 38,653 bytes)...
[INFO] Successfully uploaded s3://sic-pipeline-storage/bronze/cities/snapshot_date=2026-10-25/reference_cities_20261025_214513.parquet
[INFO] Batch reference extraction completed successfully: 2/2 tables extracted.
```

Verified objects in S3:
- `bronze/mcc/snapshot_date=2026-10-25/reference_mcc_20261025_214512.parquet` (27,414 bytes)
- `bronze/cities/snapshot_date=2026-10-25/reference_cities_20261025_214513.parquet` (38,653 bytes)
- Parquet schema and record counts validated directly against live PostgreSQL table counts.
