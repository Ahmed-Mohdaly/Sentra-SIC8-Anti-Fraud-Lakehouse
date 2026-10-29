# Component Inventory & Architecture Dependencies

**Project:** Cloud-Ready Synthetic Financial Transaction Data Provider & Ingestion Lakehouse  
**Target Environment:** AWS `eu-west-3` (Paris)  
**Classification:** Enterprise System Architecture Inventory

---

## 1. System Components Directory

| Component Name | Architectural Role | Primary Inputs | Primary Outputs | Key Dependencies | Implementation Status |
|---|---|---|---|---|---|
| **`BaselineLoader`** (`src/synthetic_streaming/baseline_loader.py`) | Reference entity parser & empirical distribution indexer. | Raw reference CSVs in `/data` (`sd254_users.csv`, `cards`, `mcc`, `cities`). | Indexed demographic pools, card catalogs, and $O(1)$ city coordinate cache. | Local or container filesystem `/data`. | **VERIFIED LIVE** |
| **`ScenarioEngine`** (`src/synthetic_streaming/scenario_engine.py`) | Financial behavior synthesizer. | Demographic profiles, random seeds, scenario weights (normal 85%, high-value 3%, fraud 0.2%, declines 4%, refunds 3%). | Synthetic transaction event envelopes (`TransactionEnvelope`). | Python PRNG, `BaselineLoader`. | **VERIFIED LIVE** |
| **`RateController`** (`src/synthetic_streaming/rate_controller.py`) | Monotonic clock rate limiter. | Configurable target TPS (e.g. 10.0 TPS). | Paced generation signals preventing clock drift. | `time.monotonic()`. | **VERIFIED LIVE** |
| **`GeneratorService`** (`src/synthetic_streaming/generator_service.py`) | Core application orchestrator. | Configuration parameters, DB connection, rate controller. | Batch atomic commits to PostgreSQL, REST status metrics. | SQLAlchemy engine, `RateController`, `ScenarioEngine`. | **VERIFIED LIVE** |
| **`FastAPI Management & Stream Server`** (`src/synthetic_streaming/api.py`) | REST API endpoint server (Source A). | HTTP requests on port 8000 (`/health`, `/api/v1/transactions`). | JSON transaction pages with cursor watermarks (`since_id`, `max_id`). | Uvicorn, FastAPI, Pydantic, SQLAlchemy. | **VERIFIED LIVE** |
| **`Relational Source Database`** (Amazon RDS PostgreSQL 18) | Source B relational store and transaction persistence. | SQL DDL & DML from container and external engineering clients. | ACID persistent tables (`users`, `cards`, `mcc`, `city_reference`, `transactions`). | AWS VPC, Private Subnets, Security Group `sg-0041172e0d9b6c810`. | **VERIFIED LIVE** |
| **`TransactionExtractor`** (`src/ingestion/extractor.py`) | Downstream incremental ingestion extractor. | Keyset paginated stream from FastAPI (`since_id`). | Snappy Parquet files in S3 Bronze, quarantine records, DynamoDB watermark commits. | Python `pyarrow`, `boto3`, `httpx`, S3, DynamoDB. | **VERIFIED LIVE** |
| **`DataQualityGate`** (`src/ingestion/extractor.py`) | In-stream quality gate and validator. | Raw JSON records batch. | Valid records list and Quarantined anomalies list. | Pydantic / Schema rules. | **VERIFIED LIVE** |
| **`IngestionStateStore`** (`src/ingestion/extractor.py`) | Watermark state keeper. | Monotonic transaction sequence ID (`max_id`). | Atomic DynamoDB checkpoints preventing regressive overwrites. | Amazon DynamoDB (`ingestion_watermarks`). | **VERIFIED LIVE** |
| **`Reference Ingestion Pipeline`** (`src/ingestion/ingest_reference_csvs.py`) | One-time / periodic reference dataset syncer to lakehouse. | Baseline reference CSV files. | Structured Parquet reference snapshots in S3 Bronze. | PyArrow CSV & Parquet, Boto3 S3. | **VERIFIED LIVE** |
| **`EventBridge Scheduler`** (Amazon EventBridge) | Cloud-native periodic orchestrator. | Cron expression (`rate(5 minutes)`). | Scheduled ECS Task invocations of `synthetic-ingestion-extractor`. | AWS IAM Role `ecsEventsRole`, ECS Cluster. | **VERIFIED LIVE** |
| **`AWS Secrets Manager`** | Secure credential management. | Master database credentials. | Encrypted secrets injected into ECS container memory at boot. | AWS KMS, IAM Policy `SecretsManagerReadDatabaseSecret`. | **VERIFIED LIVE** |
| **`CloudWatch Logs & Metrics`** | Platform observability. | stdout / stderr logs from ECS containers. | Centralized log streams (`/ecs/synthetic-data-provider`, `/ecs/synthetic-ingestion-extractor`). | CloudWatch agent / awslogs driver. | **VERIFIED LIVE** |
