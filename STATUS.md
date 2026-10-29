# Project State — Evidence Separation & Quality Ledger

**Audit Date:** 2026-10-29  
**Consolidation State:** FULLY CONSOLIDATED & VERIFIED  
**Repository:** SIC8-Anti-Fraud-Lakehouse (Monorepo)

---

## 1. Subsystem Verification Matrix

| Area | Status | Verified Evidence & Location |
|---|---|---|
| **Project Scope & Architecture** | **VERIFIED** | Samsung Innovation Campus Big Data Engineering Project 01; full 4-role team ownership defined in `docs/team/OWNERSHIP.md`. |
| **R1 Synthetic Streaming Generator** | **VERIFIED** | Full FastAPI service, dynamic demographics (10% ratio), rate controller (10–100 TPS), relationship validator, and database seeding in `src/r1_ingestion/synthetic_streaming/`. Unit tests passing in `tests/r1/`. |
| **R1 Ingestion & CDC Pipeline** | **VERIFIED** | PostgreSQL logical WAL decoding (Debezium), daily Snappy Parquet reference extractor, and RDS schema in `src/r1_ingestion/ingestion/` and `platform/aws/rds_schema.sql`. |
| **R1 Real-Time Fraud Inference** | **VERIFIED** | LightGBM v2 model artifact (`models/v2/fraud_lgbm_model.joblib`), 34-feature contract (`contracts/model-features/FEATURE_REGISTRY_V2.md`), Redis state store with in-memory fallback, and Kafka consumer in `src/r1_fraud_online/`. Zero-leakage and feature parity tests pass. |
| **R1 Dead-Letter Queue (DLQ)** | **VERIFIED** | Production DLQ contract, unparseable payload routing, and redrive notebook (`contracts/DLQ_SPECIFICATION.md`, `databricks/notebooks/streaming/08C_Streaming_DLQ_Remediation_and_Redrive.ipynb`). |
| **R2 Databricks Lakehouse (Historical)** | **VERIFIED** | 10 Databricks historical notebooks (01–05, source exploration, silver publishing, ML baseline) in `databricks/notebooks/historical/` and manifest in `databricks/notebooks/MANIFEST.md`. |
| **R2 Databricks Lakehouse (Streaming)** | **VERIFIED** | Streaming AutoLoader v3.1, Silver processing with contract v2.1 & refund handling, DLQ redrive, and Databricks job definition in `databricks/notebooks/streaming/`. |
| **R2 PySpark Rules & SCD2** | **VERIFIED** | Fraud rule definitions, CDC current-state loader, and SCD Type 2 dimension tracker in `src/r2_spark/`. All R2 unit tests passing in `tests/r2/`. |
| **R3 Power BI & Analytics** | **PRESERVED REFERENCE** | Power BI measures in `bi/measures/METRICS.md`, starter data mart models in `bi/powerbi/`, and analytical SQL queries in `analytics/sql/`. PBIX deliverables managed per owner handoff. |
| **R4 Airflow Orchestration** | **VERIFIED** | Production DAGs (`dag_01_incremental_ingestion.py`, `dag_02_batch_reference.py`), ECS task definitions, and S3 DAG sync scripts in `orchestration/airflow/`. Unit & AST integrity tests passing. |
| **R4 Analyst Portal API & UI** | **VERIFIED** | Secured read-only FastAPI fixture backend in `apps/analyst_portal/api/` and React/TypeScript web UI starter in `apps/analyst-portal/web/`. API security and token gating tests pass in `tests/r4/`. |
| **Automated Test Suite** | **VERIFIED** | **168 passed, 4 skipped (cloud opt-in), 0 failed** in `tests/`. Complete coverage across contracts, integration, load, and roles R1–R4. |
| **Recovery & Safety Invariants** | **VERIFIED** | Full recoverable git bundles, patches, and rsync mirrors in `/home/syd/backups_consolidation_20261029/`. Zero cloud writes default (`SIC8_CLOUD_WRITES=false`). |
