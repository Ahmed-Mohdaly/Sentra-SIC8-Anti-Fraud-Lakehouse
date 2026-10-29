# Workspace audit — snapshot 2026-10-29

## Environment

- Working directory originally `/mnt/data`, no existing Git repository; this is a **new** Git initialized workspace.
- Available execution: Python 3, pytest, jsonschema, FastAPI, httpx, npm/node, Git. `pyspark`, `delta-spark`, Docker, Airflow and `ruff` not installed in inspected environment.
- No S3/Databricks cloud writes or jobs initiated in this build.

## Existing source artifacts actually accessible

| Item | Origin | Action | Nature |
|---|---|---|---|
| `Untitled-Copy1.ipynb` | user-provided original, 37 cells | copied unchanged into notebook folder with SHA256 | historical exploratory Spark, **not** production or executed in this run |
| `SIC8_R3_Gold_Starter_v0.1/` | prior generated fixture and SQL | preserved under `bi/powerbi/previous_gold_starter_v0.1` | draft fixture, not R3's actual PBIX |
| `00000000000000000275.json` | user-uploaded S3 sample | copied to `tests/fixtures/r1_s3_sample_50.jsonl` | 50 records, synthetic R1 sample; source includes labels in payload |
| Official Handbook PDF, Historical Assessment PDF, R1 Bronze/Data Enhancement DOCX, project design/workload PDFs/DOCX, source context Markdown | uploaded project docs | consulted; references tracked in README and `archive/source-docs/README.md`, not copied as executable proof | requirements/claims |

## Missing as repository contents

Original R1 generator/Kafka/CDC/S3 connector/model source, R1 feature registry/model binary, R2 executed Databricks notebook exports 01–07 and evidence, R3 `.pbix`, R4 IaC/real Airflow DAG/cloud logs and CI source. Existing remote Git URL/branch cannot be inferred from document or S3 scripts alone. No invented repo address or Git commits from teammates.

## New implementation vs prior assets

`src/r1_ingestion/generator.py` is **our deterministic reference fixture**, not Sayed's implementation. `src/r1_fraud_online/model_adapter.py` represents missing model explicitly. `src/r2_spark/*` starter modules have **not been executed with PySpark/Delta**; local algorithm reference and SQLite fixture are independently runnable. R3 optional React portal frontend and R4 fixture API are generated and not production deployments. Local pytest validation only is evidence-backed.
