# SIC8 engineering build report — 2026-10-29

## 1. Scope / outcome

Created a **new GitHub-ready monorepo** from the available files; there was **no existing local Git repo**. Source-preserved historical user notebook and earlier R3 Gold starter fixtures. This is a **verified offline reference implementation and integration scaffold**, NOT a complete deployed SIC8 Project 01 system. No actual AWS or Databricks writes or deployments performed. Cloud access and entitlement in previous conversations do not substitute for current repo tests.

## 2. Actual executed checks

See `evidence/integration/LOCAL_TEST_RESULTS.txt`. Final: **33 passed, 3 skipped** (live Kafka/MSK→S3→Databricks→Power BI, real LightGBM parity, Airflow/Delta recovery). `make demo-local` executed: **13 fixture transactions, 4 alerts on 3 unique transactions, 8 truth-labeled scenario events, 3 detected truth events, 3/3 scenario families detected, 6 terminal attempts with 1 explicit terminal failure**. `event_detection_rate = 3/8 = 0.375`, `scenario_family_detection_rate = 3/3 = 1.0`, `terminal_failure_rate = 1/6 ≈ 0.1667`. They are **fabricated deterministic fixture inputs but genuinely computed test outputs**, never claims about live banking, Kafka/MSK or R1's LightGBM. Python `compileall` successful; editable install `--no-deps --no-build-isolation` successful; 8 JSON Schemas parsed successfully.

Ruff failed to install because package index/DNS unavailable; lint beyond Python compile is unverified. Docker unavailable; Airflow/PySpark/Delta unavailable; React dependencies absent, so no npm build/typecheck. GitHub CI workflow authored but **not run on GitHub**. Streaming Spark helper and cloud DAG are not validated as deployed. No independent Cloud/Kafka/Databricks/Power BI end-to-end test performed.

## 3. Main deliverables

- `src/common/contracts.py`: event validation, UTC/money/location checks, S3 wrapper parser, truth isolation; `contracts/jsonschema/`: **8 draft schemas** and tests.
- `src/r1_ingestion/generator.py`: fixed-seed **reference** three-channel + CNP/Geo/Micro fixtures. `src/r1_ingestion/kafka_reference.py`: explicitly gated optional local Kafka producer (broker NOT tested).
- `src/r2_spark/bronze_adapter.py`: real R1 `jsonl` sample contract parser with explicit account mapping; 50-row source sample test. `src/r2_spark/rules.py`: event-time local reference rules and per-card/per-account 10-minute velocities; `cdc.py`, `scd2.py`: pure reference CDC current-state + SCD2 intervals. `spark_jobs.py`: guarded Databricks reader/merge starter, NOT executed with PySpark.
- `src/common/pipeline.py` + `sqlite_gold.py`: deterministic local raw→schema validation→rule detections→separate SQLite facts/truth and idempotent replay.
- `src/r1_fraud_online/model_adapter.py`: truthful `MODEL_NOT_CONFIGURED`, not fictional LightGBM scores.
- `analytics/sql/streaming_kpis.sql` and `analytics/gold_models/GRAINS.md`: draft Gold semantic model and three required KPI definitions, no cloud publication.
- `apps/analyst_portal/api/main.py`: guarded read-only local fixture metrics and trace; `apps/analyst-portal/web/`: source-only React demo with explicit DEMO_FIXTURE. Genie returns 503 not configured.
- `orchestration/airflow/sic8_pipeline.py`: local fixture DAG with real local operations if Airflow installed; **not** cloud job DAG.
- `.github/workflows/ci.yml`, `.github/PULL_REQUEST_TEMPLATE.md`, placeholder CODEOWNERS (unknown handles), `Makefile`, `pyproject.toml`, `.env.example`, Compose template, owner docs, architecture Mermaid diagrams, runbooks, rubric matrix, ADRs, demo and defense Q&A, bonus roadmap.
- Original `Untitled-Copy1.ipynb` copied unchanged into `databricks/notebooks/historical/` with SHA256 manifest. Prior Gold starter v0.1 copied as draft to `bi/powerbi/previous_gold_starter_v0.1`.

## 4. Ownership (user-confirmed)

R1 **Sayed** — actual generator/Kafka/CDC/S3 and online LightGBM + Redis; R2 **Ahmed Mohdaly** — historical and streaming Spark/Silver, rules, Gold SQL; R3 **Ahmed Abd El Razek** — Power BI, BI QA, optional React UI; R4 **Ahmed Eslam** — platform, Airflow, monitoring, security, read-only API. See `docs/team/OWNERSHIP.md` and the 4 role handoff READMEs.

## 5. Explicit blockers before saying the graduation project is completed

1. **R1 actual code, infra and model:** no original R1 repository/branch, model/checkpoint, 34-feature ordering, parity code, Redis/online scoring evidence locally. Source and model claims remain TEAM_REPORTED.
2. **R2 Databricks:** executed notebooks 01–07 not supplied; historical Silver counts user-reported from actual prior execution, not rechecked; historical Gold publication unverified. Spark/Delta/Streaming/SCD2 cloud writes disabled and untested.
3. **R3 Power BI:** actual PBIX, DAX, dashboards and refresh/reconciliation evidence missing.
4. **R4 orchestration/cloud:** real Airflow DAG/cloud task IDs/permissions and recovery logs absent. Existing AWS resources may incur cost; check before any launch.
5. **Mandatory live acceptance:** need actual Kafka/raw durability/retry DLQ, all 3 live scenarios, Spark Silver/Gold Delta and 3 mandatory KPIs, source-correct CDC SCD2, Power BI, Airflow logs and replay evidence. Offline fixture is NOT a substitute.
6. **Bonuses:** Trace Explorer implemented for **local fixture only**; other bonus scenarios are honest roadmap/partial code, Genie not enabled, no fabricated ML/SHAP/stress evidence. See `docs/bonuses/BONUS_ROADMAP.md`.

## 6. Next safe merge steps

1. R1 attaches *actual* code and verified event/model artifacts; R2 signs canonical field semantics (account/card, original timestamp, EUR vs USD, geo precision, terminal failure).
2. R2 imports actual notebook exports without modifying original notebook; validates source schemas and one tiny Silver canary in staging, no full 24M reprocessing.
3. R3 starts approved Gold fixture, checks KPI grain and attaches actual PBIX/DAX.
4. R4 reviews AWS/Databricks IAM/cost, enables real Airflow only with approval; records end-to-end evidence.
5. Integration reviewer connects 1 verified event_id across all layers, records exact run/commit/log evidence and confirms replay does not duplicate business facts.

No secrets, full historical 2.35GB CSV or real credit-card data were added to this repository. The supplied R1 test JSONL contains synthetic IDs and is retained for parsing regression tests.
