# Project state — evidence separation, 2026-10-29

| Area | Status | Supporting/limiting evidence |
|---|---|---|
| Official Project 01 scope | CONFIRMED | Handbook and user selection |
| Four-member owners | CONFIRMED | User: R1 Sayed, R2 Ahmed Mohdaly, R3 Ahmed Abd El Razek, R4 Ahmed Eslam |
| S3 `sic-pipeline-storage` raw/bronze assets in eu-west-3 | OBSERVED IN PRIOR CONNECTED SESSION | AWS read-only inventory: 5 raw CSV files incl IBM, 8 JSON.gz +36 Parquet transaction objects, 3 users+3 cards CDC, MCC/Cities reference snapshots. No live reads in current repo test. |
| Published historical Databricks Silver (`workspace.sic8_silver`) | **USER-REPORTED VERIFIED IN DATABRICKS** | historical_transactions 24,386,900; historical_ground_truth 24,386,900, fraud positives 29,757; quarantine 0; users 2,000; cards 6,146; mcc 914; cities 1,430. NOT independently queried in this repo. |
| Historical ML features PIT v1 | TEAM_REPORTED | existence/schema not checked here |
| Historical Gold (`workspace.sic8_gold`) | PREPARED / NOT VERIFIED PUBLISHED | Notebook 07 missing; no publish logs |
| Historical notebook uploaded | PRESERVED ORIGINAL | user notebook SHA256 in manifest |
| Local 3-channel simulator/3 anomaly fixtures | IMPLEMENTED + LOCAL_TESTED | local Python deterministic reference tests, not R1 cloud generator |
| Local parsing/label isolation/metrics | IMPLEMENTED + LOCAL_TESTED | pytest + `build/demo` SQLite; not Delta/Kafka |
| Streaming Spark / Delta MERGE/CDC SCD2 | STARTER ONLY / NOT RUN IN SPARK | PySpark/Delta runtime absent; CDC local current-state helper != SCD2 history |
| Actual R1 Kafka ingestion/Redis/LightGBM v2 | TEAM_REPORTED / ARTIFACTS MISSING | no model, feature contract, parity or measured logs in workspace; LightGBM 0.8746 ROC-AUC / 81.18% recall / 6.4ms are unverified claims |
| R3 Power BI | BLOCKED OWNER HANDOFF | no PBIX or actual dashboard screenshots |
| R4 AWS Airflow orchestration | BLOCKED / LOCAL DAG TEMPLATE ONLY | no Airflow runtime or real job IDs |
| Optional local API + trace | IMPLEMENTED LOCAL/TESTED | fixture-only read-only SQLite API tests; not secured production infrastructure |
| Optional React | IMPLEMENTED SOURCE / NOT BUILT HERE | Node available; npm dependencies not installed |
| Optional Genie | NOT CONFIGURED | API returns explicit 503, no fabricated responses |
| Cloud deployment | NOT PERFORMED | safe default off, no mutations |

For future updates cite commit SHA, task/run log, exact command, timestamp and environment before switching a status to VERIFIED.
