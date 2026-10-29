# Ownership and acceptance boundaries

| Role | Owner | Owns | Input | Handoff / Acceptance |
|---|---|---|---|---|
| R1 | Sayed | `src/r1_ingestion`, `src/r1_fraud_online`, `configs/r1`, Kafka/MSK/CDC source configurations, sample events, model/Redis artifact metadata | IBM baseline and simulator config | versioned envelope, stable event/business IDs, 3 channels + 3 scenario truth sidecars, reproducible Kafka-to-S3/replay evidence, model parity evidence; R2 reviews payload |
| R2 | Ahmed Mohdaly | `src/r2_spark`, `databricks/notebooks`, `analytics/sql`, `analytics/gold_models`, contract mapping | R1 raw/CDC and immutable historical CSV | Silver Delta and quarantines, 3 rules + 10m velocity, stable identities, idempotent replay, Gold facts/SCD2/views, KPI reconciliation; R3 reviews definitions |
| R3 | Ahmed Abd El Razek | `bi`, `apps/analyst-portal/web`, demo/business narrative | R2 published documented Gold SQL/views or approved test fixtures | PBIX/PBIT, slicers/drillthrough, 3 KPI views, historical vs simulated distinction and SQL reconciliation |
| R4 | Ahmed Eslam | `platform`, `orchestration`, `.github`, `apps/analyst-portal/api`, CI and runbooks | R1 ingestion and R2 jobs, credentials held outside repo | Airflow DAG job graph, secure least privilege, monitoring/latency, replay recovery evidence, billing checks; no unapproved deploy |

## Shared review gate

- Transaction/CDC changes: R1 + R2.
- Fraud logic/feature parity: R2 + R1.
- Gold/DAX KPI semantics: R2 + R3.
- Cloud APIs, IAM, Airflow, CI: R4 and affected owner.
- Cross-role PRs require tests plus an evidence entry or explicit `NOT_TESTED` note.

**CODEOWNERS includes only the verified GitHub login `@Ahmed-Mohdaly` for R2 paths.** R1/R3/R4 GitHub handles are not known; R4 must add them only after the owners confirm access. Keep code independently mergeable; do not overwrite teammate implementations with local fixtures.
