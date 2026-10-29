# Required next actions / teammate handoffs

1. **R1 Sayed:** provide actual repo/branch, generator Kafka/MSK/CDC connector config (sanitized), simulation truth sidecar, deterministic 3 scenario fixtures, full model binary/version & feature registry, Redis state spec, `mcc_risk_score` leakage audit, train/split manifest and inference logs. Please confirm EUR/USD, exact GPS vs city centroids, terminal failure reasons, and transaction/account ID mapping. Do not send secrets.
2. **R2 Ahmed Mohdaly:** export actual executed Databricks notebooks 01–07 and report actual Silver/Gold table schema + counts + run logs. Do not rerun historical 24M or overwrite Unity Catalog just to merge repo. Add tested Spark Structured Streaming/Delta MERGE/SCD2; approve account mapping and 10-minute velocity event-time policy; compare live wrappers with fixtures.
3. **R3 Ahmed Abd El Razek:** add actual PBIX/PBIT, DAX definitions and dashboard screenshots with run context. Reconcile three official KPIs to Gold SQL; label Historical Source Truth vs Live Detected. Own optional portal UI.
4. **R4 Ahmed Eslam:** provide IaC and Airflow DAGs already in use plus read-only S3/Databricks roles, runtime versions, cost guardrail and recovery evidence; never commit credentials. Wire actual scheduled dependencies and monitor Kafka/S3/Stream separately.
5. **Integration meeting:** approve schema version, source namespaces, account/card IDs, currency/geo policies, terminal-failure semantics and team event trace. Record ADR before cloud writes.
6. **P0 proof:** 3 actual channels + scenario events → Kafka → immutable raw → Silver Delta/checkpoint/replay → Rule/ML outputs → Gold SQL 3 KPIs → Power BI → Airflow/recovery logs. Record evidence ID for each step. Keep all optional bonuses behind explicit flags.

## Minimal developer commands

```bash
python -m pip install -e '.[dev]'
make test
make demo-local
make lint
```

## Safe PR order

`feature/r1-contract-and-source` → `feature/r2-silver-and-fraud` → `feature/r2-gold` → `feature/r3-powerbi` → `feature/r4-orchestration` → `feature/integration-evidence`. Reviews by owners per `docs/team/OWNERSHIP.md`.
