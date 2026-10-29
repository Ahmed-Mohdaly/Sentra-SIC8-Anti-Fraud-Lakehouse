# Project 01 rubric traceability — honest status

| P0 requirement | Owner | Implemented here | Test/evidence | State |
|---|---|---|---|---|
| Python 3-channel simulator, 3 fraud families | R1 | `src/r1_ingestion/generator.py` **reference fixture only** | `tests/r1` | LOCAL TESTED; actual R1 source missing |
| Kafka ingestion + S3 raw/CDC | R1/R4 | `src/r1_ingestion/kafka_reference.py` and raw parsing | test local guard; prior S3 inventory, not live E2E | BLOCKED LIVE |
| Bulk/scheduled historical ingestion | R1/R2 | original historical notebook preserved | user-reported Silver counts; no notebook execution here | USER REPORTED |
| Schema/quality/dedup/quarantine | R2 | `src/common/contracts.py`, `src/r2_spark/bronze_adapter.py` and local fixture | `tests/r2` | LOCAL TESTED, DELTA UNTESTED |
| Delta ACID / merge/time travel | R2 | safe guarded `spark_jobs.py` starter | test write guard only | BLOCKED DELTA RUNTIME |
| Sliding 10m account velocity | R2 | local event-time per card with account field; SQL draft account KPI | local rule tests | ACCOUNT SQL NOT CLOUD VERIFIED |
| CNP/Geo/Micro distributed Spark rules | R2 | local deterministic pure Python rules | local positive/negative tests | REFERENCE, NOT DISTRIBUTED VERIFIED |
| Source truth isolation | R1/R2 | separate truth sidecar and relational table | `tests/r1`, `tests/r2` | LOCAL TESTED |
| Terminal failure KPI | R2/R3 | local explicit reason-based KPI, Gold SQL draft | `tests/r3` | LOCAL TESTED, CLOUD PENDING |
| Fraud detection rate KPI | R2/R3 | local event grain vs sidecar; Gold SQL draft | integration test | LOCAL TESTED, LIVE PENDING |
| Gold dims/facts + SCD1/SCD2 | R2 | SQLite facts + Gold grain documentation; legacy SQL draft | `tests/integration`, `tests/r2/test_cdc.py` | SCD2 / DELTA BLOCKED |
| Power BI dashboard | R3 | R3 draft template docs only | no PBIX | BLOCKED OWNER FILE |
| Airflow DAGs / recovery | R4 | local fixture DAG template; recovery runbook | Python compile only | AIRFLOW RUNTIME BLOCKED |
| Security, CI and automated tests | R4 | .github CI, API auth and tests | `tests/r4` | LOCAL TESTED; CI not run on GitHub |
| Live event trace & presentation | All | read-only local fixture trace API + demo outline | `tests/r4/test_api.py` | LOCAL FIXTURE VERIFIED ONLY |
