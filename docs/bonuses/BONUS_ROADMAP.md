# Bonus modules — do not displace P0

| Order | Bonus | Owner | This repository | Required genuine evidence |
|---|---|---|---|---|
| 1 | Trace explorer | R2 SQL + R4 API + R3 React | **LOCAL FIXTURE IMPLEMENTED**, live stages deliberately missing | same event_id through Kafka/S3/Silver/Gold + metadata |
| 2 | ML vs rules comparison | R1 model + R2 SQL + R3 BI | PLANNED; no actual model feature evidence | matched population/time/truth with unbiased per-event metrics |
| 3 | Replay & failure dashboard | R1 injections + R2 replay + R4 metrics + R3 visuals | PLANNED, local SQLite replay test only | actual restart logs and counters |
| 4 | Explainable alerts | R1 real model + R2 rule evidence + R3 UI | rule evidence locally implemented; SHAP NOT AVAILABLE | versioned model or traceable rule reasons |
| 5 | Databricks Genie analyst | R4 backend + R3 UI + R2 governed SQL | 503 not configured; no fake answers | verified entitlement, governed semantic views and SQL reconciliation |
| 6 | Drift/threshold sensitivity | R1 model + R2 evaluation + R3 BI | PLANNED | validation population and score distribution evidence |
| 7 | Stress/latency percentiles | R4 test harness, R1/R2 instrument | PLANNED | samples + environment + p50/p95/p99 measured logs |

All bonus execution is opt-in. `DEMO_FIXTURE` must remain visible for local data; never render synthetic values as live. No SHAP, Genie, scores or feature parity should be fabricated. Power BI is primary rubric BI, optional React never replaces it.
