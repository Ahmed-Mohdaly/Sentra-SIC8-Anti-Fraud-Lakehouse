# Team graduation demo — 7–10 minutes (planned)

0:00–1:00 R1: official fraud scope, three channels, Kafka architecture; state explicitly live vs fixture.
1:00–2:00 R1: generate deterministic three fraud scenarios; show Kafka topic + S3 proof *only if real run is verified*.
2:00–4:00 R2: raw vs Silver, schema quality, duplicate/replay gate, truth separation, Spark/Delta MERGE/time travel if actually executed; explain historical timestamp/geo limits.
4:00–5:30 R2: 10-min account velocity, fraud rate against truth, terminal failures, fact grain/Gold SQL; honest performance, no fabricated detector accuracy.
5:30–7:00 R3: Power BI historical and live pages + measured SQL reconciliation and alerts; currency/source-scope labels.
7:00–8:30 R4: Airflow, security, observability and documented restart/failure recovery; present actual logs.
8:30–10:00 optional bonuses: transaction trace and Genie *only if connected*; otherwise show clearly marked local fixture and missing stages.

**Fallback (explicit DEMO_FIXTURE):** `make demo-local`, inspect `build/demo/report.json` and guarded `/trace/{event_id}`. This cannot replace actual required live system validation and must be introduced as a reference demo.
