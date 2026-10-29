# Fraud detection semantics

- CNP Burst: event-time ECOMMERCE positive approved attempts by same verified account/card within `(t-10m,t]`; threshold configurable and not inferred from historical test results. Reject duplicated event IDs.
- Geo Velocity: compare **exact valid synthetic GPS** points for same card/account, UTC timestamps, positive elapsed time, enough physical distance (e.g. 5000 miles in 15 minutes as official example). City-centroid location => `NOT_EVALUATED`; never treat zeros as missing GPS.
- Micro-testing: a burst of small positive card-not-present attempts below configured amount threshold. Refunds and declines require explicit scenario policy; no use of labels in decisions.
- Rules produce separate, stable `alert_id` for each event/rule; online LightGBM should emit a prediction/status for **every eligible event**. Missing artifact => no fabricated score or alert. Keep source truth isolated and evaluation-only.
- The **local** rules in `src/r2_spark/rules.py` are reference batch-sorted tests, not a validated Spark state store; actual watermark, late events, SCD2 lookups, and streaming cost performance remain blocked.
