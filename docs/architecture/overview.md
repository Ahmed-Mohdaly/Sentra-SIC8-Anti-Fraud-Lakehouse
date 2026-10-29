# High-level system architecture

```mermaid
flowchart TD
    subgraph Source
      H[IBM/Kaggle historical CSV]
      G[R1 payment simulator]
      PG[PostgreSQL users/cards]
      REF[MCC/Cities reference extract]
    end
    G-->K[Kafka / MSK]
    PG-->D[Debezium CDC]-->K
    K-->S3[S3 raw transaction and CDC]
    H-->RAW[S3 raw baseline]
    REF-->S3
    S3-->B[Databricks streaming Bronze parser]
    RAW-->HB[Historical batch Spark]
    B-->S[Delta Silver]
    HB-->S
    S-->F[R2 fraud rules and model-result normalization]
    F-->GO[Delta Gold facts/dimensions]
    GO-->BI[R3 Power BI]
    GO-.governed SELECT.->API[Optional R4 API / R3 React]
    AIR[R4 Airflow]-.coordinate batch, monitor stream.->S
```

**Verified vs designed:** The exact deployed R1 configuration is only team-reported, some S3 object prefixes were observed via read-only connected AWS inventory; actual R1 code unavailable here. The new local generator and SQLite data are *fixtures* and not replayable proof of MSK/Delta integration. Historical Gold publication is not verified. Airflow scheduling of perpetual streams must monitor health, not wait for process to finish.
