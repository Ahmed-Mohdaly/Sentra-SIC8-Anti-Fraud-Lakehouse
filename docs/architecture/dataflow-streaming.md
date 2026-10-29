# Streaming event sequence

```mermaid
sequenceDiagram
    participant G as R1 Generator
    participant K as Kafka
    participant S as S3 Sink
    participant B as R2 Bronze/Silver
    participant ML as R1 Model / R2 Rules
    participant GO as R2 Gold
    participant BI as R3 Dashboard
    G->>K: event_id, transaction_id, UTC, amount/currency
    G-->>G: write isolated truth sidecar
    K->>S: at-least-once delivery + partition offsets
    S->>B: compressed JSONL files (micro-batch arrival)
    B->>B: unwrap, validate, quarantine, deterministic merge
    B->>ML: canonical features, no labels
    ML->>GO: prediction status for all + distinct alerts
    GO->>BI: governed KPIs, freshness + source scope
```

Direct Kafka online inference may have a lower latency path (R1 claimed), which is **not the same** as file-arrival micro-batch latency via S3. Neither end-to-end latency path is verified in this repo; benchmark separately with trace IDs, timestamps and actual logs.
