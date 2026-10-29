# Deep Dive Technical Architecture: AWS ElastiCache for Redis Online Feature Store
## Real-Time Transaction Scoring & Fraud Detection Pipeline

---

## 1. System Overview and Topology

The fraud detection streaming engine processes real-time financial transaction events emitted from the synthetic transaction provider. The core latency requirement for real-time inference is **< 10ms per transaction**, encompassing Kafka payload ingestion, online feature hydration, LightGBM model evaluation, alert dispatch, and manual offset commitment.

```mermaid
flowchart TB
    subgraph VPC["AWS VPC: vpc-0d66731212396d1e9 (172.31.0.0/16) | Region: eu-west-3"]
        subgraph MSK_TIER["Streaming Tier: Amazon MSK (Cluster: sic-streaming-msk)"]
            T_TX[("Topic: transactions\n(Partitioned Stream)")]
            T_ALERT[("Topic: fraud_alerts\n(Enriched Security Alerts)")]
        end

        subgraph ECS_TIER["Compute Tier: ECS Fargate (Cluster: synthetic-data-cluster)"]
            subgraph TASK_FC["Task: sic-fraud-consumer:2 (0.25 vCPU, 1 GB RAM)"]
                C_KAFKA["confluent_kafka Consumer\n(Group: sic-fraud-consumer-group)"]
                C_STATE["StateStore Engine\n(Pipeline Batching + In-Memory Fallback)"]
                C_FE["FeatureExtractor\n(34 Deterministic Features)"]
                C_ML["LightGBM Classifier\n(fraud_lgbm_model.joblib)"]
                C_PROD["confluent_kafka Producer\n(Synchronous Broker Flush)"]
            end
        end

        subgraph REDIS_TIER["Online Feature Store Tier: AWS ElastiCache for Redis"]
            subgraph REDIS_REP["Replication Group: sic-feature-store (Node: cache.t4g.micro)"]
                REDIS_NODE[("ElastiCache Redis 7.1.0\nPrimary Endpoint: master.sic-feature-store...\nPort: 6379 | TLS Enabled")]
            end
        end

        subgraph SG_SEC["Security Boundaries & VPC Subnets"]
            SG_APP["Container SG: synthetic-app-sg\n(sg-0dd0933f4da2b5ea7)"]
            SG_REDIS["Redis SG: sic-redis-sg\n(sg-045ac4d82b2de70c6)"]
        end
    end

    %% Network Connections
    T_TX -->|Poll Message| C_KAFKA
    C_KAFKA -->|Raw JSON| C_STATE
    C_STATE <-->|TLS rediss:// Pipeline Reads/Writes| REDIS_NODE
    C_STATE -->|Hydrated State| C_FE
    C_FE -->|34-dim Feature Vector| C_ML
    C_ML -->|Prediction & Prob| C_PROD
    C_PROD -->|Emit Alert if Prob >= 0.20| T_ALERT
    C_PROD -.->|Flush Ack -> Commit Offset| C_KAFKA

    %% Security Associations
    SG_APP -.->|Inbound TCP 6379 Authorized| SG_REDIS
    TASK_FC -.->|Runs with SG| SG_APP
    REDIS_NODE -.->|Guarded by SG| SG_REDIS
```

---

## 2. End-to-End Transaction Processing Sequence

The sequence diagram below models the exact transaction processing lifecycle implemented in [`src/ml/fraud_consumer.py`](file:///home/syd/Downloads/archive/src/ml/fraud_consumer.py) and [`src/ml/state_store.py`](file:///home/syd/Downloads/archive/src/ml/state_store.py).

```mermaid
sequenceDiagram
    autonumber
    participant MSK_IN as MSK (transactions)
    participant FC as FraudConsumer
    participant SS as StateStore
    participant REDIS as ElastiCache Redis
    participant EXT as FeatureExtractor
    participant LGBM as LightGBM Model
    participant MSK_OUT as MSK (fraud_alerts)

    MSK_IN->>FC: poll(timeout=1.0) -> Consume Transaction Msg
    Note over FC: Parse JSON Envelope & Payload (Amount, User, Card, Lat, Lon, Time)

    FC->>SS: get_and_update_state(user, card, amount, epoch, lat, lon, event_id)
    
    activate SS
    Note over SS,REDIS: Pipeline 1: Query Prior State (Sub-ms)
    SS->>REDIS: BATCH [GET last_tx, ZCOUNT 1h, ZCOUNT 24h, ZRANGEBYSCORE 1h, HGETALL stats]
    REDIS-->>SS: Return [last_tx, count_1h, count_24h, amounts_1h, stats_hash]

    Note over SS: Calculate Running Mean & Variance from Prior State
    
    Note over SS,REDIS: Dedup Check (Idempotency)
    SS->>REDIS: SET dedup:{event_id} "1" NX EX 172800
    REDIS-->>SS: Return True (New) or Nil (Duplicate)

    alt Event is NOT Duplicate
        Note over SS,REDIS: Pipeline 2: Atomic State Mutation
        SS->>REDIS: BATCH [SET last_tx, ZADD tx_window, ZREMRANGEBYSCORE old, HINCRBY stats]
        REDIS-->>SS: Confirm Execution
    else Event IS Duplicate
        Note over SS: Skip State Mutation (Prevent Count Drift)
    end
    SS-->>FC: Return Hydrated State Dict
    deactivate SS

    FC->>EXT: extract_online_features(payload, user_profile, card_profile, state)
    EXT-->>FC: Return 34 Features Dict
    FC->>EXT: to_feature_vector(fdict) -> Return ordered List[float]

    FC->>LGBM: predict_proba(X_vector)
    LGBM-->>FC: Return fraud_probability (e.g. 0.851)

    alt is_fraud_predicted == True (prob >= threshold)
        FC->>MSK_OUT: produce(topic='fraud_alerts', key=user_id, value=alert_json)
        FC->>MSK_OUT: flush(timeout=5.0)
        MSK_OUT-->>FC: Delivery Acknowledged
    end

    FC->>MSK_IN: commit(message, asynchronous=False)
    Note over FC,MSK_IN: Manual Synchronous Offset Commit Successful
```

---

## 3. Redis Key Schema and Data Modeling

| Key Pattern | Data Structure | Encoding / Fields | Purpose in Fraud Engine | TTL Policy | Memory Overhead |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `profile:user:{user_id}` | **String (JSON)** | Demographic fields: `current_age`, `fico_score`, `yearly_income`, `total_debt`, `latitude`, `longitude` | Static customer reference profile used to calculate `distance_to_home_km` and credit risk. | Permanent (No TTL) | ~320 bytes / user |
| `profile:card:{user_id}:{card_idx}` | **String (JSON)** | Card dimensions: `card_brand`, `card_type`, `has_chip`, `credit_limit`, `card_on_dark_web` | Static card dimensions used to evaluate limit utilization and compromised status. | Permanent (No TTL) | ~280 bytes / card |
| `last_tx:{user_id}:{card_idx}` | **String (JSON)** | `{"lat": float, "lon": float, "epoch": float}` | Records preceding GPS fix and timestamp to evaluate Great-Circle velocity (`travel_speed_kmh`). | 48 Hours (`172800s`) | ~120 bytes / card |
| `tx_window:{user_id}:{card_idx}` | **Sorted Set (ZSET)** | **Score:** `epoch_sec` (Float timestamp)<br>**Member:** `"{amount}:{epoch_sec}"` | High-efficiency rolling window. Enables $O(\log N + M)$ queries for 1h/24h counts and sliding spend summation. | 48 Hours (`172800s`) | ~64 bytes / tx entry |
| `spend_stats:{user_id}` | **Hash** | `total_sum`: Float<br>`total_count`: Integer<br>`total_sq_sum`: Float | Maintains running historical totals for online Welford/sample variance and mean calculation without keeping arrays. | 48 Hours (`172800s`) | ~150 bytes / user |
| `dedup:{event_id}` | **String** | Value: `"1"` | Idempotency lock via `SET NX EX`. Prevents duplicated Kafka replays from artificially inflating rolling transaction counts. | 48 Hours (`172800s`) | ~60 bytes / event |

---

## 4. State Lifecycle and Pruning Strategy

### Sliding Window Ingestion and Pruning
To prevent unbounded memory growth in ElastiCache, rolling transaction histories are pruned continuously during the write phase:
1. When a transaction arrives at time $t_{\text{now}}$, the 24-hour cutoff threshold is calculated:
   $$t_{\text{cutoff}} = t_{\text{now}} - 86400.0$$
2. The current event is inserted into the card's Sorted Set:
   `ZADD tx_window:{user}:{card} t_now "{amount}:{t_now}"`
3. All entries with a score strictly below $t_{\text{cutoff}}$ are pruned in the same pipeline execution:
   `ZREMRANGEBYSCORE tx_window:{user}:{card} -inf t_cutoff`
4. The key-level expiration timer is reset to 48 hours:
   `EXPIRE tx_window:{user}:{card} 172800`

### Running Variance and Sample Standard Deviation
Rather than maintaining an unbounded list of prior transaction amounts in Redis, `spend_stats:{user_id}` stores aggregate accumulators:
$$\mu = \frac{\sum x}{N}$$
$$\sigma = \sqrt{\frac{\sum x^2 - \frac{(\sum x)^2}{N}}{N - 1}} \quad (\text{for } N \ge 2)$$

- **Prior-State Invariant:** When transaction $k$ arrives with amount $x_k$, the mean $\mu$ and standard deviation $\sigma$ are calculated from the existing Redis Hash values *before* applying the current transaction.
- **Atomic Hash Increments:**
  - `HINCRBYFLOAT spend_stats:{user_id} total_sum x_k`
  - `HINCRBY spend_stats:{user_id} total_count 1`
  - `HINCRBYFLOAT spend_stats:{user_id} total_sq_sum (x_k * x_k)`

---

## 5. Security Architecture and Network Isolation

```text
       INTERNET
          |
       [ BLOCKED ] (No Public IP, No Internet Gateway Routing to Redis)
          |
+---------v-----------------------------------------------------------------------+
|  AWS VPC: vpc-0d66731212396d1e9 (172.31.0.0/16)                                |
|                                                                                 |
|  +-------------------------------------+   +---------------------------------+  |
|  | Subnet: subnet-003d3891dab36b354   |   | Subnet: subnet-0b18944bde732d1fd |  |
|  | (eu-west-3a - Private Workload)    |   | (eu-west-3b - Private Feature)  |  |
|  |                                     |   |                                 |  |
|  |  [ ECS Fargate Container ]          |   |  [ ElastiCache Redis Node ]     |  |
|  |  Security Group: synthetic-app-sg   |   |  Security Group: sic-redis-sg   |  |
|  |  (sg-0dd0933f4da2b5ea7)             |   |  (sg-045ac4d82b2de70c6)         |  |
|  |                                     |   |                                 |  |
|  |  Role: syntheticTaskRole            |   |  Storage: sse-elasticache       |  |
|  |  Traffic: Outbound TCP 6379 --------+---> Inbound: TCP 6379 (TLS Only)    |  |
|  +-------------------------------------+   +---------------------------------+  |
+---------------------------------------------------------------------------------+
```

### Verified Security Controls:
1. **Network Layer Isolation:**
   - Redis cluster nodes are situated within private subnets (`sic-redis-subnets`). No public DNS records or elastic IPs are assigned.
2. **Security Group Ingress Rule:**
   - Security group `sic-redis-sg` (`sg-045ac4d82b2de70c6`) allows inbound TCP traffic on port `6379` strictly from `sg-0dd0933f4da2b5ea7` (`synthetic-app-sg`). All other IP CIDRs and security groups are dropped.
3. **In-Transit Encryption (TLS):**
   - ElastiCache is configured with `TransitEncryptionMode: required`. Client connections must use the TLS wrapper protocol (`rediss://`). Unencrypted plaintext connections on port 6379 are refused at the TLS handshake level.
4. **At-Rest Encryption:**
   - Storage volume encryption is enabled via `StorageEncryptionType: sse-elasticache` (AES-256).

---

## 6. Failure Modes, Edge Cases, and Recovery Behavior

```mermaid
flowchart TD
    START(["Process Transaction Event"]) --> REDIS_CHECK{"Is Redis Available?"}
    
    REDIS_CHECK -- "Yes" --> REDIS_PIPE["Execute Redis Pipeline (1)"]
    REDIS_CHECK -- "Connection Failed / Timeout" --> FALLBACK_WARN["Log Observable WARNING\nDegrade to In-Memory Fallback"]
    
    REDIS_PIPE -- "Pipeline Throws Exception" --> FALLBACK_WARN
    REDIS_PIPE -- "Success" --> CALC_FEATURES["Calculate Features & Evaluate LightGBM"]
    FALLBACK_WARN --> CALC_LOCAL["Calculate Features Using Process RAM Dictionaries"]
    
    CALC_LOCAL --> CALC_FEATURES
    CALC_FEATURES --> FRAUD_CHECK{"Fraud Predicted?"}
    
    FRAUD_CHECK -- "No (PASS)" --> COMMIT_OFFSET["consumer.commit(asynchronous=False)"]
    FRAUD_CHECK -- "Yes (ALERT)" --> EMIT_ALERT["producer.produce(topic='fraud_alerts')"]
    
    EMIT_ALERT --> FLUSH_CHECK{"producer.flush(timeout=5.0) Ack?"}
    FLUSH_CHECK -- "Ack Received (unflushed==0)" --> COMMIT_OFFSET
    FLUSH_CHECK -- "Timeout / Error (unflushed > 0)" --> ABORT_OFFSET["Log ERROR & Raise RuntimeError\nDO NOT COMMIT OFFSET"]
    
    COMMIT_OFFSET --> FINISH(["Event Complete"])
    ABORT_OFFSET --> KAFKA_RETRY(["Kafka Re-delivers Message on Poll"])
```

### Behavior Under Specific Failures:
1. **Redis Unavailable at Cold Start:**
   - If Redis is unreachable during `FraudStreamingConsumer.__init__`, `StateStore._connect()` catches the socket timeout, logs a warning, and sets `_using_fallback = True`. The consumer continues operating entirely within container RAM.
2. **Redis Fails Mid-Stream:**
   - If a Redis read/write pipeline throws a connection drop or timeout during stream processing, the exception is trapped in `state_store.py` (line 330). An observable warning is logged with the event ID, and execution falls back to `_local_user_tx_history` and `_local_user_spend_stats`.
3. **Alert Dispatch Failure vs Offset Commit:**
   - If LightGBM predicts fraud and `producer.produce()` fails or `producer.flush(5.0)` times out, an explicit `RuntimeError` is raised. 
   - `consumer.commit(asynchronous=False)` is **not executed**. The consumer loop pauses for 1.0s and retries on the next poll cycle, guaranteeing that alerts are never silently skipped.

---

## 7. What This Design Does NOT Guarantee

To maintain rigorous technical integrity and prevent unrealistic production assumptions, the following boundaries must be clearly understood:

1. **No Distributed Synchronization Between Fallback and Redis (State Split):**
   - If Redis fails temporarily and the consumer falls back to in-memory mode, any transactions processed during that window are stored exclusively in container RAM. 
   - When Redis recovers, **no dual-write backfill occurs**. State recorded in local RAM is not retroactively committed to Redis, creating a permanent gap in the shared rolling window.
2. **No Exactly-Once Delivery (Duplicate Alerts Can Occur):**
   - Kafka does not support atomic distributed transactions across disparate external systems without two-phase commit overhead.
   - If an alert is published and acknowledged by MSK, but the consumer process crashes before `consumer.commit(...)` finishes, the message will be redelivered upon task restart, emitting a **duplicate alert**. Downstream consumers of `fraud_alerts` must deduplicate by `event_id`.
3. **No High Availability Failover on Current Node (Demo Sizing):**
   - The verified cluster `sic-feature-store-001` is deployed on a single node `cache.t4g.micro` with `MultiAZ: disabled` and `AutomaticFailover: disabled`. If AWS hardware underlying `eu-west-3b` experiences an outage, Redis will be unavailable until manually replaced or rebooted.
4. **No Cross-Region Disaster Recovery:**
   - All resources reside exclusively in Paris (`eu-west-3`). No global replication groups or cross-region replication links are configured.
