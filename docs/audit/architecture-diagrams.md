# Verified Architecture & Data Flow Diagrams
## Synthetic Financial Data Ingestion Pipeline (SIC Platform)

**Audit Date:** October 26, 2026  
**AWS Account ID:** `138257835434` | **Region:** `eu-west-3`  
**Classification:** Reconstructed Architecture & Lineage Diagrams

---

## 1. Complete End-to-End Actual Architecture

```mermaid
flowchart TD
    subgraph DataGen["Data Generation & API (ECS Fargate)"]
        API["FastAPI App / Health Probe\n(Port 8000)"]
        GenLoop["generator_service.py\n(_generation_loop)"]
        Pub["KafkaEventPublisher\n(confluent-kafka)"]
    end

    subgraph RelationalDB["Relational Storage (Amazon RDS)"]
        RDS[("PostgreSQL 18.3: containerdb\nTables: users, cards, mcc, cities\nNO TRANSACTIONS COMMITTED")]
        WAL["Write-Ahead Log (WAL)\npgoutput plugin"]
        Slot["Slot: debezium_cdc_slot\nPub: debezium_publication"]
    end

    subgraph StreamingBrokers["Streaming Platform (Amazon MSK)"]
        MSK1["Topic: transactions\n(3 Partitions, RF 2)"]
        MSK2["Topic: users\n(3 Partitions, RF 2)"]
        MSK3["Topic: cards\n(3 Partitions, RF 2)"]
    end

    subgraph ConnectCluster["Kafka Connect Distributed (ECS Fargate)"]
        Deb["Debezium PostgreSQL Connector\n(Task 0)"]
        Router["RegexRouter SMT\n(.*.public.* -> $1)"]
        Sink["Aiven S3 Sink Connector\n(Task 0)"]
    end

    subgraph BatchSystem["Batch Ingestion (EventBridge + ECS)"]
        EB["EventBridge Rule:\nscheduled-ingestion-extractor\n[rate(5 minutes)]"]
        BatchTask["ECS Task: synthetic-ingestion-extractor:5\n(BatchReferenceExtractor)"]
    end

    subgraph FraudInference["Real-Time Fraud Detection (ECS Fargate)"]
        FC["FraudStreamingConsumer\n(Task: sic-fraud-consumer:2)\nManual Offset Commit Guard"]
        Model["LightGBM Classifier Core\n(models/v2/ 450 Trees, <1ms)"]
        MSKAlert["Topic: fraud_alerts\n(2 Partitions, RF 2)"]
    end

    subgraph FeatureStore["Online Feature Store (AWS ElastiCache Redis)"]
        RedisNode[("sic-feature-store (Redis 7.1)\ncache.t4g.micro • In-Transit TLS & Encryption\n8,346 Keys: profile:*, tx_window:*, spend_stats:*")]
    end

    subgraph S3Storage["Amazon S3 Lakehouse Bucket: sic-pipeline-storage"]
        S3RootTx["transactions/{p}/*.json.gz\n[PATH DEFECT: at bucket root]"]
        S3RootUsers["users/{p}/*.json.gz\n[PATH DEFECT: at bucket root]"]
        S3RootCards["cards/{p}/*.json.gz\n[PATH DEFECT: at bucket root]"]
        S3MCC["bronze/mcc/snapshot_date=YYYY-MM-DD/\n*.parquet"]
        S3Cities["bronze/cities/snapshot_date=YYYY-MM-DD/\n*.parquet"]
    end

    subgraph Management["Observability & Secrets"]
        SM["AWS Secrets Manager\nsynthetic-data-provider/database"]
        CW["Amazon CloudWatch Logs\n/ecs/..."]
    end

    %% Data Flows
    GenLoop -->|1. Data Flow: tx_envelope| Pub
    Pub -->|2. Data Flow: Kafka produce| MSK1
    GenLoop -->|3. Data Flow: SQL INSERT users/cards| RDS
    RDS -->|4. CDC/WAL Flow: Commit LSN| WAL
    WAL -->|5. CDC/WAL Flow: Logical replication| Slot
    Slot -->|6. CDC/WAL Flow: pgoutput stream| Deb
    Deb -->|7. Data Flow: Entity change event| Router
    Router -->|8. Data Flow: Remapped topic event| MSK2
    Router -->|8. Data Flow: Remapped topic event| MSK3
    MSK1 -->|9. Data Flow: Micro-batch poll| Sink
    MSK2 -->|9. Data Flow: Micro-batch poll| Sink
    MSK3 -->|9. Data Flow: Micro-batch poll| Sink
    Sink -->|10. Data Flow: Flush Gzip JSONL| S3RootTx
    Sink -->|10. Data Flow: Flush Gzip JSONL| S3RootUsers
    Sink -->|10. Data Flow: Flush Gzip JSONL| S3RootCards

    %% Batch Flow
    EB -->|11. Scheduled Execution: Trigger RunTask| BatchTask
    BatchTask -->|12. Data Flow: SQL query mcc/cities| RDS
    BatchTask -->|13. Data Flow: Snappy Parquet PUT| S3MCC
    BatchTask -->|13. Data Flow: Snappy Parquet PUT| S3Cities

    %% Real-Time ML Fraud Flow
    MSK1 -->|14. Streaming Consume (Parallel)| FC
    FC <-->|Atomic 1h/24h Sliding Window & Profile Hydration (1.9ms)| RedisNode
    FC -->|34-dim Feature Vector| Model
    Model -->|Scoring Decision & Risk Reasons (0.5ms)| FC
    FC -->|15. Publish Enriched Alert & Commit Offset| MSKAlert

    %% Control Plane & Monitoring
    API -.->|Control Plane: Read secret| SM
    BatchTask -.->|Control Plane: Read secret| SM
    API -.->|Monitoring & Logging: logs| CW
    Sink -.->|Monitoring & Logging: logs| CW
    BatchTask -.->|Monitoring & Logging: logs| CW
    FC -.->|Monitoring & Logging: logs| CW
```

---

## 2. Diagram 1: Transaction Direct Streaming Flow

```mermaid
flowchart LR
    A["TransactionsGenerator\n(in-memory PRNG)"] -->|Data Flow: JSON event| B["KafkaEventPublisher\n(confluent-kafka)"]
    B -->|Data Flow: acks=1, key=user_id| C["Amazon MSK Broker\nTopic: transactions"]
    C -->|Data Flow: consumer group sic-connect-cluster| D["Aiven S3 Sink Task 0\n(Kafka Connect)"]
    D -->|Data Flow: gzip JSONL flush| E["S3: transactions/{partition}/\n*.json.gz"]

    subgraph PostgreSQL_Isolation["PostgreSQL Isolation Boundary"]
        F[("PostgreSQL RDS\ntransactions table")]
    end
    A -.->|STRICT GUARANTEE: Zero inserts| F
```

---

## 3. Diagram 2: Users & Cards CDC Pipeline Flow

```mermaid
flowchart TD
    G["generator_service.py\n(Prerequisite generator)"] -->|Data Flow: Atomic SQL INSERT| U[("public.users\n(user_id PK)")]
    G -->|Data Flow: Atomic SQL INSERT| C[("public.cards\n(user_id, card_index PK)")]
    U -->|CDC/WAL Flow: Postgres WAL| W["WAL Engine (pgoutput)"]
    C -->|CDC/WAL Flow: Postgres WAL| W
    W -->|CDC/WAL Flow: Slot LSN| S["Slot: debezium_cdc_slot"]
    S -->|CDC/WAL Flow: Streaming engine| D["Debezium Connector\n(Tasks: 1)"]
    D -->|Data Flow: RegexRouter $1| T1["MSK: users"]
    D -->|Data Flow: RegexRouter $1| T2["MSK: cards"]
    T1 -->|Data Flow: Micro-batch consume| K["Aiven S3 Sink"]
    T2 -->|Data Flow: Micro-batch consume| K
    K -->|Data Flow: JSONL.GZ| S3U["S3: users/{partition}/*.json.gz"]
    K -->|Data Flow: JSONL.GZ| S3C["S3: cards/{partition}/*.json.gz"]
```

---

## 4. Diagram 3: MCC & Cities Batch Ingestion Flow

```mermaid
flowchart TD
    EB["Amazon EventBridge Rule\nscheduled-ingestion-extractor\n[rate(5 minutes)]"] -->|Scheduled Execution: RunTask| Fargate["AWS ECS Fargate Task\nsynthetic-ingestion-extractor:5"]
    Fargate -->|Control Plane: Fetch DB Secret| SM["AWS Secrets Manager"]
    SM -.->|Control Plane: Return URL| Fargate
    Fargate -->|Data Flow: Query 914 rows| T1[("public.merchant_category_codes")]
    Fargate -->|Data Flow: Query 1,433 rows| T2[("public.city_reference")]
    T1 -->|Data Flow: PyArrow Table| P1["Snappy Parquet Buffer\n(27,414 bytes)"]
    T2 -->|Data Flow: PyArrow Table| P2["Snappy Parquet Buffer\n(38,653 bytes)"]
    P1 -->|Data Flow: S3 PUT| S3_1["s3://.../bronze/mcc/\nsnapshot_date=YYYY-MM-DD/*.parquet"]
    P2 -->|Data Flow: S3 PUT| S3_2["s3://.../bronze/cities/\nsnapshot_date=YYYY-MM-DD/*.parquet"]
```

---

## 5. Diagram 4: Deployment & Network Relationships

```mermaid
graph TD
    subgraph VPC_172_31["AWS VPC: 172.31.0.0/16"]
        subgraph Subnet_AZ1["Subnet eu-west-3a (subnet-003d3891dab36b354)"]
            Broker1["MSK Broker 1\n(b-1...:9092, 9094)"]
            TaskAPI["ECS Task: synthetic-data-provider:4\n(Port 8000)"]
            TaskExt["ECS Task: synthetic-ingestion-extractor:5\n(Batch Extractor)"]
        end

        subgraph Subnet_AZ2["Subnet eu-west-3b (subnet-0b18944bde732d1fd)"]
            Broker2["MSK Broker 2\n(b-2...:9092, 9094)"]
            TaskConnect["ECS Task: sic-kafka-connect:6\n(Port 8083)"]
            RDS[("PostgreSQL RDS: containerdb\n(Port 5432 - Non-Public)")]
        end

        subgraph Subnet_AZ3["Subnet eu-west-3c (subnet-03fa02b6eabf60d25)"]
            RedisCluster[("ElastiCache Redis: sic-feature-store\n(Port 6379 - TLS Enabled)")]
            TaskConsumer["ECS Task: sic-fraud-consumer:2\n(Real-Time ML Scorer)"]
        end
    end

    subgraph Security_Groups["Security Group Boundary"]
        AppSG["synthetic-app-sg (sg-0dd0933f4da2b5ea7)"]
        DBSG["default (sg-0041172e0d9b6c810)"]
        MSKSG["sg-0ddf4e9366885421b"]
        RedisSG["sic-redis-sg (sg-045ac4d82b2de70c6)"]
    end

    AppSG -->|Port 5432 (Postgres Ingress)| DBSG
    AppSG -->|Port 9092/9094 (MSK Ingress)| MSKSG
    AppSG -->|Port 6379 (Redis TLS Ingress)| RedisSG
```

---

## 6. Diagram 5: Real-Time Fraud Detection & ElastiCache Redis Feature Store Pipeline

```mermaid
sequenceDiagram
    autonumber
    participant MSK as Amazon MSK (Topic: transactions)
    participant FC as FraudStreamingConsumer (sic-fraud-consumer:2)
    participant Redis as AWS ElastiCache Redis (sic-feature-store)
    participant LGBM as LightGBM Core (models/v2/)
    participant AlertTopic as Amazon MSK (Topic: fraud_alerts)

    MSK->>FC: 1. Poll streaming transaction (key: user_id)
    FC->>Redis: 2. Pipeline: GET last_tx, ZCOUNT 1h/24h, ZRANGE 1h, HGETALL spend_stats
    Redis-->>FC: 3. Hydrate rolling features (1.11ms)
    FC->>FC: 4. Extract 34-dim feature vector (time diff, travel velocity, spend spikes)
    FC->>LGBM: 5. Apply LightGBM Classifier (predict_proba)
    LGBM-->>FC: 6. Return fraud probability & explainable reasons (0.5ms)
    opt Fraud Detected (Score >= 0.046)
        FC->>AlertTopic: 7. Publish enriched alert (event_id, reasons, score, latency)
        AlertTopic-->>FC: 8. Broker Delivery ACK
    end
    FC->>Redis: 9. Pipeline: ZADD window, ZREMRANGE, HINCRBY stats, SET NX dedup (1.94ms)
    Redis-->>FC: 10. Atomic write confirmation
    FC->>MSK: 11. Synchronous Kafka Offset Commit (Zero Missed Alerts)
```

