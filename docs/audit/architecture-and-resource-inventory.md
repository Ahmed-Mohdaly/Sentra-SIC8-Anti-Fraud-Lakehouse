# Architecture and AWS Cloud Resource Inventory
## Synthetic Financial Data Ingestion Pipeline (SIC Platform)

**Audit Date:** October 26, 2026  
**AWS Account ID:** `138257835434`  
**AWS Region:** `eu-west-3` (Paris)  
**Inspection Method:** Read-Only AWS CLI, Boto3 SDK, Source Code & CloudWatch Log Inspection

---

## 1. Comprehensive Component Inventory Table

| Component | Purpose | Actual Resource Identifier (ARN / ID) | Observed Runtime Status | Network Path (VPC / Subnet / SG) | Data Input | Data Output | Configuration Evidence | Runtime Evidence | Identified Gaps / Risks |
|---|---|---|---|---|---|---|---|---|---|
| **Synthetic Transaction Generator** | Generates synthetic transactions and entity prerequisites in memory | Service: `synthetic-data-service`<br>Task Def: `synthetic-data-provider:4` | `ACTIVE` (1/1 running) | VPC: `vpc-01ef046927c3da33f`<br>Subnets: `subnet-003d3891dab36b354`, `subnet-0b18944bde732d1fd`<br>SG: `sg-0dd0933f4da2b5ea7` | In-memory PRNG & baseline CSVs (`/data`) | MSK `transactions` (stream)<br>RDS PostgreSQL `users` & `cards` (SQL) | ECS Task Def: `synthetic-data-provider:4`<br>Source: `src/synthetic_streaming/generator_service.py` | CloudWatch Log Group: `/ecs/synthetic-data-provider`<br>HTTP probe: `GET /health` | No async delivery retry callback in `publish()`. Endpoint `/transactions` reads stale PostgreSQL data. |
| **Amazon MSK Cluster** | High-throughput distributed message broker | ARN: `arn:aws:kafka:eu-west-3:138257835434:cluster/sic-streaming-msk/ff954d9e-7cba-437e-b180-83ad115d0996-2` | `ACTIVE` | Client Subnets: `subnet-003d3891dab36b354`, `subnet-0b18944bde732d1fd`<br>SG: `sg-0ddf4e9366885421b`<br>Public Access: `DISABLED` | Producers: `generator_service.py`, Debezium CDC | Consumers: Aiven S3 Sink Connector | AWS CLI: `aws kafka describe-cluster`<br>Version: Apache Kafka `3.8.x`<br>Brokers: 2x `kafka.t3.small`<br>EBS: 10 GB/broker | Live broker endpoints active on ports 9092 & 9094.<br>Active topics: `transactions`, `users`, `cards`, connect topics. | Documentation claimed 50 GB EBS and Kafka 3.6.0; actual AWS reality is 10 GB and Kafka 3.8.x. |
| **Amazon RDS PostgreSQL** | Relational OLTP storage for user & card accounts | DB Identifier: `containerdb`<br>Endpoint: `containerdb.cjm6iag8qrwi.eu-west-3.rds.amazonaws.com:5432` | `available` | VPC: `vpc-0d66731212396d1e9`<br>Subnet Group: `default-vpc-0d66731212396d1e9`<br>SG: `sg-0041172e0d9b6c810`<br>Publicly Accessible: `false` | Generator Service (SQL INSERT)<br>Admin runner | PostgreSQL WAL -> Debezium CDC slot (`debezium_cdc_slot`) | AWS CLI: `aws rds describe-db-instances`<br>Instance: `db.t4g.micro`<br>Engine: PostgreSQL 18.3<br>Parameter Group: `synthetic-pg18-cdc` | Verified table counts: `users` (3,126+), `cards` (9,551+), `mcc` (914), `cities` (1,433), `transactions` (32,353 baseline). | Logical replication slot retention risk: if Debezium stops, WAL will accumulate on the 20 GB gp3 storage. |
| **Debezium CDC Source Connector** | Captures WAL row-level changes from PostgreSQL | Connector: `debezium-postgres-source`<br>Class: `io.debezium.connector.postgresql.PostgresConnector` | `RUNNING` (Task 0 `RUNNING`) | Runs inside `sic-kafka-connect-service` (ECS Fargate)<br>Port: 8083 (Internal) | PostgreSQL WAL via `pgoutput` plugin | MSK topics `users` and `cards` (via `RegexRouter`) | Connector config: `scripts/fix_debezium_config.py`<br>REST API: `GET /connectors/debezium-postgres-source/status` | S3 JSONL records containing Debezium 2.7.3 envelopes with before/after payloads and LSNs. | Topic rerouting relies on RegexRouter. Schema evolution on PostgreSQL tables requires testing. |
| **Aiven S3 Sink Connector** | Flushes MSK streaming events to compressed JSONL in S3 | Connector: `aiven-s3-sink`<br>Class: `io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector` | `RUNNING` (Task 0 `RUNNING`) | Runs inside `sic-kafka-connect-service` (ECS Fargate)<br>Port: 8083 (Internal) | MSK topics `transactions`, `users`, `cards` | Amazon S3 bucket `sic-pipeline-storage` | Connector config: `scripts/fix_s3_sink_sts.py`<br>Task Def: `sic-kafka-connect:4` | S3 objects: `transactions/0/*.json.gz`, `users/0/*.json.gz`, `cards/0/*.json.gz` | **Defect:** `file.name.template` omits `{{file_name_prefix}}`, causing files to land at bucket root rather than `bronze/`. |
| **Batch Reference Extractor** | Extracts static reference dimensions to columnar Parquet | Task Def: `synthetic-ingestion-extractor:5`<br>Script: `src/ingestion/batch_reference_extractor.py` | `ACTIVE` (Executed periodically) | Subnets: `subnet-003d3891dab36b354`, `subnet-0b18944bde732d1fd`<br>SG: `sg-0dd0933f4da2b5ea7`<br>Fargate: 256 CPU, 512 MB | RDS PostgreSQL tables `merchant_category_codes` and `city_reference` | S3 Bronze `bronze/mcc/` and `bronze/cities/` (Parquet) | Task Def: `synthetic-ingestion-extractor:5`<br>Source: `src/ingestion/batch_reference_extractor.py` | CloudWatch log stream `scheduled/ingestion-extractor/ea93c1abb...`: 914 MCC & 1,433 Cities persisted. | **Defect:** EventBridge rule runs at `rate(5 minutes)`, generating 288 duplicate snapshots per day instead of daily snapshots. |
| **Amazon EventBridge Rule** | Schedules periodic execution of the reference extractor | Rule ARN: `arn:aws:events:eu-west-3:138257835434:rule/scheduled-ingestion-extractor` | `ENABLED` | IAM Role: `arn:aws:iam::138257835434:role/ecsEventsRole` | Clock trigger (`rate(5 minutes)`) | ECS Fargate `RunTask` on `synthetic-data-cluster` | AWS CLI: `aws events list-rules`<br>`aws events list-targets-by-rule` | CloudWatch log streams created every 5 minutes in `/ecs/synthetic-ingestion-extractor`. | Schedule expression `rate(5 minutes)` is excessive for static reference tables. |
| **Amazon S3 Bronze Bucket** | Raw storage layer for the data lakehouse | Bucket: `sic-pipeline-storage` | `ACTIVE` | Regional S3 Endpoint (`eu-west-3`)<br>Encryption: SSE-S3 (AES256)<br>Public Access: Blocked | Kafka Connect S3 Sink<br>Batch Reference Extractor | Downstream Silver / Gold ETL, Athena, Glue | AWS CLI: `aws s3api get-bucket-encryption`, `get-public-access-block` | S3 listings show active Parquet files in `bronze/` and JSONL.GZ files at bucket root. | No S3 Lifecycle Policy configured (`NoSuchLifecycleConfiguration`). Dual directory structure (`bronze/` vs root). |
| **AWS Secrets Manager** | Encrypted credential store for relational database | Secret ARN: `arn:aws:secretsmanager:eu-west-3:138257835434:secret:synthetic-data-provider/database-w5PpeT` | `ACTIVE` | Regional KMS encrypted (`aws/secretsmanager`) | Administrator / RDS rotation | Injected dynamically into ECS Task Definitions | AWS CLI: `aws secretsmanager describe-secret`<br>ECS Task Def secrets block | Task boots successfully and authenticates to RDS without plaintext passwords in task definitions. | Automated scheduled rotation is not configured on the secret. |
| **ECS Task Execution Role** | IAM pre-launch bootstrap role | Role: `ecsTaskExecutionRole`<br>ARN: `arn:aws:iam::138257835434:role/ecsTaskExecutionRole` | `ACTIVE` | IAM Global | AWS ECS Agent | ECR pulls, CloudWatch log streams, Secrets Manager | AWS CLI: `aws iam list-role-policies`<br>Policy: `SecretsManagerReadDatabaseSecret` | Tasks launch cleanly and pull ECR images without permission errors. | Scoped to `secret:synthetic-data-provider/database*` (least privilege compliant). |
| **ECS Runtime Task Role** | IAM runtime application authorization | Role: `syntheticTaskRole`<br>ARN: `arn:aws:iam::138257835434:role/syntheticTaskRole` | `ACTIVE` | IAM Global | Application process (Python / Java) | S3 Bronze bucket, DynamoDB watermarks, CloudWatch metrics, STS Self-Assume | AWS CLI: `aws iam get-role-policy`<br>Policy: `SyntheticDataRuntimePermissions` | S3 Sink successfully assumes role and writes objects using regional STS. | Policy contains legacy DynamoDB and ECS discovery permissions from previous extractor revisions. |
| **AWS ElastiCache Redis** | Shared online feature store for streaming fraud inference | Replication Group: `sic-feature-store`<br>Node Type: `cache.t4g.micro` | `available` | Subnets: `subnet-003d3891dab36b354`, `subnet-0b18944bde732d1fd`, `subnet-03fa02b6eabf60d25`<br>SG: `sg-045ac4d82b2de70c6`<br>Port: 6379 (TLS `rediss://`) | Preloaded static profiles (`/data`) & streaming transaction sliding windows | Online feature vectors (1.11ms read, 1.94ms write) | Redis 7.1, In-Transit Encryption (TLS), At-Rest Encryption (KMS), 48h TTL | Live keyspace verified: 8,346 active keys (`profile:*`, `tx_window:*`, `spend_stats:*`) | Single-node demo instance; for mission-critical HA, enable Multi-AZ with automatic failover. |
| **Real-Time Fraud Detection Consumer** | Scires streaming transactions with LightGBM in real-time | Task Def: `sic-fraud-consumer:2`<br>Container: `fraud-consumer` | `RUNNING` (Task: `e7339964e9...`) | Subnet: `subnet-03fa02b6eabf60d25`<br>SG: `sg-0dd0933f4da2b5ea7`<br>Fargate: 512 CPU, 1024 MB | MSK topic `transactions`<br>ElastiCache Redis feature store | MSK topic `fraud_alerts`<br>CloudWatch Logs `/ecs/synthetic-data-provider` | Task Def: `sic-fraud-consumer:2`<br>Source: `src/ml/fraud_consumer.py` | CloudWatch log stream: 6-8ms end-to-end inference latency, alerts delivered, manual offset commits verified. | None; automatic fallback to local RAM state ensures high availability if Redis degrades. |

---

## 2. Infrastructure Topology & Networking Relationships

```mermaid
graph TB
    subgraph VPC["AWS VPC: vpc-01ef046927c3da33f / vpc-0d66731212396d1e9 (172.31.0.0/16)"]
        subgraph SubnetA["Subnet eu-west-3a: subnet-003d3891dab36b354"]
            Broker1["MSK Broker 1\nb-1...:9092"]
            TaskAPI["FastAPI / Generator\nsynthetic-data-provider:4"]
            TaskExt["Batch Extractor Task\nsynthetic-ingestion-extractor:5"]
        end

        subgraph SubnetB["Subnet eu-west-3b: subnet-0b18944bde732d1fd"]
            Broker2["MSK Broker 2\nb-2...:9092"]
            TaskConnect["Kafka Connect Distributed\nsic-kafka-connect:6"]
            RDS[("PostgreSQL 18.3: containerdb\n(sg-0041172e0d9b6c810)")]
        end

        subgraph SubnetC["Subnet eu-west-3c: subnet-03fa02b6eabf60d25"]
            RedisCluster[("ElastiCache Redis: sic-feature-store\ncache.t4g.micro • Port 6379 TLS")]
            TaskConsumer["Fraud Consumer Task\nsic-fraud-consumer:2"]
        end
    end

    subgraph SecurityGroups["Security Group Ingress Topology"]
        AppSG["synthetic-app-sg (sg-0dd0933f4da2b5ea7)\n- Port 8000: 172.31.0.0/16 & 197.43.198.206/32\n- Port 8083: Internal VPC only"]
        MSKSG["sg-0ddf4e9366885421b\n- Port 9092/9094: sg-0dd0933f4da2b5ea7"]
        DBSG["sg-0041172e0d9b6c810\n- Port 5432: sg-0dd0933f4da2b5ea7 only"]
        RedisSG["sic-redis-sg (sg-045ac4d82b2de70c6)\n- Port 6379: sg-0dd0933f4da2b5ea7 only"]
    end

    subgraph AWS_Managed["AWS Managed Services"]
        S3[("S3 Bronze Bucket\nsic-pipeline-storage")]
        SM["AWS Secrets Manager\nsynthetic-data-provider/database"]
        CW["Amazon CloudWatch Logs & Metrics"]
        EB["Amazon EventBridge"]
    end

    TaskAPI -->|Port 9092 (Streaming)| Broker1
    TaskAPI -->|Port 5432 (SQL INSERT)| RDS
    RDS -->|WAL Logical Replication (Slot)| TaskConnect
    TaskConnect -->|Port 9092 (Produce users/cards)| Broker2
    TaskConnect -->|Port 9092 (Consume all streams)| Broker1
    TaskConnect -->|Regional HTTPS STS + S3 PUT| S3
    EB -->|cron/rate Trigger| TaskExt
    TaskExt -->|Port 5432 (Batch SQL)| RDS
    TaskExt -->|Snappy Parquet PUT| S3
    TaskAPI -->|Read Secret| SM
    TaskConnect -->|Read Secret| SM
    TaskExt -->|Read Secret| SM
```
