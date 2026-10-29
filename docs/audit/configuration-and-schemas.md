# Configuration & Schemas Technical Audit Report
## Synthetic Financial Data Ingestion Pipeline (SIC Platform)

**Audit Date:** October 26, 2026  
**AWS Account ID:** `138257835434` | **Region:** `eu-west-3`  
**Classification:** Technical Configuration Baseline & Schema Inventory

---

## 1. Amazon MSK Topic Configurations

| Topic Name | Partitions | Replication Factor | Cleanup Policy | Retention Period | In-Sync Replicas | Primary Producer | Primary Consumer |
|---|:---:|:---:|---|---|:---:|---|---|
| `transactions` | 3 | 2 | `delete` | 7 days (`604800000 ms`) | 1 | `generator_service.py` (`KafkaEventPublisher`) | `aiven-s3-sink` (Task 0) |
| `users` | 3 | 2 | `delete` | 7 days (`604800000 ms`) | 1 | `debezium-postgres-source` | `aiven-s3-sink` (Task 0) |
| `cards` | 3 | 2 | `delete` | 7 days (`604800000 ms`) | 1 | `debezium-postgres-source` | `aiven-s3-sink` (Task 0) |
| `sic_connect_configs` | 1 | 2 | `compact` | Infinite | 1 | Kafka Connect Worker | Kafka Connect Worker |
| `sic_connect_offsets` | 25 | 2 | `compact` | Infinite | 1 | Kafka Connect Worker | Kafka Connect Worker |
| `sic_connect_status` | 5 | 2 | `compact` | Infinite | 1 | Kafka Connect Worker | Kafka Connect Worker |

**Broker Endpoints (VPC Internal):**
- PLAINTEXT: `b-1.sicstreamingmsk.4io9um.c2.kafka.eu-west-3.amazonaws.com:9092,b-2.sicstreamingmsk.4io9um.c2.kafka.eu-west-3.amazonaws.com:9092`
- TLS: `b-1.sicstreamingmsk.4io9um.c2.kafka.eu-west-3.amazonaws.com:9094,b-2.sicstreamingmsk.4io9um.c2.kafka.eu-west-3.amazonaws.com:9094`

---

## 2. Kafka Producer Configuration (`KafkaEventPublisher`)

Inspected from `src/synthetic_streaming/kafka_publisher.py:88-100` and `config.py:46-72`:

```python
conf = {
    "bootstrap.servers": "b-1.sicstreamingmsk...:9092,b-2...:9092",
    "security.protocol": "PLAINTEXT",
    "acks": "1",                                # Acknowledged when leader broker writes to local log
    "enable.idempotence": False,                 # Set to False; duplicates possible on retry
    "max.in.flight.requests.per.connection": 5,
    "retries": 3,
    "retry.backoff.ms": 100,
    "delivery.timeout.ms": 30000,
    "queue.buffering.max.messages": 100000,
    "queue.buffering.max.kbytes": 1048576,       # 1 GB in-memory buffer limit
    "batch.num.messages": 10000,
    "linger.ms": 5,                              # 5ms micro-batch accumulation
}
```

---

## 3. Distributed Kafka Connect Connector Configurations

### 3.1. Debezium PostgreSQL Source Connector (`debezium-postgres-source`)
Active configuration registered via REST API (`scripts/fix_debezium_config.py:26-53`):

```json
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "tasks.max": "1",
  "plugin.name": "pgoutput",
  "database.hostname": "containerdb.cjm6iag8qrwi.eu-west-3.rds.amazonaws.com",
  "database.port": "5432",
  "database.user": "dbadmin",
  "database.password": "********",
  "database.dbname": "postgres",
  "database.server.name": "sic",
  "database.sslmode": "require",
  "topic.prefix": "sic",
  "table.include.list": "public.users,public.cards",
  "publication.name": "debezium_publication",
  "slot.name": "debezium_cdc_slot",
  "snapshot.mode": "initial",
  "tombstones.on.delete": "false",
  "decimal.handling.mode": "double",
  "time.precision.mode": "connect",
  "transforms": "reroute",
  "transforms.reroute.type": "org.apache.kafka.connect.transforms.RegexRouter",
  "transforms.reroute.regex": "(?:.*\\.public\\.)?(.*)",
  "transforms.reroute.replacement": "$1",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "false",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false"
}
```

### 3.2. Aiven S3 Sink Connector (`aiven-s3-sink`)
Active configuration registered via REST API (`scripts/fix_s3_sink_sts.py:10-29`):

```json
{
  "connector.class": "io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector",
  "tasks.max": "1",
  "topics": "transactions,users,cards",
  "aws.s3.bucket.name": "sic-pipeline-storage",
  "aws.s3.region": "eu-west-3",
  "aws.sts.role.arn": "arn:aws:iam::138257835434:role/syntheticTaskRole",
  "aws.sts.role.session.name": "sic-s3-sink-session",
  "aws.sts.config.endpoint": "https://sts.eu-west-3.amazonaws.com",
  "format.output.type": "jsonl",
  "format.output.fields": "key,value,offset,timestamp",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false",
  "file.name.prefix": "bronze/",
  "file.name.template": "{{topic}}/{{partition}}/{{start_offset:padding=true}}.json.gz",
  "file.compression.type": "gzip",
  "file.max.records": "50",
  "flush.timeout.ms": "5000"
}
```

---

## 4. PostgreSQL Relational Schemas & Logical Replication

### 4.1. Users Table (`public.users`)
```sql
CREATE TABLE users (
    user_id BIGINT PRIMARY KEY,
    person VARCHAR(255) NOT NULL,
    current_age INTEGER,
    retirement_age INTEGER,
    birth_year INTEGER,
    birth_month INTEGER,
    gender VARCHAR(16),
    address VARCHAR(255),
    apartment VARCHAR(64),
    city VARCHAR(128) NOT NULL,
    state VARCHAR(64) NOT NULL,
    zipcode VARCHAR(16) NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    per_capita_income VARCHAR(32),
    yearly_income VARCHAR(32),
    total_debt VARCHAR(32),
    fico_score INTEGER,
    num_credit_cards INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

### 4.2. Cards Table (`public.cards`)
```sql
CREATE TABLE cards (
    user_id BIGINT NOT NULL,
    card_index INTEGER NOT NULL,
    card_brand VARCHAR(32) NOT NULL,
    card_type VARCHAR(32) NOT NULL,
    card_number VARCHAR(32) NOT NULL,
    expires VARCHAR(16) NOT NULL,
    cvv VARCHAR(8) NOT NULL,
    has_chip VARCHAR(8) DEFAULT 'YES',
    cards_issued INTEGER DEFAULT 1,
    credit_limit VARCHAR(32),
    acct_open_date VARCHAR(16),
    year_pin_last_changed INTEGER,
    card_on_dark_web VARCHAR(8) DEFAULT 'No',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (user_id, card_index),
    CONSTRAINT fk_cards_user_id FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
);
```

### 4.3. Logical Replication Prerequisite State
- `rds.logical_replication = 1`
- `wal_level = logical`
- Publication: `debezium_publication` (`users`, `cards`)
- Slot: `debezium_cdc_slot` (Plugin: `pgoutput`)

---

## 5. Batch Reference Extraction Schemas (PyArrow / Parquet)

### 5.1. Merchant Category Codes (`MCC_ARROW_SCHEMA`)
- Extracted from `public.merchant_category_codes` (914 rows).
- Snappy-compressed Parquet.

| Field Name | PyArrow Type | Nullable | Source System Mapping |
|---|---|:---:|---|
| `mcc` | `pa.string()` | No (PK) | `merchant_category_codes.mcc` |
| `description` | `pa.string()` | Yes | `merchant_category_codes.description` |
| `category` | `pa.string()` | Yes | `merchant_category_codes.category` |
| `valid_payment_brands` | `pa.string()` | Yes | `merchant_category_codes.valid_payment_brands` |
| `required_abbreviation`| `pa.string()` | Yes | `merchant_category_codes.required_abbreviation` |
| `extraction_timestamp` | `pa.string()` | No | ISO-8601 UTC execution timestamp |
| `source_system` | `pa.string()` | No | Static literal: `"RDS_POSTGRESQL"` |

### 5.2. City Reference (`CITY_ARROW_SCHEMA`)
- Extracted from `public.city_reference` (1,433 rows).
- Snappy-compressed Parquet.

| Field Name | PyArrow Type | Nullable | Source System Mapping |
|---|---|:---:|---|
| `city_id` | `pa.int64()` | No (PK) | `city_reference.city_id` |
| `city_name` | `pa.string()` | No | `city_reference.city_name` |
| `state_code` | `pa.string()` | Yes | `city_reference.state_code` |
| `country_code` | `pa.string()` | Yes | `city_reference.country_code` |
| `latitude` | `pa.float64()` | Yes | `city_reference.latitude` |
| `longitude` | `pa.float64()` | Yes | `city_reference.longitude` |
| `coordinate_source` | `pa.string()` | Yes | `city_reference.coordinate_source` |
| `coordinate_precision`| `pa.string()` | Yes | `city_reference.coordinate_precision` |
| `extraction_timestamp` | `pa.string()` | No | ISO-8601 UTC execution timestamp |
| `source_system` | `pa.string()` | No | Static literal: `"RDS_POSTGRESQL"` |

---

## 6. ECS Task Definitions & Scheduling Configuration

| Task Definition Family | Revision | CPU | Memory | Container Name | Image URI | Command / Entrypoint |
|---|:---:|:---:|:---:|---|---|---|
| `synthetic-data-provider` | 4 | 512 (0.5 vCPU) | 1024 MiB | `synthetic-data-provider` | `.../synthetic-data-provider:latest` | `["api"]` |
| `sic-kafka-connect` | 4 | 1024 (1.0 vCPU) | 2048 MiB | `plugin-installer`<br>`kafka-connect`<br>`connector-registrar` | `...:root`<br>`debezium/connect:2.7.3.Final`<br>`...:root` | Plugin install -> Start connect -> Register connectors |
| `synthetic-ingestion-extractor` | 5 | 256 (0.25 vCPU) | 512 MiB | `ingestion-extractor` | `.../synthetic-data-provider:latest` | `["batch-reference"]` |

**EventBridge Scheduled Rule:**
- Name: `scheduled-ingestion-extractor`
- Status: `ENABLED`
- Expression: `rate(5 minutes)`
- Target: ECS Task Definition `synthetic-ingestion-extractor:5` on cluster `synthetic-data-cluster`
- Network: `AssignPublicIp: ENABLED`, Subnets: `subnet-003d3891dab36b354`, `subnet-0b18944bde732d1fd`, SG: `sg-0dd0933f4da2b5ea7`.
