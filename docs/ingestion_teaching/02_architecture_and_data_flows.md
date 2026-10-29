# المعمارية ورسومات تدفق البيانات: Data Ingestion Pipeline
## دليل المخططات والرسومات المعمارية التفصيلية

---

## 1. المعمارية الشاملة للمنظومة (Complete System Architecture)

يوضح هذا المخطط المعمارية الكلية للنظام، وكيف تتكامل مسارات الاستيعاب المتعددة مع طبقات التخزين والمعالجة اللحظية داخل شبكة AWS الافتراضية الموحدة (**VPC: `vpc-0d66731212396d1e9`**).

```mermaid
flowchart TB
    subgraph VPC["AWS VPC: vpc-0d66731212396d1e9 (Region: eu-west-3 Paris)"]
        subgraph SOURCES["طبقة مصادر البيانات (Data Sources)"]
            GEN["خدمة المحاكاة البنكية\n(FastAPI / Python Generator)\n[VERIFIED LIVE]"]
            RDS[("Amazon RDS PostgreSQL\n(Identifier: containerdb)\nTables: users, cards, ref\n[VERIFIED LIVE]")]
        end

        subgraph STREAMING["طبقة الوساطة اللحظية (Streaming Tier)"]
            subgraph MSK["Amazon MSK (Cluster: sic-streaming-msk | Kafka 3.8.x) [VERIFIED LIVE]"]
                T_TX[("Topic: transactions\n(3 Partitions)")]
                T_USR[("Topic: users\n(CDC Stream)")]
                T_CRD[("Topic: cards\n(CDC Stream)")]
                T_ALERT[("Topic: fraud_alerts\n(ML Alerts)")]
                T_DLQ[("Topic: transaction_dlq\n(Poison Pills)")]
            end
        end

        subgraph ECS_CONNECT["طبقة موصلات كافكا (Kafka Connect Tier)"]
            subgraph CONNECT_POD["ECS Task: sic-kafka-connect:4 [VERIFIED LIVE]"]
                DEB_SRC["Debezium PostgreSQL Source Connector\n(Slot: debezium_cdc_slot)"]
                S3_SINK["Aiven S3 Sink Connector\n(Micro-batch GZIP Flusher)"]
            end
        end

        subgraph ECS_COMPUTE["طبقة الحوسبة المستقلة (ECS Fargate Tier)"]
            BATCH_TASK["ECS Task: synthetic-ingestion-extractor:5\n(Batch Reference Extractor)\n[VERIFIED LIVE]"]
            FRAUD_TASK["ECS Task: sic-fraud-consumer:2\n(LightGBM Real-Time Scoring)\n[VERIFIED LIVE]"]
        end

        subgraph FEATURE_STORE["طبقة الذاكرة اللحظية (Feature Store Tier)"]
            REDIS[("AWS ElastiCache Redis 7.1.0\n(Cluster: sic-feature-store-001)\n[VERIFIED LIVE]")]
        end

        subgraph S3_BRONZE["طبقة التخزين المجمعة (S3 Bronze Lakehouse)"]
            S3_RAW[("Amazon S3: sic-pipeline-storage\nPrefix: bronze/\n[VERIFIED LIVE]")]
        end

        subgraph PROPOSED["المكونات المقترحة للتطوير المستقبلي [PROPOSED]"]
            PROP_SPARK["Databricks / PySpark Lakehouse\n(Silver & Gold Delta Transformations)"]
            PROP_HA_REDIS["Multi-AZ Redis Cluster\n(Automatic Failover)"]
        end
    end

    %% Ingestion Path 2: Direct Streaming
    GEN -->|1. Direct Events Streaming| T_TX
    T_TX -->|2. Consume for S3 Archiving| S3_SINK

    %% Ingestion Path 1: CDC via Debezium
    GEN -->|SQL Transactions / Updates| RDS
    RDS -->|WAL Logical Replication Logs| DEB_SRC
    DEB_SRC -->|Stream CDC Events| T_USR
    DEB_SRC -->|Stream CDC Events| T_CRD
    T_USR -->|Consume CDC| S3_SINK
    T_CRD -->|Consume CDC| S3_SINK

    %% Ingestion Path 3: Batch Reference Extractor
    RDS -.->|Batch Query Snapshots| BATCH_TASK
    BATCH_TASK -.->|Upload Snappy Parquet| S3_RAW

    %% S3 Sink Delivery
    S3_SINK -->|Micro-batch Flush JSONL.GZ| S3_RAW

    %% Serving Path: Fraud Detection
    T_TX -->|Consume for Scoring| FRAUD_TASK
    FRAUD_TASK <-->|TLS rediss:// State Query/Update| REDIS
    FRAUD_TASK -->|Emit Fraud Alerts| T_ALERT

    %% Future Ingestion
    S3_RAW -.->|Bronze to Silver ETL| PROP_SPARK
```

---

## 2. المسار الأول: تدفق الـ CDC عبر Debezium (PostgreSQL -> MSK -> S3)

يوضح هذا المخطط مسار التقاط التغييرات اللحظية من قاعدة بيانات PostgreSQL إلى بحيرة البيانات عبر **Debezium**:

```mermaid
sequenceDiagram
    autonumber
    participant App as Generator / Operator
    participant PG as PostgreSQL (containerdb)
    participant WAL as Write-Ahead Log (WAL)
    participant Deb as Debezium Source Connector
    participant MSK as Amazon MSK (Topic: users/cards)
    participant Sink as Aiven S3 Sink Connector
    participant S3 as S3 Bronze Bucket

    App->>PG: INSERT / UPDATE user profile (user_id=10)
    PG->>WAL: Commit transaction to disk (Write-Ahead Log)
    Note over PG,Deb: Logical Replication via 'debezium_cdc_slot'
    Deb->>WAL: Read continuous WAL change events
    Deb->>Deb: Apply RegexRouter: re-route 'sic.public.users' -> 'users'
    Deb->>MSK: produce(topic='users', key=user_id, value=cdc_json)
    Note over MSK: Event buffered in partition

    Sink->>MSK: poll() -> Consume CDC events
    Note over Sink: Buffer in memory until 1,000 records or 10s timeout
    Sink->>S3: Flush multi-part object (GZIP JSONL)
    Note over S3: Stored in s3://sic-pipeline-storage/bronze/users/partition=X/
```

### الشرح الهندسي للمسار:
1. أي تعديل في جدول المستخدمين أو الكروت بيسجله البوستجريس فوراً في الـ **Write-Ahead Log (WAL)**.
2. موصل **Debezium** بيسحب التغييرات من الـ Replication Slot بدون ما يعمل كويري مباشر يرهق المعالج.
3. الموصل بيستخدم **RegexRouter (SMT)** عشان يحذف الـ Schema Prefix ويحول اسم القناة لـ `users` أو `cards` مباشرة.
4. موصل **Aiven S3 Sink** بيقرا الرسائل من MSK، يجمعها في Micro-batch، ويضغطها بـ Gzip ويكتبها في S3.

---

## 3. المسار الثاني: التدفق المباشر للمعاملات (Transactions Direct Streaming)

يوضح المخطط كيفية انتقال أحداث المعاملات عالية الكثافة من محرك التوليد إلى التخزين النهائي في S3:

```mermaid
flowchart LR
    subgraph PRODUCER["المولد (Producer)"]
        GEN["GeneratorService\n(TransactionsGenerator)"]
        PUB["KafkaPublisher\n(confluent-kafka)"]
    end

    subgraph BROKER["وسيط كافكا (Amazon MSK)"]
        TOPIC["Topic: transactions\nPartitions: 0, 1, 2\nKey: user_id"]
    end

    subgraph SINK_WORKER["خادم التفريغ (Kafka Connect)"]
        AIVEN["Aiven S3 Sink Connector\nfile.max.records: 1000\nflush.timeout.ms: 10000"]
    end

    subgraph BRONZE_DEST["بحيرة البيانات (S3 Bronze)"]
        S3_PART["bronze/transactions/\npartition=0/00000000000000020000.json.gz\npartition=1/...\npartition=2/..."]
    end

    GEN -->|Generate EventEnvelope| PUB
    PUB -->|Produce with user_id key| TOPIC
    TOPIC -->|Stream Consume| AIVEN
    AIVEN -->|Flush Compressed GZIP JSONL| S3_PART
```

### الشرح الهندسي للمسار:
1. المعاملات بتتبعت فور توليدها باستخدام مكتبة `confluent-kafka` بمفتاح رسالة يمثل `user_id`.
2. توجيه الرسائل بمفتاح يضمن بقاء جميع معاملات العميل في نفس الـ Partition وبالترتيب الزمني الدقيق.
3. موصل S3 يفرغ البيانات عند تراكم 1,000 سجل أو مرور 10 ثوانٍ، مما يحمي النظام من مشكلة الملفات الصغيرة (Small Files Problem).

---

## 4. المسار الثالث: الاستخراج الدفعي للبيانات المرجعية (Batch Reference Extractor)

يوضح المخطط معمارية سحب لقطات الجداول المرجعية الثابتة دون المرور عبر كافكا لترشيد التكاليف:

```mermaid
flowchart TD
    subgraph TRIGGER["المجدول (Trigger)"]
        EB["Amazon EventBridge Rule\n(scheduled-ingestion-extractor)\nRate: Periodic Schedule"]
    end

    subgraph ECS_TASK["مهمة الحوسبة (Serverless Fargate)"]
        TASK["ECS Fargate Task: synthetic-ingestion-extractor:5\nImage: synthetic-data-provider:latest\n(0.25 vCPU, 512 MB RAM)"]
        EXT["BatchReferenceExtractor\n(SQLAlchemy + PyArrow)"]
    end

    subgraph DATABASE["قاعدة البيانات (Amazon RDS)"]
        PG[("PostgreSQL: containerdb\n• public.merchant_category_codes (914 rows)\n• public.city_reference (1,433 rows)")]
    end

    subgraph LAKEHOUSE["بحيرة البيانات (Amazon S3 Bronze)"]
        S3_MCC["bronze/mcc/snapshot_date=YYYY-MM-DD/\nreference_mcc_YYYYMMDD_HHMMSS.parquet\n(27 KB Snappy)"]
        S3_CIT["bronze/cities/snapshot_date=YYYY-MM-DD/\nreference_cities_YYYYMMDD_HHMMSS.parquet\n(38 KB Snappy)"]
    end

    EB -->|Trigger RunTask API| TASK
    TASK --> EXT
    EXT -->|Query Full Snapshot| PG
    PG -->|Result Rows| EXT
    EXT -->|Convert to Arrow & Compress Snappy| EXT
    EXT -->|boto3 put_object Direct Upload| S3_MCC
    EXT -->|boto3 put_object Direct Upload| S3_CIT
```

### الشرح الهندسي للمسار:
1. الجداول الثابتة (أكواد التصنيف 914 كود والمدن 1,433 مدينة) لا تحتاج إلى Streaming لحظي عبر كافكا.
2. خدمة **EventBridge** تشغل Fargate Task صغيرة جداً (0.25 vCPU) تسحب البيانات دفعة واحدة (Batch).
3. الكود يحول البيانات في الذاكرة إلى **Apache Parquet** مضغوط بـ **Snappy** ويرفعها مباشرة إلى S3 مقسمة بالتاريخ اليومي (`snapshot_date=YYYY-MM-DD/`).

---

## 5. مسار المعالجة الفورية وكشف الاحتيال (Real-Time Fraud Serving Path)

يوضح المخطط كيفية استهلاك الحركات لحظياً وتقييمها بالذكاء الاصطناعي مع حفظ الحالة في **Redis Feature Store**:

```mermaid
sequenceDiagram
    autonumber
    participant MSK_IN as MSK (transactions)
    participant Consumer as ECS Fargate (sic-fraud-consumer:2)
    participant Redis as ElastiCache Redis (sic-feature-store)
    participant ML as LightGBM Model
    participant MSK_OUT as MSK (fraud_alerts)

    MSK_IN->>Consumer: poll() -> Consume Transaction Event
    Note over Consumer: Parse JSON & extract User, Card, Amount, Lat, Lon

    Consumer->>Redis: Pipeline 1: Read prior state (last_tx, tx_window ZSET, spend_stats HASH)
    Redis-->>Consumer: Return prior sliding window counts & running stats

    Consumer->>Consumer: Compute 34 Features (Zero Leakage: based on prior state only)
    Consumer->>Redis: SET dedup:{id} 1 NX EX (Check Duplicate)
    alt Event is NOT Duplicate
        Consumer->>Redis: Pipeline 2: Atomic Update (last_tx, ZADD tx_window, HINCRBY stats)
    end

    Consumer->>ML: predict_proba(34-dim Feature Vector)
    ML-->>Consumer: Return fraud_probability (e.g. 0.851)

    alt is_fraud_predicted == True
        Consumer->>MSK_OUT: produce(topic='fraud_alerts', key=user_id, value=alert_json)
        Consumer->>MSK_OUT: flush(timeout=5.0)
        MSK_OUT-->>Consumer: Broker Ack
    end

    Consumer->>MSK_IN: commit(message, asynchronous=False)
    Note over Consumer,MSK_IN: Synchronous Offset Commit Only After Alert Flush
```

---

## 6. دورة حياة رسالة البيانات من المصدر إلى التخزين (Message Lifecycle)

```mermaid
stateDiagram-v2
    [*] --> Generated: توليد المعاملة في GeneratorService
    Generated --> Enveloped: تغليف الحدث بهيكل EventEnvelope القياسي
    Enveloped --> BufferedInProducer: تجميع في ذاكرة كافكا المؤقتة (Producer Buffer)
    BufferedInProducer --> StoredInMSK: كتابة في Partition كافكا وضمان استقرار الـ Leader
    
    state StoredInMSK {
        [*] --> IngestedBySinkConnector: قراءة عبر Aiven S3 Sink
        [*] --> ConsumedByFraudModel: قراءة عبر FraudStreamingConsumer
    }

    IngestedBySinkConnector --> MicroBatched: تجميع في ذاكرة الموصل حتى 1000 سجل
    MicroBatched --> CompressedGZIP: ضغط الدفعة بخوارزمية GZIP
    CompressedGZIP --> StoredInS3Bronze: حفظ دائم في S3: bronze/transactions/...

    ConsumedByFraudModel --> ScoredByModel: استخراج الميزات وحساب الاحتمالية
    ScoredByModel --> EmittedToFraudAlerts: إرسال تنبيه في حال تجاوز العتبة
    ScoredByModel --> OffsetCommitted: تثبيت الإزاحة يدوياً في كافكا

    StoredInS3Bronze --> [*]
    OffsetCommitted --> [*]
```
