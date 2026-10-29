# المعمارية الكلية لمنظومة استيعاب البيانات (System Architecture)
## المعمارية السحابية الشاملة لمنصة SIC — مخططات C4 والشبكات وتدفق البيانات

---

> [!NOTE]
> **مسار الملف:** `docs/architecture/system-architecture.md`  
> **الهدف:** توثيق معمارية النظام الكلية وفق منهجية نماذج C4 (Context, Container, Component, Code)، مع تحديد حدود الشبكات، آليات الاتصال، مخازن البيانات، ومخططات تدفق ومعالجة الفشل.

---

## 1. سياق النظام والحدود الخارجية (System Context - C4 Level 1)

تتفاعل منصة **SIC** مع مستخدميها وأنظمتها الخارجية ضمن بيئة سحابية متكاملة على **Amazon Web Services (AWS)**:

```mermaid
C4Context
    title مخطط سياق النظام لمنصة SIC (C4 System Context Diagram)

    Person(data_eng, "مهندس البيانات / المحلل", "يستهلك البيانات النظيفة من بحيرة البيانات ويراقب تدفق الـ Pipelines.")
    Person(operator, "مهندس العمليات / SRE", "يراقب صحة الخدمات ويدير معدلات التوليد وحالات الأعطال.")

    System_Boundary(sic_boundary, "منظومة SIC لاستيعاب البيانات والمعاملات المالية") {
        System(sic_core, "SIC Ingestion Platform", "توليد المعاملات، التقاط التعديلات CDC، كشف الاحتيال الفوري، والتخزين في بحيرة البيانات.")
    }

    System_Ext(databricks, "Databricks Lakehouse Workspace", "معالجة وتحويل البيانات من Bronze إلى Silver و Gold Delta Tables.")
    System_Ext(aws_cloud, "AWS Managed Services", "Amazon MSK (Kafka), RDS PostgreSQL, ElastiCache Redis, S3, EventBridge, Secrets Manager.")

    Rel(operator, sic_core, "يتحكم بالمعدلات ويفحص الجاهزية عبر", "FastAPI / Port 8000")
    Rel(sic_core, aws_cloud, "يستضيف وينقل ويخزن البيانات على", "TCP, TLS, IAM, REST")
    Rel(sic_core, databricks, "يغذي بحيرة البيانات الخام لتقرأها", "S3 Bronze Parquet & JSONL")
    Rel(data_eng, databricks, "يجري التحليلات واستعلامات SQL عبر", "Delta Tables / SQL Warehouse")
```

---

## 2. معمارية الحاويات والخدمات (Containers Architecture - C4 Level 2)

يوضح المخطط التالي الحاويات والخدمات الأساسية والحدود الشبكية داخل شبكة الـ VPC:

```mermaid
flowchart TD
    %% Styling
    classDef vpc fill:#020617,stroke:#3b82f6,stroke-width:2px,color:#f8fafc;
    classDef publicSubnet fill:#0f172a,stroke:#0ea5e9,stroke-width:1.5px,color:#f8fafc;
    classDef privateSubnet fill:#1e1b4b,stroke:#8b5cf6,stroke-width:1.5px,color:#f8fafc;
    classDef storageSubnet fill:#14532d,stroke:#22c55e,stroke-width:1.5px,color:#f8fafc;
    classDef ext fill:#1c1917,stroke:#f59e0b,stroke-width:2px,color:#f8fafc;

    subgraph AWS_REGION ["AWS Region: eu-west-3 (Paris) — Dedicated VPC (172.31.0.0/16)"]

        subgraph PUB_SUBNET ["الشبكة الفرعية العامة (Public Subnet)"]
            ALB["Application Load Balancer / Public Entry\n(Optional Port 80 / 443)"]
        end
        class PUB_SUBNET publicSubnet;

        subgraph PRIV_SUBNET ["الشبكة الفرعية الخاصة بالتطبيقات (Private Application Subnet)"]
            GEN_SVC["<b>Generator Service & API</b><br/>(ECS Fargate Task / 0.5 vCPU)<br/>IP: 172.31.9.107 : 8000"]
            FRAUD_SVC["<b>Fraud Consumer Service</b><br/>(ECS Fargate Task :2 / 1 vCPU)<br/>LightGBM Core / Non-root 10001"]
            CONNECT_SVC["<b>Kafka Connect Cluster</b><br/>(ECS Fargate Task :4 / 1 vCPU)<br/>Debezium + Aiven S3 Sink<br/>IP: 172.31.10.236 : 8083"]
            EXTRACT_TASK["<b>Batch & Incremental Extractor</b><br/>(Ephemeral ECS Task / Airflow Runner)"]
        end
        class PRIV_SUBNET privateSubnet;

        subgraph DATA_SUBNET ["الشبكة الفرعية الخاصة بالبيانات والتخزين (Isolated Data Subnet)"]
            RDS_DB[("<b>Amazon RDS PostgreSQL 18.3</b><br/>containerdb / Port 5432<br/>wal_level=logical")]
            REDIS_CACHE[("<b>AWS ElastiCache for Redis 7.1</b><br/>cache.t4g.micro / Port 6379 (TLS)<br/>8,346 Keys / In-transit Auth")]
            MSK_CLUSTER[("<b>Amazon MSK (Apache Kafka 3.x)</b><br/>2 Brokers (b-1, b-2) / Port 9092<br/>3 Partitions / Replication Factor 2")]
        end
        class DATA_SUBNET storageSubnet;

    end
    class AWS_REGION vpc;

    subgraph AWS_STORAGE ["خدمات التخزين السحابية المركزية (Global Storage Plane)"]
        S3_LAKE[("<b>Amazon S3: sic-pipeline-storage</b><br/>Prefix: bronze/<br/>- transactions/<br/>- users/ & cards/<br/>- mcc/ & cities/<br/>- quarantine/")]
        DYNAMO_TABLE[("<b>Amazon DynamoDB</b><br/>Table: ingestion_watermarks<br/>Atomic Checkpointing")]
        SECRETS[("<b>AWS Secrets Manager</b><br/>DB Credentials & TLS Keys")]
    end
    class AWS_STORAGE ext;

    %% Network Interconnections
    ALB -.->|Forward :8000| GEN_SVC
    GEN_SVC -->|1. Stream Tx / TCP 9092| MSK_CLUSTER
    GEN_SVC -->|2. Insert Users/Cards / SQL 5432| RDS_DB
    GEN_SVC -.->|Read Secrets| SECRETS

    RDS_DB -->|3. WAL Logical Decoding| CONNECT_SVC
    CONNECT_SVC -->|4. Publish CDC Events| MSK_CLUSTER
    MSK_CLUSTER -->|5. Consume Topics| CONNECT_SVC
    CONNECT_SVC -->|6. Gzip JSONL Upload| S3_LAKE

    MSK_CLUSTER -->|7. Real-Time Stream| FRAUD_SVC
    FRAUD_SVC <-->|8. Rediss:// TLS State Query & Update| REDIS_CACHE
    FRAUD_SVC -->|9. Emit Alert| MSK_CLUSTER

    EXTRACT_TASK -->|Extract Keyset| GEN_SVC
    EXTRACT_TASK -->|Atomic Watermark| DYNAMO_TABLE
    EXTRACT_TASK -->|Parquet Upload| S3_LAKE
```

---

## 3. مخطط تدفق البيانات الكامل (End-to-End Data Flow Diagram)

يوضح المخطط حركة كل صنف من البيانات عبر بروتوكولات النقل من الإنشاء وحتى الهبوط النهائي:

```mermaid
flowchart LR
    %% Data Flow
    subgraph S_GEN ["التوليد الأولي"]
        TX_MEM["معاملات مالية حية\n(في ذاكرة المولد)"]
        USR_MEM["حسابات وبطاقات جديدة\n(10% Ratio)"]
        REF_DB["مرجعيات ثابتة\n(914 MCC / 1,433 Cities)"]
    end

    subgraph S_TRANSPORT ["قنوات النقل والمعالجة"]
        TCP_KAFKA["TCP Direct Stream\n(MSK: transactions)"]
        SQL_INSERT["SQL INSERT Atomic\n(PostgreSQL WAL)"]
        WAL_DECODE["Debezium Logical Decoding\n(MSK: users, cards)"]
        BATCH_ARROW["PyArrow In-Memory Tables\n(Batch Extraction)"]
        ML_SCORING["LightGBM 0.5ms Inference\n(ElastiCache Redis Features)"]
    end

    subgraph S_OUTPUT ["المخرجات وبحيرة البيانات"]
        JSONL_TX["s3://.../bronze/transactions/\n(partition={P}/{offset}.json.gz)"]
        JSONL_CDC["s3://.../bronze/{users,cards}/\n(partition={P}/{offset}.json.gz)"]
        PARQUET_REF["s3://.../bronze/{mcc,cities}/\n(year={Y}/month={M}/*.parquet)"]
        ALERTS_TOPIC["MSK: fraud_alerts\n(Real-Time High-Risk Alerts)"]
    end

    TX_MEM --> TCP_KAFKA --> JSONL_TX
    TX_MEM -.-> ML_SCORING --> ALERTS_TOPIC
    USR_MEM --> SQL_INSERT --> WAL_DECODE --> JSONL_CDC
    REF_DB --> BATCH_ARROW --> PARQUET_REF
```

---

## 4. مخطط التبعيات البرمجية والتكامل (Component Dependencies Diagram)

يوضح المخطط كيف ترتبط الحزم البرمجية داخل `src/` ببعضها وبالمكتبات الخارجية:

```mermaid
graph TD
    subgraph CORE_DEPS ["مكتبات بايثون الخارجية الأساسية"]
        P_CONFLUENT["confluent-kafka (v2.3+)"]
        P_PYARROW["pyarrow (v15.0+)"]
        P_LIGHTGBM["lightgbm + libgomp1"]
        P_FASTAPI["fastapi + uvicorn"]
        P_SQLA["sqlalchemy (v2.0+)"]
        P_REDIS["redis-py (v5.0+)"]
        P_BOTO3["boto3 (AWS SDK)"]
    end

    subgraph SYNTH_STREAMING ["حزمة synthetic_streaming"]
        G_SVC["GeneratorService"]
        R_CTRL["RateController"]
        S_ENG["ScenarioEngine"]
        K_PUB["KafkaPublisher"]
        API_MOD["api.py (FastAPI)"]
        DB_MOD["database.py (SQLAlchemy)"]
    end

    subgraph INGESTION ["حزمة ingestion"]
        EXTRACTOR["TransactionExtractor"]
        BATCH_EXT["BatchReferenceExtractor"]
        STATE_DDB["IngestionStateStore (DynamoDB)"]
    end

    subgraph ML_FRAUD ["حزمة ml"]
        CONSUMER["FraudStreamingConsumer"]
        FEAT_ENG["FeatureExtractor"]
        STATE_STORE["StateStore (Redis / In-Memory)"]
    end

    %% Internal Wiring
    G_SVC --> R_CTRL
    G_SVC --> S_ENG
    G_SVC --> K_PUB
    G_SVC --> DB_MOD
    API_MOD --> G_SVC

    EXTRACTOR --> STATE_DDB
    CONSUMER --> FEAT_ENG
    CONSUMER --> STATE_STORE

    %% External Connections
    K_PUB --> P_CONFLUENT
    DB_MOD --> P_SQLA
    API_MOD --> P_FASTAPI
    EXTRACTOR --> P_PYARROW
    EXTRACTOR --> P_BOTO3
    BATCH_EXT --> P_PYARROW
    CONSUMER --> P_LIGHTGBM
    CONSUMER --> P_CONFLUENT
    STATE_STORE --> P_REDIS
    STATE_DDB --> P_BOTO3
```

---

## 5. مخطط معالجة الفشل والتعافي الذاتي (Failure Recovery Workflows)

تتبع المنصة استراتيجيات هندسية مرنة للتعامل مع أي عطل طارئ دون فقدان البيانات:

```mermaid
flowchart TD
    %% Failure Handling
    START_EVT["حدث جديد في المنظومة"] --> CHK_TYPE{"نوع الحدث والمسار؟"}

    %% Path 1 & 4: Streaming & ML Failures
    CHK_TYPE -->|Streaming Transaction| CHK_REDIS{"هل اتصال Redis سليم؟"}
    CHK_REDIS -->|نعم| REDIS_OK["استرجاع الخصائص من ElastiCache"]
    CHK_REDIS -->|انقطاع / Timeout| REDIS_FAIL["التحول التلقائي لـ Local In-Memory Cache في الرام"]
    REDIS_OK --> RUN_ML["تشغيل استنتاج LightGBM"]
    REDIS_FAIL --> RUN_ML

    RUN_ML --> CHK_ALERT{"هل تم تقييم المعاملة كاحتيال؟"}
    CHK_ALERT -->|نعم| SEND_ALERT["نشر التنبيه لموضوع fraud_alerts"]
    CHK_ALERT -->|لا| COMMIT_OFFSET["تثبيت الـ Kafka Offset"]
    SEND_ALERT --> CHK_ACK{"هل استلم كافكا التنبيه؟ (Broker ACK)"}
    CHK_ACK -->|نعم| COMMIT_OFFSET
    CHK_ACK -->|فشل / Timeout| RETRY_ALERT["إعادة المحاولة (5 Retries) مع تراجع زمني"]
    RETRY_ALERT -->|استنفاد المحاولات| CRASH_GUARD["إيقاف الـ Offset Commit لمنع ضياع التنبيه (At-Least-Once)"]

    %% Path 1A: Extractor / S3 Failures
    CHK_TYPE -->|Incremental Extraction| RUN_GATE["بوابة جودة البيانات (DataQualityGate)"]
    RUN_GATE --> CHK_VALID{"هل السجل مطابق للشروط؟"}
    CHK_VALID -->|سليم| WRITE_S3["كتابة Parquet في S3 Bronze"]
    CHK_VALID -->|مشوه / سالب / تكرار| ISOLATE["عزل السجل في S3 Quarantine وكتابة DLQ"]
    WRITE_S3 --> CHK_S3{"هل تم تأكيد كتابة S3 بنجاح؟"}
    CHK_S3 -->|نعم| UPDATE_DDB["تحديث علامة الإزاحة في DynamoDB ذرياً"]
    CHK_S3 -->|فشل| ABORT_DDB["إلغاء التحديث وإعادة المحاولة في الدورة القادمة"]
```

---

## 6. القرارات المعمارية الكبرى ومبرراتها (Architectural Decisions Summary)

1. **فصل المعاملات عن RDBMS (Zero-DB-Inserts):**
   * *المبرر:* منع اختناق قرص التخزين والتنافس على الـ Locks؛ استغلال سرعة كتابة كافكا التراكمية في الذاكرة (Append-Only Commit Log).
2. **استخدام `user_id` كمفتاح توزيع في كافكا (Partition Key):**
   * *المبرر:* ضمان وصول جميع معاملات نفس المستخدم لنفس الـ Partition بدقة زمنية متسلسلة (Strict FIFO)، مما يمنع تعارض حسابات النوافذ المتحركة (Sliding Windows) في كشف الاحتيال.
3. **نمط الـ Pod متعدد الحاويات لـ Kafka Connect على Fargate:**
   * *المبرر:* تجنب صيانة وتحديث Docker Images ضخمة مخصصة؛ استخدام Init Container لتحميل وتثبيت الـ Plugins في Volume مشترك عند الإقلاع.
4. **تخزين الـ Watermark في Amazon DynamoDB مع شروط ذرية:**
   * *المبرر:* ضمان عدم تراجع الـ Offset إلى الوراء عند تشغيل مهمتين متزامنتين، مما يضمن اتساق الاستخراج التزايدي بدون تكرار.
5. **درع الإزاحة (Offset Commit Guard) في مستهلك الاحتيال:**
   * *المبرر:* تقديم ضمانات تسليم صارمة (At-Least-Once) لتنبيهات الاحتيال، بحيث لو تعطل الكونتينر قبل إرسال التنبيه، تعاد قراءة المعاملة وتوليد التنبيه فور إعادة التشغيل.
