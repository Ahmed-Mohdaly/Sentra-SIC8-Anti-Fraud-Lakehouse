# مسارات استيعاب البيانات الأربعة (The Four Ingestion Paths)
## الدليل التفصيلي المستقل لكل مسار استيعاب في منصة SIC

---

> [!NOTE]
> **مسار الملف:** `docs/architecture/ingestion-paths.md`  
> **الهدف:** شرح كل مسار من مسارات تدفق البيانات الأربعة بشكل مستقل وعميق (المصدر، المنتج، وسيلة النقل، المستهلك، المصب، الهيكل، التوقيت، أساليب المعالجة وإعادة المحاولة)، ثم استعراض كيفية تكاملها في بحيرة البيانات المركزية.

---

## 1. المسار الأول: البث الحي للمعاملات (Path 1: Real-Time Transaction Streaming)

### 1.1 المفهوم والمسؤولية
هذا المسار هو العصب الرئيسي لتدفق البيانات عالية السرعة (High-Velocity Streaming Transactions). مسؤوليته استيعاب معاملات الشراء والسحب في اللحظة التي تقع فيها، ونقلها فورياً إلى بحيرة البيانات دون إدخالها في قاعدة بيانات علائقية (**Zero-DB-Inserts**).

### 1.2 المواصفات الفنية للمسار
* **المصدر الأصلي (Source of Record):** ذاكرة محرك التوليد [`TransactionsGenerator`](file:///home/syd/Downloads/archive/src/synthetic_streaming/transactions_generator.py#L40-L120) داخل خدمة `GeneratorService`.
* **المنتج (Producer):** كلاس [`KafkaEventPublisher`](file:///home/syd/Downloads/archive/src/synthetic_streaming/kafka_publisher.py#L73-L120) المعتمد على مكتبة `confluent-kafka` ومحرك `librdkafka` الداخلي.
* **وسيلة النقل (Transport):** اتصال TCP مباشر عبر المنفذ `9092` إلى Amazon MSK Cluster.
* **المستقر الوسيط:** موضوع `transactions` على كافكا (3 Partitions، عامل تكرار Replication Factor = 2).
* **مفتاح التوزيع (Partition Key):** `user_id` بصيغة نصية لضمان وصول جميع معاملات نفس المستخدم بالترتيب الصارم.
* **المستهلك (Consumer):** موصل `aiven-s3-sink` المشغل داخل مجموعة Kafka Connect المستقلة على ECS Fargate (Consumer Group: `connect-aiven-s3-sink`).
* **المصب النهائي (Destination):** باكت بحيرة البيانات على Amazon S3:  
  `s3://sic-pipeline-storage/bronze/transactions/partition={partition}/{start_offset}.json.gz`
* **التوقيت ومعدل التدفق:** مستمر بمعدل افتراضي 10 معاملات/ثانية (قابل للتعديل عبر API حتى 100+ TPS).
* **هيكل البيانات (Data Contract):** غلاف [`EventEnvelope`](file:///home/syd/Downloads/archive/src/synthetic_streaming/event_schema.py#L137-L155) يحمل الحمولة [`TransactionPayload`](file:///home/syd/Downloads/archive/src/synthetic_streaming/event_schema.py#L77-L135).
* **معالجة الفشل وإعادة المحاولة (Retry & Failure Behavior):**
  * ضبط المنتج بـ `acks="all"` و `retries=5` و `retry.backoff.ms=100`.
  * تفعيل خاصية الـ Idempotence لمنع تكرار الرسائل عند إعادة الإرسال.
  * في حالة فشل النشر بعد استنفاد المحاولات، يُسجل الخطأ وتتوقف الدورة للحماية من فقدان البيانات.
* **استراتيجية التحقق (Validation Strategy):** فحص الروابط الحسابية بالذاكرة عبر [`RelationshipValidator`](file:///home/syd/Downloads/archive/src/synthetic_streaming/relationship_validator.py) والتأكد من مطابقة إحداثيات التاجر والـ Entry Method.

---

## 2. المسار الثاني: التقاط تعديلات الكيانات (Path 2: Relational Entity CDC)

### 2.1 المفهوم والمسؤولية
العملاء الجدد والبطاقات المصرفية المصدرة تمثل كيانات علائقية (Relational Entities) تتطلب اتساقاً صارماً. هذا المسار يضمن تسجيل الحسابات أولاً في PostgreSQL ثم التقاطها لحظياً فور كتابتها في سجلات الـ WAL لبثها إلى كافكا و S3 دون التأثير على أداء قاعدة البيانات.

### 2.2 المواصفات الفنية للمسار
* **المصدر الأصلي (Source of Record):** قاعدة بيانات **Amazon RDS PostgreSQL 18.3** (`containerdb`) — جدولا `public.users` و `public.cards`.
* **المنتج (Producer):** موصل **Debezium PostgreSQL Connector v2.5.4** يعمل كـ Source Connector داخل Kafka Connect على Fargate.
* **آلية الالتقاط (Capture Engine):** قراءة وفك تشفير سجلات الـ Write-Ahead Log (WAL) عبر الـ Replication Slot `debezium_cdc_slot` و الـ Publication `debezium_publication` باستخدام إضافة `pgoutput`.
* **وسيلة النقل:** أحداث CDC يتم تحويلها بواسطة أداة التحويل المنطقي (SMT: `ByLogicalTableRouter`) لترسل مباشرة إلى كافكا.
* **المستقر الوسيط:** موضوعا كافكا: `users` و `cards` (3 Partitions لكل موضوع).
* **مفتاح التوزيع (Partition Key):** `user_id`.
* **المستهلك:** موصل `aiven-s3-sink` (نفس الموصل الذي يستهلك المعاملات في المسار الأول).
* **المصب النهائي:**
  `s3://sic-pipeline-storage/bronze/users/partition={partition}/{start_offset}.json.gz`  
  `s3://sic-pipeline-storage/bronze/cards/partition={partition}/{start_offset}.json.gz`
* **التوقيت ومعدل التدفق:** مدفوع بالأحداث (Event-Driven)؛ يعمل عند توليد مستخدمين جدد (بنسبة 10% من وتيرة المعاملات).
* **هيكل البيانات (CDC Schema):** يحتوي الحدث على بنية Debezium القياسية: حالة الحقول قبل التعديل (`before`)، وحالتها بعد التعديل (`after`)، ومعلومات المصدر وموقع الـ LSN وتوقيت المعاملة.
* **معالجة الفشل وإعادة المحاولة:**
  * إذا توقف موصل Debezium، يحتفظ الـ Replication Slot بسجلات الـ WAL في PostgreSQL حتى يعود الموصل للعمل ويستأنف القراءة من آخر LSN مؤكد.
  * إذا فشل موصل S3 Sink، يحتفظ كافكا بالأحداث وفق سياسة الـ Retention دون فقدان أي تحديث.

---

## 3. المسار الثالث: الاستخراج الدفعي للمرجعيات (Path 3: Scheduled Batch Reference)

### 3.1 المفهوم والمسؤولية
البيانات المرجعية (أكواد تصنيف المتاجر MCC ومعلومات المدن الجغرافية) هي بيانات بطيئة التغير (Slowly Changing Dimensions). لا جدوى هندسياً من بثها عبر كافكا؛ بل تُستخرج كلقطات دورية يومية (Daily Snapshots) وتُخزن بصيغة عمودية عالية الكفاءة في الاستعلام.

### 3.2 المواصفات الفنية للمسار
* **المصدر الأصلي (Source of Record):** جداول `merchant_category_codes` (914 كود) و `city_reference` (1,433 مدينة) في PostgreSQL RDS.
* **المستخرج (Extractor):** كلاس [`BatchReferenceExtractor`](file:///home/syd/Downloads/archive/src/ingestion/batch_reference_extractor.py#L100-L260) المنفذ عبر بايثون.
* **وسيلة النقل ومحرك المعالجة:** قراءة SQL مباشرة عبر `sqlalchemy`، ثم تحويل السجلات في الذاكرة إلى جداول **Apache Arrow**، وضغطها بصيغة **Apache Parquet**.
* **المستقر الوسيط:** ذاكرة الحاوية (RAM) فقط — لا يمر هذا المسار عبر كافكا نهائياً!
* **المصب النهائي:** يُرفع الملف مباشرة إلى S3 Bronze مقسماً حسب سنة وشهر الاستخراج:  
  `s3://sic-pipeline-storage/bronze/mcc/year={YYYY}/month={MM}/mcc_{timestamp}.snappy.parquet`  
  `s3://sic-pipeline-storage/bronze/cities/year={YYYY}/month={MM}/cities_{timestamp}.snappy.parquet`
* **التوقيت والجدولة:** يومياً في تمام الساعة 02:00 UTC عبر **Amazon EventBridge Scheduler**، أو منتصف الليل عبر **Airflow DAG 2** (`dag_02_batch_reference.py`).
* **معالجة الفشل وإعادة المحاولة:**
  * في Airflow: المهمة مضبوطة بـ 2 Retries وتراجع زمني 5 دقائق.
  * مهمة التحقق `task_validate_landing` تتصل بـ S3 وتفحص وجود الكائنات وحجمها؛ لو كان الحجم صفراً تفشل المهمة فوراً.
* **استراتيجية التحقق:** مقارنة عدد السجلات المستخرجة بعدد الصفوف في قاعدة البيانات لضمان الاكتمال بنسبة 100%.

---

## 4. المسار الرابع: كشف الاحتيال الفوري (Path 4: Real-Time ML Fraud Inference)

### 4.1 المفهوم والمسؤولية
تقييم خطورة المعاملة المالية في الوقت الفعلي واتخاذ قرار بشأن احتمال كونها احتيالاً (Fraud Scoring) في نافذة زمنية فائقة الصغر (أقل من **10 ميلي ثانية**) وتوليد تنبيهات فورية لحماية الحسابات.

### 4.2 المواصفات الفنية للمسار
* **المصدر الأصلي للحدث:** موضوع كافكا `transactions` (يقرأ الأحداث بالتوازي مع موصل S3 Sink دون تعارض).
* **المستهلك (Consumer):** كلاس [`FraudStreamingConsumer`](file:///home/syd/Downloads/archive/src/ml/fraud_consumer.py#L38-L120) يعمل في حاوية مخصصة على ECS Fargate (Consumer Group: `fraud-scoring-group`).
* **متجر الخصائص اللحظي (Online Feature Store):** **AWS ElastiCache for Redis 7.1** (`cache.t4g.micro`) عبر اتصال TLS آمن. يحتوي على 8,346 بروفايل وسجلات النوافذ المتحركة (1h و 24h).
* **محرك الذكاء الاصطناعي (ML Core):** مصنف **LightGBM** مدرب ومحفوظ (`models/v2/fraud_lgbm_model.joblib`).
* **أبعاد المتجه (Feature Dimensions):** **34 ميزة رقمية** تشمل مسافات الدائرة العظمى (Haversine)، سرعات الانتقال، ونوافذ الإنفاق.
* **المصب النهائي:**
  * إذا كانت المعاملة احتيالية (احتمال $\ge 0.0453$): يُرسل الحدث إلى موضوع كافكا `fraud_alerts`.
  * يتم تحديث عدادات العميل ومجموع إنفاقه في Redis مع TTL مدته 48 ساعة.
* **التوقيت والسرعة (Measured Latencies):**
  * استرجاع الخصائص من Redis: **1.11 ميلي ثانية**.
  * استنتاج موديل LightGBM: **0.5 ميلي ثانية**.
  * تحديث الحالة في Redis: **1.94 ميلي ثانية**.
  * إجمالي المعالجة لكل معاملة: **6 إلى 8 ميلي ثانية**.
* **معالجة الفشل والاعتمادية (Resilience & Offset Guard):**
  * **درع الإزاحة:** تثبيت الـ Offset في كافكا يحدث بعد التأكد من نشر التنبيه بنجاح لضمان عدم ضياع التنبيهات (At-Least-Once Delivery).
  * **البديل المحلي:** إذا انقطع الاتصال بـ Redis، يتحول المستهلك تلقائياً إلى الذاكرة المحلية (In-Memory Fallback) لضمان استمرار الفحص دون توقف.

---

## 5. مصفوفة مقارنة المسارات وتكاملها في بحيرة البيانات

```text
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                 S3 BRONZE DATA LAKEHOUSE CONVERGENCE                                  │
├───────────────────────────────┬─────────────────────────────────┬──────────────────────────────────────┤
│ المسار المعماري               │ مسار التخزين في S3              │ صيغة التخزين ومبررها الفني           │
├───────────────────────────────┼─────────────────────────────────┼──────────────────────────────────────┤
│ المسار 1: المعاملات الحية      │ bronze/transactions/partition=X/│ JSONL.GZ (سرعة كتابة متسلسلة وضغط)   │
│ المسار 2: تعديلات الكيانات CDC │ bronze/{users,cards}/           │ JSONL.GZ (الحفاظ على بنية التعديلات) │
│ المسار 3: لقطات المرجعيات     │ bronze/{mcc,cities}/year=Y/...  │ Snappy Parquet (كفاءة استعلام عمودية)│
│ المسار 4: كشف الاحتيال الفوري │ MSK: fraud_alerts (Live Stream) │ JSON Alerts (استهلاك فوري للأنظمة)   │
└───────────────────────────────┴─────────────────────────────────┴──────────────────────────────────────┘
```

هذا التكامل يضمن أن طبقة بحيرة البيانات الخام (Bronze Layer) تحتوي دائماً على الحقيقة الكاملة للبيانات التاريخية واللحظية، مما يسمح لمحركات المعالجة اللاحقة في Databricks بدمج المعاملات مع بيانات العملاء والمرجعيات بكفاءة تامة.
