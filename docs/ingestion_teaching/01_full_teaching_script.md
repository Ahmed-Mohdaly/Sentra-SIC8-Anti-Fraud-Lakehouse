# سكريبت الشرح الشامل: معمارية وتدفق بيانات الـ Data Ingestion Pipeline
## دليل تدريسي متكامل بالعامية المصرية الهندسية لمهندسي البيانات

---

> [!NOTE]
> **طبيعة الوثيقة:** سكريبت تعليمي عملي جاهز للإلقاء المباشر أمام الطلاب أو مهندسي البيانات (Junior to Intermediate Data Engineers).
> **المدة الزمنية التقديرية للشرح:** 35 إلى 45 دقيقة.
> **لغة الشرح:** العامية المصرية الهندسية السلسة مع تثبيت المصطلحات التقنية، وأسماء خدمات AWS، ومسارات الكود باللغة الإنجليزية كما هي مستخدمة في بيئات العمل الاحترافية.

---

## فهرس فصول الشرح (Table of Contents)
- [الفصل الأول: مقدمة المشروع والمشكلة التي يحلها](#الفصل-الأول-مقدمة-المشروع-والمشكلة-التي-يحلها)
- [الفصل الثاني: شرح الـ Architecture بالكامل](#الفصل-الثاني-شرح-الـ-architecture-بالكامل)
- [الفصل الثالث: مصادر البيانات (Data Sources)](#الفصل-الثالث-مصادر-البيانات-data-sources)
- [الفصل الرابع: شرح Kafka و Amazon MSK عملياً](#الفصل-الرابع-شرح-kafka-و-amazon-msk-عملياً)
- [الفصل الخامس: شرح مسارات الـ Ingestion الثلاثة](#الفصل-الخامس-شرح-مسارات-الـ-ingestion-الثلاثة)
- [الفصل السادس: شرح Amazon S3 وطبقة Bronze](#الفصل-السادس-شرح-amazon-s3-وطبقة-bronze)
- [الفصل السابع: شرح تقنية الـ CDC والبيانات المرجعية](#الفصل-السابع-شرح-تقنية-الـ-cdc-والبيانات-المرجعية)
- [الفصل الثامن: شرح البنية التحتية على AWS Infrastructure](#الفصل-الثامن-شرح-البنية-التحتية-على-aws-infrastructure)
- [الفصل التاسع: شرح الكود الفعلي من البداية للنهاية](#الفصل-التاسع-شرح-الكود-الفعلي-من-البداية-للنهاية)
- [الفصل العاشر: الاختبارات والمراقبة والتحقق الرقمي](#الفصل-العاشر-الاختبارات-والمراقبة-والتحقق-الرقمي)
- [الفصل الحادي عشر: المشاكل الواقعية والقرارات الهندسية](#الفصل-الحادي-عشر-المشاكل-الواقعية-والقرارات-الهندسية)
- [الفصل الثاني عشر: الخاتمة وخلاصة المعمارية](#الفصل-الثاني-عشر-الخاتمة-وخلاصة-المعمارية)

---

## الفصل الأول: مقدمة المشروع والمشكلة التي يحلها
*(مدة الإلقاء: 2.5 - 3 دقائق)*

"مساء الخير يا شباب، وأهلاً بيكم في جلسة النهاردة!

النهاردة هنتكلم عن عصب أي منصة بيانات حديثة في البنوك والـ FinTech: **إزاي نبني Data Ingestion Pipeline متكامل، مرن، وقابل للتوسع على السحابة (AWS Cloud)؟**

خلونا نبدأ بالسؤال البديهي اللي أي حد ممكن يسأله: هو إحنا ليه أصلاً بنعمل Data Ingestion Pipeline؟ وليه مش بننقل الداتا بـ Script عادي مرة واحدة كل يوم بالليل ونخلص؟

في الأنظمة البنكية الحقيقية، الداتا مبتستناش! كل ثانية في آلاف المعاملات بتتسحب على كروت ائتمان، في عملاء جدد بيفتحوا حسابات، وفي كروت بتتجدد أو تتلغي. لو اعتمدت على النقل اليومي التقليدي (**Nightly Batch Load**)، معناه إن فريق التحليلات وموديلات كشف الاحتيال هيكونوا شغالين على بيانات متأخرة 24 ساعة كاملة! لو في كارت اتسرق دلوقتي، البنك مش هيعرف إلا تاني يوم الصبح بعد ما يكون الحساب اتصفى. 

عشان كده بنحتاج **Streaming Ingestion مستمر**، يلتقط الحدث في أجزاء من الثانية من الـ Source ويوصله للمخازن المركزية لحظياً.

بس المشكلة الحقيقية مش بس في السرعة؛ المشكلة إن البيانات مش نوع واحد! في مشروعنا ده، عندنا 3 أصناف مختلفة تماماً من البيانات:
1. **بيانات معاملات حية سريعة (`transactions`):** دي High-Volume Streaming Events بتتدفق بمعدل ثابت من الـ Payment Gateways.
2. **بيانات حسابات تشغيلية في قاعدة بيانات (`users` و `cards`):** دي بتعيش جوه OLTP Database (PostgreSQL)، ومحتاجين نلتقط أي تعديل فيها لحظياً بدون ما نضغط على أداء قاعدة البيانات التشغيلية.
3. **بيانات مرجعية بطيئة التغير (Reference Data):** زي أكواد تصنيف المتاجر (`merchant_category_codes`) والمدن (`city_reference`).

الهدف النهائي من الـ Pipeline اللي بنيناه هو:
1. تجميع كل التدفقات دي بدون فقدان أي رسالة (**Zero Data Loss**).
2. هبوط البيانات في بحيرة البيانات (**Data Lakehouse Bronze Layer**) على **Amazon S3** بصيغ قياسية ومضغوطة عشان فرق الـ Analytics والـ BI يشتغلوا عليها بـ Databricks أو Athena.
3. تغذية مسار كشف احتيال فوري (**Real-Time Fraud Detection**) بموديل Machine Learning بيقيم المعاملة في أقل من 10 ميلي ثانية.

تعالوا بقى ندخل جوه المعمارية ونشوف إزاي القطع دي كلها اتركبت مع بعضها."

---

## الفصل الثاني: شرح الـ Architecture بالكامل

"تعالوا نبص على الرسمة المعمارية الكاملة للنظام، عشان نفهم البيانات بتتحرك منين ورايحة فين:

```mermaid
flowchart TD
    subgraph SOURCES["1. مصادر البيانات (Data Sources Tier)"]
        GEN["Synthetic Data Provider\n(FastAPI / Python Generator)"]
        RDS[("Amazon RDS PostgreSQL\n(containerdb: users, cards, ref)")]
    end

    subgraph STREAMING["2. طبقة المراسلة والوساطة (Streaming Tier)"]
        subgraph MSK["Amazon MSK (Apache Kafka 3.8.x)"]
            T_TX["Topic: transactions\n(Partitioned Stream)"]
            T_USR["Topic: users\n(CDC Events)"]
            T_CRD["Topic: cards\n(CDC Events)"]
            T_ALERT["Topic: fraud_alerts\n(Scored Alerts)"]
            T_DLQ["Topic: transaction_dlq\n(Poison Pills)"]
        end
    end

    subgraph CONNECT["3. خوادم الـ Integration (Kafka Connect on ECS Fargate)"]
        DEB["Debezium PostgreSQL Source Connector\n(Reads WAL via debezium_cdc_slot)"]
        SINK["Aiven S3 Sink Connector\n(Micro-batch flusher, JSONL.GZ)"]
    end

    subgraph STORAGE["4. طبقة التخزين المجمعة (Amazon S3 Bronze Lakehouse)"]
        S3_TX["s3://sic-pipeline-storage/bronze/transactions/"]
        S3_USR["s3://sic-pipeline-storage/bronze/users/"]
        S3_CRD["s3://sic-pipeline-storage/bronze/cards/"]
        S3_REF["s3://sic-pipeline-storage/bronze/mcc/ & cities/\n(Snappy Parquet via EventBridge Extractor)"]
    end

    subgraph SERVING["5. المعالجة الفورية (Real-Time Fraud Serving Tier)"]
        FC["ECS Fargate: sic-fraud-consumer\n(LightGBM Inference Engine)"]
        REDIS[("AWS ElastiCache Redis\n(sic-feature-store: Online State)")]
    end

    %% Flow Connections
    GEN -->|Path 2: Direct Kafka Streaming| T_TX
    GEN -->|SQL Inserts / Updates| RDS
    RDS -->|Path 1: WAL Logical Replication| DEB
    DEB -->|Stream Changes (RegexRouter)| T_USR
    DEB -->|Stream Changes (RegexRouter)| T_CRD

    T_TX -->|Consume Streams| SINK
    T_USR -->|Consume CDC| SINK
    T_CRD -->|Consume CDC| SINK

    SINK -->|Write Partitions| S3_TX
    SINK -->|Write Partitions| S3_USR
    SINK -->|Write Partitions| S3_CRD

    RDS -.->|Path 3: Batch Reference Extractor\n(EventBridge Triggered ECS Task)| S3_REF

    T_TX -->|Consume Stream for Fraud| FC
    FC <-->|TLS rediss:// State Hydration| REDIS
    FC -->|Emit High Risk Flag| T_ALERT
    GEN -.->|Corrupted Payloads| T_DLQ
```

### شرح أجزاء الرسمة بالعربي:
1. **Data Sources Tier:** عندنا خدمة مولد البيانات المصطنع (**Synthetic Generator**) بتنتج تدفق حركات واقعي ومطابق للمواصفات البنكية، بالإضافة لقاعدة بيانات **PostgreSQL على Amazon RDS**.
2. **Streaming Tier (Amazon MSK):** ده السيرفر المركزي لإدارة الطوابير عبر **Apache Kafka 3.8.x** موزعة على وسيطين (Brokers).
3. **Kafka Connect Tier:** كونتينر شغال على **AWS ECS Fargate** بيشيل اثنين Connectors: الأول **Debezium** لقراءة تغييرات قاعدة البيانات، والتاني **Aiven S3 Sink** لتحويل الرسائل لملفات مضغوطة في S3.
4. **Bronze Storage Tier (Amazon S3):** الحاوية النهائية لكل البيانات الخام في بحيرة البيانات بدون أي تعديل أو تشويه.
5. **Real-Time Serving Tier:** مستهلك مستقل على Fargate مع **Redis Feature Store** وموديل ذكاء اصطناعي **LightGBM** لفرز المعاملات فورياً."

---

## الفصل الثالث: مصادر البيانات (Data Sources)

"تعالوا نفهم البيانات اللي بتغذي النظام ده جاية منين بالضبط من واقع الكود:

### 1. الـ Synthetic Data Provider (`src/synthetic_streaming/`):
في مشاريع البنوك، ممنوع منعاً باتاً استخدام بيانات العملاء الحقيقية في التطوير والـ Demo عشان قوانين الـ GDPR وسرية الحسابات. لذلك المشروع بيحتوي على محرك محاكاة بنكي كامل مبني بـ Python و FastAPI:
* بيبدأ بتحميل Baseline ديموغرافي من ملفات مرجعية (`sd254_users.csv` فيها 2,000 مستخدم، و `sd254_cards.csv` فيها 6,100 كارت).
* المحرك بيولد معاملات واقعية بـ Markov Chain وسيناريوهات صرف بشرية: مشتريات بقالة، مطاعم، اشتراكات رقمية، وسحوبات ATM، وسيناريوهات احتيال مصطنعة (سحب في أوقات متأخرة، أو قفزات مكانية).

### 2. الفرق بين أنواع البيانات في المشروع:
* **بيانات المعاملات (`transactions` - Streaming Events):** بيانات متغيرة بسرعة، كل معاملة ليها `event_id` (UUID)، توقيت المعاملة، رقم الكارت، المبلغ، إحداثيات GPS للمتجر، وكود الـ MCC. دي بتمشي كـ JSON Envelope في Stream مباشر.
* **بيانات المستخدمين والبطاقات (`users` و `cards` - Operational CDC):** دي متخزنة في جداول علائقية في PostgreSQL. المستخدم بيغير عنوانه أو بيجدد كارت، فدي بيانات حالة (Stateful Entity Data).
* **بيانات المرجع (`mcc` و `cities` - Static Reference Data):** أكواد الـ Merchant Category Codes (914 كود) وإحداثيات 1,433 مدينة. دي بيانات شبه ثابتة بنسحبها كـ Snapshots دورية."

---

## الفصل الرابع: شرح Kafka و Amazon MSK عملياً

"ليه بنستخدم **Kafka**؟ وليه اخترنا **Amazon MSK** بالتحديد؟

### 1. الفرق بين Apache Kafka و Amazon MSK:
Apache Kafka هو المحرك مفتوح المصدر لإدارة الـ Distributed Event Streaming. لو نزلته بنفسك على EC2، هتتعب جداً في إدارة الـ Zookeeper/KRaft، والـ OS Patches، ومراقبة الـ Disks والـ Broker Failures.
**Amazon MSK (Managed Streaming for Apache Kafka)** هو خدمة مُدارة بالكامل من AWS؛ بتوفر لك كلاسترات Kafka بضغطة زر مع High Availability، وتوزيع تلقائي على الـ Availability Zones، وتشفير جاهز للـ Storage والـ Network.

### 2. المفاهيم الأساسية من واقع إعدادات مشروعنا:
* **الـ Broker:** هو السيرفر المشغل لـ Kafka. الكلاستر بتاعنا على MSK اسمه `sic-streaming-msk` فيه **2 Brokers** من نوع `kafka.t3.small` في باريس (`eu-west-3`):
  * `b-1.sicstreamingmsk.4io9um.c2.kafka.eu-west-3.amazonaws.com:9092`
  * `b-2.sicstreamingmsk.4io9um.c2.kafka.eu-west-3.amazonaws.com:9092`
* **الـ Topic:** القناة اللي بتستقبل نوع محدد من الرسائل. عندنا `transactions`، `users`، `cards`، و `fraud_alerts`.
* **الـ Partition والترتيب (Ordering):** 
  الـ Topic مش ملف واحد؛ هو مقسم لـ Partitions (عندنا 3 Partitions لكل Topic). 
  **قاعدة ذهبية في Kafka:** *الترتيب مضمون بنسبة 100% داخل نفس الـ Partition فقط، لكن غير مضمون عبر الـ Partitions المختلفة!*
  عشان كده بنستخدم **Message Key**: لما بنبعت Transaction بنحط الـ Key هو `user_id`. بكده كل معاملات نفس المستخدم بتروح لنفس الـ Partition دائماً، وبالتالي تاريخ صرف العميل يفضل مرتب زمنياً بدون أي لخبطة!
* **الـ Consumer Group والـ Offset:**
  الـ Consumer Group (زي `sic-fraud-consumer-group`) بيمكننا نشغل أكتر من نسخة للبرنامج يتقاسموا الـ Partitions.
  الـ **Offset** هو رقم تسلسلي فريد بيعبر عن مكان الـ Consumer في الطابور.
  في مشروعنا، قافلين الـ Auto-Commit (`enable.auto.commit: false`) وبنعمل Manual Commit بعد التأكد من معالجة الرسالة عشان نحقق عقد **At-Least-Once Delivery**."

---

## الفصل الخامس: شرح مسارات الـ Ingestion الثلاثة

"في المشروع ده عندنا **3 مسارات استيعاب مستقلة ومدروسة هندسياً**، تعالوا نفصص كل مسار من الألف للياء:

### المسار الأول: مسار الـ CDC للحسابات (`users` و `cards`)
1. **المصدر:** جداول `public.users` و `public.cards` على Amazon RDS PostgreSQL.
2. **الاستخراج:** عبر تقنية Logical Replication بمحرك Debezium اللي بيراقب الـ Write-Ahead Log (WAL) من خلال Replication Slot اسمه `debezium_cdc_slot`.
3. **الإرسال:** Debezium Source Connector بينقل الحدث كـ JSON لـ MSK.
4. **إعادة التوجيه (SMT):** بنستخدم `RegexRouter` لتحويل اسم الـ Topic من `sic.public.users` إلى `users` مباشرة.
5. **التخزين النهائي:** الـ Aiven S3 Sink Connector بيستهلك الـ Topic ويكتبه في `s3://sic-pipeline-storage/bronze/users/` و `bronze/cards/`.
6. **الصيغة:** ملفات Line-delimited JSON مضغوطة بـ GZIP (`.json.gz`).
7. **التقسيم (Partitioning):** مسارات متوافقة مع Hive: `partition={partition}/{start_offset}.json.gz`.
8. **معالجة الأخطاء:** تفعيل DLQ خاص بالـ Sink باسم `sic_sink_dlq`.
9. **التحقق:** مقارنة عدد السجلات المستخرجة (3,126 مستخدم، 9,551 كارت) بالأصل في قاعدة البيانات.
10. **القيود:** لو توقف Debezium لفترة طويلة، الـ WAL بيفضل يتراكم على ديسك الـ RDS (20 GB) وممكن يملى المساحة لو مفيش مراقبة.

---

### المسار الثاني: مسار المعاملات المباشر (`transactions`)
1. **المصدر:** خدمة توليد البيانات (`generator_service.py`).
2. **التوليد:** إنشاء أحداث معاملات لحظية بهيكل `EventEnvelope` موحد.
3. **الإرسال:** عبر كلاس `KafkaPublisher` في بايثون مستخدماً مكتبة `confluent-kafka`.
4. **الوسيط:** Amazon MSK Topic باسم `transactions` بـ 3 Partitions.
5. **التخزين النهائي:** نفس الـ Aiven S3 Sink Connector بيسحب الحركات ويكتبها في S3.
6. **الصيغة:** JSONL.GZ بمعدل تفريغ (Flush) كل 1,000 سجل أو كل 10 ثوانٍ (`flush.timeout.ms: 10000`).
7. **التقسيم:** `bronze/transactions/partition=X/000000000000000XXXXX.json.gz`.
8. **معالجة الأخطاء:** الرسائل التي تفشل في الفحص الهيكلي تُرسل فوراً لـ `transaction_dlq`.
9. **التحقق:** فحص استهلاك الـ Offsets وتدفق ملفات الـ Gzip في الـ Bronze Bucket.
10. **القيود:** في فترات انخفاض الترافيك، الملفات بتستنى 10 ثوانٍ كاملة قبل ما تتكتب في S3.

---

### المسار الثالث: مسار البيانات المرجعية المجمعة (Batch Reference Extractor)
1. **المصدر:** جداول الأكواد والمدن (`merchant_category_codes` و `city_reference`) على RDS.
2. **الاستخراج:** سكريبت بايثون مستقل (`src/ingestion/batch_reference_extractor.py`) بيشتغل كل فترة.
3. **الإرسال:** قراءة الداتا بـ SQLAlchemy وتحويلها لـ `pyarrow.Table`.
4. **الوسيط:** مفيش وسيط Kafka هنا! دي معمارية Point-to-Batch مباشرة لترشيد التكاليف وعدم استهلاك شبكة Kafka في داتا ثابتة.
5. **التخزين النهائي:** `s3://sic-pipeline-storage/bronze/mcc/` و `bronze/cities/`.
6. **الصيغة:** ملفات **Apache Parquet** عالية الكفاءة مضغوطة بخوارزمية **Snappy**.
7. **التقسيم:** تقسيم زمني يومي: `snapshot_date=YYYY-MM-DD/reference_mcc_YYYYMMDD_HHMMSS.parquet`.
8. **معالجة الأخطاء:** فحص الـ Schema الصارم ورفض الـ Upload لو عدد الصفوف أقل من المتوقع.
9. **التحقق:** تأكيد وجود 914 كود متجر و 1,433 مدينة في S3 بحجم 27KB و 38KB فقط.
10. **القيود:** المسار مجدول بـ EventBridge؛ لو حصل تعديل مفاجئ في كود متجر، التعديل مش هيظهر في S3 إلا مع الرنة المجدولة القادمة."

---

## الفصل السادس: شرح Amazon S3 وطبقة Bronze

"ليه اخترنا **Amazon S3** كوجهة نهائية للـ Bronze Layer؟ وليه مينفعش نكتفي بـ Kafka؟

### 1. الفرق بين Kafka و S3:
* **Kafka** هو Message Bus لحظي وسريع جداً، لكنه مش معمول لتخزين تاريخ الشركة لسنوات طويلة. تكلفة أقراص التخزين التابعة للـ Brokers غالية جداً، والاحتفاظ بمليارات الرسائل جواه بيبطئ عمليات الفهرسة وإعادة التوازن (Rebalance).
* **Amazon S3** هو Object Storage بيوفر متانة أسطورية بنسبة 99.999999999% (11 9's of Durability) بتكلفة رخيصة جداً ($0.023 لكل جيجابايت شهرياً)، ومدعوم مباشرة من كل محركات التحليلات الضخمة زي Databricks، Snowflake، و AWS Athena.

### 2. إزاي الداتا بتتحول من Streaming لملفات في S3؟
الـ **Aiven S3 Sink Connector** شغال كـ Micro-Batch Engine:
بيجمع رسائل الـ Kafka في الذاكرة لحد ما يتحقق واحد من شرطين:
* إما يوصل عدد السجلات لـ **1,000 رسالة** (`file.max.records: 1000`).
* أو يعدي **10 ثوانٍ** على أقدم رسالة (`flush.timeout.ms: 10000`).
أول ما شرط يتحقق، بيضغط الرسائل بـ Gzip ويكتب ملف واحد نظيف في S3.

### 3. تنظيم الـ Prefixes وتفادي مشكلة الـ Small Files Problem:
الملفات بتتسجل بالنمط التالي:
`bronze/transactions/partition=0/00000000000000020000.json.gz`
- استخدام أصفار في البداية (`padding=true`) بيخلي الملفات تترتب زمنياً وأبجدياً بشكل طبيعي.
- تحديد 1,000 سجل كحد أدنى بيحمي بحيرة البيانات من كارثة **The Small Files Problem** (لما السيستم ينشئ آلاف الملفات بحجم 1KB، وده بيدمر أداء محركات الاستعلام زي Spark و Athena).

### 4. القيمة العظيمة للـ Raw Bronze Layer:
البيانات بتدخل الـ Bronze زي ما جت من المصدر بالضبط بدون أي تنظيف أو تغيير. ده بيضمن إننا نقدر نعمل **Audit كامل**، ولو اكتشفنا Bug في كود المعالجة بعد شهرين، نقدر نعمل Re-process للداتا الخام من أول وجديد بدون أي خسارة."

---

## الفصل السابع: شرح CDC و Reference Data

"تعالوا نغوص في التقنية اللي بتميز مهندس البيانات المحترف: **Change Data Capture (CDC)**.

### 1. يعني إيه CDC وليه بنستخدمه؟
الطريقة القديمة لسحب بيانات الجداول كانت الـ Polling: نعمل كويري `SELECT * FROM users WHERE updated_at > last_sync`. 
الطريقة دي كارثية في الإنتاج لأنها:
1. بتعمل حمل تقيل على قاعدة البيانات (CPU & I/O).
2. مبتعرفش تكتشف السجلات اللي اتمسحت (`DELETE`) لأنها ببساطة اختفت من الجدول!
3. ممكن تفوت التعديلات السريعة لو صف اتعدل مرتين بين عمليتي الـ Polling.

**الحل:** تقنية الـ CDC! 
محرك **Debezium** مش بيعمل كويري على الجداول أصلاً؛ هو بيقرا ملف الـ **Write-Ahead Log (WAL)** اللي محرك PostgreSQL بيكتبه على القرص فور حدوث المعاملة. 

### 2. هيكل رسالة الـ CDC الفعلية:
رسالة الـ CDC اللي بتوصل لـ Kafka بتحتوي على تفاصيل الحدث بالكامل:
* `before`: صورة الحقول قبل التعديل.
* `after`: صورة الحقول بعد التعديل.
* `op`: نوع العملية (`c` للإنشاء Create، `u` للتحديث Update، `d` للحذف Delete، و `r` للقراءة الأولية Read Snapshot).
* `ts_ms`: التوقيت الزمني الدقيق بالملي ثانية لوقوع العملية على السيرفر.

### 3. الفرق بين الـ Snapshot والـ Continuous CDC:
لما Debezium اشتغل أول مرة في مشروعنا، كان في وضع `snapshot.mode: initial`:
* سحب أولاً لقطة كاملة لكل المستخدمين القدامى (3,126 مستخدم) والكروت (9,551 كارت) وضخهم في الـ Topics.
* بعد ما خلص، مسك في الـ WAL وفضل يراقب في وضع الـ Streaming اللحظي."

---

## الفصل الثامن: شرح AWS Infrastructure

"كل قطعة كود في المشروع ده متظبط وراها خدمة سحابية مختارة بعناية على AWS في منطقة باريس (`eu-west-3`). تعالوا نستعرض البنية التحتية المؤكدة وجودها:

| الخدمة (AWS Service) | اسم المورد (Resource Name / Identifier) | الدور في المنظومة | التبرير الهندسي لاختيارها |
| :--- | :--- | :--- | :--- |
| **Amazon MSK** | `sic-streaming-msk` | الوسيط المركزي لرسائل الـ Kafka | التخلص من تعقيدات إدارة وتحديث كلاسترات كافكا يدوياً. |
| **ECS Fargate** | `synthetic-data-cluster` | تشغيل الـ Containers بدون سيرفرات | بيئة Serverless Containers بتلغي الحاجة لإدارة سيرفرات EC2. |
| **Amazon RDS** | `containerdb` (PostgreSQL 18.3) | قاعدة البيانات التشغيلية للعملاء | قاعدة بيانات علائقية تدعم التكرار المنطقي بـ `wal_level = logical`. |
| **Amazon S3** | `sic-pipeline-storage` | بحيرة البيانات الخام (Bronze Layer) | تخزين دائم رخيص غير محدود السعة بمتانة 11 تسعات. |
| **ElastiCache Redis** | `sic-feature-store` (Node: `cache.t4g.micro`) | Online Feature Store لكشف الاحتيال | استرجاع وتحديث الـ Sliding Windows في أقل من 2 ميلي ثانية. |
| **Amazon EventBridge** | `scheduled-ingestion-extractor` | جدولة تشغيل سحب البيانات المرجعية | تشغيل المهمة الدورية كـ Serverless Cron Job بدون سيرفر شغال 24 ساعة. |
| **AWS Secrets Manager**| `synthetic-data-provider/database` | إدارة وحماية كلمات مرور الداتابيز | منع وضع الباسوردات في ملفات الكود أو بيئة التشغيل كـ Plaintext. |

### شبكة الـ VPC والأمان:
كل الخدمات دي محبوسة جوه **Private VPC** (`vpc-0d66731212396d1e9`) عبر Subnets موزعة على 3 مناطق توفر (`eu-west-3a`, `eu-west-3b`, `eu-west-3c`):
- الـ MSK والـ Redis والـ RDS معندهمش أي Public IP.
- الاتصال محكوم بـ **Security Groups** صارمة جداً بمبدأ **Least Privilege**: الـ Redis بورت 6379 لا يفتح إلا للكونتينر الخاص بالـ Fraud Consumer، والـ RDS بورت 5432 لا يقبل اتصالات إلا من داخل الـ VPC."

---

## الفصل التاسع: شرح الكود الفعلي من البداية للنهاية

"تعالوا بقى نبص في الكود الحقيقي جوه الـ Repository ونشوف المنطق البرمجي اتكتب إزاي:

### 1. ناشر رسائل كافكا (`src/synthetic_streaming/kafka_publisher.py`):
الكود بيستخدم مكتبة `confluent_kafka.Producer`:
```python
def publish_transaction(self, envelope: EventEnvelope) -> bool:
    payload_bytes = envelope.model_dump_json().encode("utf-8")
    user_key = str(envelope.payload.get("User", 0)).encode("utf-8")
    self.producer.produce(
        topic=self.config.transactions_topic,
        key=user_key,
        value=payload_bytes,
        on_delivery=self._delivery_report
    )
    self.producer.poll(0)
```
- **ليه الكود مكتوب كده؟**
  - استخدام `user_key` بيضمن توجيه كل معاملات نفس المستخدم لنفس الـ Partition.
  - استدعاء `poll(0)` بيخدم الـ Delivery Callbacks بدون ما يعطل سرعة إرسال الـ Loop.

### 2. سحب البيانات المرجعية (`src/ingestion/batch_reference_extractor.py`):
```python
# تحويل البيانات الناتجة من الكويري لـ Apache Parquet Table بقيم ومخطط نوعي صارم
arrow_table = pa.Table.from_pydict(column_data, schema=schema)
pq_buffer = io.BytesIO()
pq.write_table(arrow_table, pq_buffer, compression="snappy")
s3_client.put_object(
    Bucket=target_bucket,
    Key=f"bronze/{table_name}/snapshot_date={today}/{file_name}",
    Body=pq_buffer.getvalue()
)
```
- **ليه الكود مكتوب كده؟**
  - بيستخدم `io.BytesIO()` لإنشاء ملف الـ Parquet في الذاكرة مباشرة ورفعه لـ S3 بدون الحاجة لكتابة ملفات مؤقتة على ديسك الكونتينر المحلي، وده بيوفر I/O ويسرع التنفيذ.

### 3. إعداد الـ S3 Sink وتفادي خطأ الـ STS الشهير (`scripts/fix_s3_sink_sts.py`):
```json
{
  "connector.class": "io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector",
  "aws.s3.region": "eu-west-3",
  "aws.sts.config.endpoint": "https://sts.eu-west-3.amazonaws.com"
}
```
- **السر الهندسي هنا:** موصل Aiven كان بيفشل ويعطي `SignatureDoesNotMatch` لأنه كان بيكلم الـ Global STS افتراضياً. السطر الصغير ده أجبره يكلم الـ Regional STS Endpoint في باريس وحل المشكلة تماماً!"

---

## الفصل العاشر: الاختبارات والمراقبة والتحقق الرقمي

"أي مهندس شاطر لازم يثبت نجاح شغله بالأرقام والأدلة (**Empirical Verification**). إحنا إزاي اتأكدنا إن الـ Ingestion شغال صح؟

### 1. اختبارات الوحدة (Unit & Parity Tests):
نفذنا 22 اختبار وحدة متكامل عبر Pytest:
`pytest tests/unit/test_ml_fraud.py tests/unit/test_state_store_parity.py -v`
- النتيجة: **22 passed in 1.45s** بنسبة نجاح 100%.
- تم التحقق من سلامة معالجة الرسائل الفاسدة، وتطابق معادلات الـ Sample Variance.

### 2. اختبار الـ Batch Reference Extractor (`tests/unit/test_batch_reference_extractor.py`):
- تم التحقق من مطابقة عدد السجلات: 914 كود متجر في PostgreSQL اتكتبوا 914 سطر في ملف الـ Parquet، و 1,433 مدينة اتكتبوا 1,433 سطر بدون أي فقدان حقول.

### 3. المراقبة وسجلات CloudWatch الحية:
من خلال تتبع الـ Log Group `/ecs/synthetic-data-provider`:
- تم التحقق من عمل Fargate Consumer وسحب المعاملات بمعدل استجابة حي بين **6.15ms و 8.95ms** للمعاملة، وده أقل بكتير من الحد الأقصى المسموح (10ms SLA).
- تم التحقق من تسجيل أخطاء غياب الـ Topic في البداية وتوقف الـ Offset عن التقدم لحين إنشاء الـ Topic، وده أثبت عمل حزام الأمان بنجاح."

---

## الفصل الحادي عشر: المشاكل الواقعية والقرارات الهندسية

"ما فيش مشروع حقيقي بيطلع للنور من غير مطبات هندسية. تعالوا نشارك أهم 4 مشاكل ظهرت في المشروع وحليناها إزاي:

### 1. مشكلة فشل توثيق موصل S3 مع AWS STS:
* **اللي حصل:** الـ S3 Sink Connector كان بيقع ويعطي خطأ: `AWSSecurityTokenServiceException: Credential should be scoped to a valid region`.
* **السبب:** الموصل المفتوح المصدر من Aiven كان بيستخدم الـ Endpoint الافتراضي العالمي `sts.amazonaws.com` بدلاً من الإقليمي الخاص بباريس.
* **الحل:** تم حقن الإعداد `"aws.sts.config.endpoint": "https://sts.eu-west-3.amazonaws.com"` في إعدادات الموصل، واشتغل فوراً بدون أخطاء.

### 2. مشكلة سقف المعالجات لخدمة Fargate (vCPU Quota Limit):
* **اللي حصل:** عند محاولة تشغيل كونتينر جديد، كان ECS بيعطي خطأ placement failure ويرفض بدء الـ Task.
* **السبب:** حساب AWS في منطقة باريس عليه Hard Quota مقيدة بـ **2.0 vCPUs** لخدمة Fargate. الخدمات القائمة كانت واخدة 1.5 vCPUs، والمتبقي 0.5 vCPU فقط!
* **الحل:** تم تقليص حجم كونتينر الـ Fraud Consumer لـ **0.25 vCPU (256 CPU units)** ورام **1024 MB**، فاشتغل بسلاسة جوه المساحة المتبقية بدون الحاجة لانتظار موافقة الدعم الفني.

### 3. مشكلة تراكم ملفات الـ WAL في PostgreSQL:
* **اللي حصل:** اكتشفنا إن الـ Storage في RDS بدأ يزيد بشكل غير معتاد.
* **السبب:** Debezium لما بيقف، محرك البوستجريس بيفضل يحتفظ بملفات الـ Write-Ahead Logs في الـ Replication Slot ومبيحذفهاش لأنه مستني Debezium يرجع يقراها.
* **الحل:** ضبط تنبيهات CloudWatch على مساحة الديسك في RDS والتأكد من استمرار عمل الـ Connector بشكل دائم.

### 4. مشكلة تعارض الـ Consumer Groups عند الـ Cutover:
* **اللي حصل:** تشغيل كونتينرين بيقروا من نفس الـ Topic بنفس الـ Group ID وكل واحد عنده State Store مختلف.
* **السبب:** Kafka بيقسم الـ Partitions، فكان نص الحركات بيروح للـ RAM والنص التاني لـ Redis، وده سبب تضارب في حساب سرعات السفر.
* **الحل:** اعتماد خطة Cutover صريحة بإيقاف النسخة القديمة تماماً والانتظار حتى اكتمال الـ Rebalance قبل تشغيل النسخة الجديدة."

---

## الفصل الثاني عشر: الخاتمة وخلاصة المعمارية
*(مدة الإلقاء: دقيقة واحدة)*

> "في ختام شرحنا يا شباب، نقدر نقول إننا نجحنا في بناء **Data Ingestion Pipeline بنكي متكامل على AWS**:
> 
> جمعنا بين السرعة الفائقة لـ **Amazon MSK**، ودقة الـ **CDC** عبر **Debezium** و **PostgreSQL RDS**، وأمان واستمرارية التخزين في الـ **Bronze Layer على Amazon S3**، مع محرك استجابة فوري لكشف الاحتيال مدعوم بـ **ElastiCache Redis** وموديل **LightGBM** في أقل من 10 ميلي ثانية.
> 
> النظام بيفصل بوضوح بين مسؤوليات المكونات، بيحمي الـ State من الضياع، وبيحترم قيود التكلفة والأمان السحابي. شكراً ليكم جداً وجاهز لأي أسئلة في تفاصيل الكود أو المعمارية!"
