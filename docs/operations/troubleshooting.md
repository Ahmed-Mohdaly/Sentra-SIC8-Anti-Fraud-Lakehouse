# دليل استكشاف الأعطال وحلها بالأعراض (Troubleshooting Guide)
## كتيب الطوارئ الهندسية لمهندسي البيانات والعمليات في منصة SIC

---

> [!NOTE]
> **مسار الملف:** `docs/operations/troubleshooting.md`  
> **الهدف:** توفير دليل عملي سريع مصنف بحسب **الأعراض المرئية (Symptom-Based)**؛ لكل مشكلة: ما هي الأعراض، الأسباب المرجحة، أوامر التشخيص الدقيقة، الإجراء التصحيحي، وكيفية التأكد من زوال العطل.

---

## 1. فهرس المشاكل والأعراض الشائعة

1. [المولد متوقف ولا ينتج أي معاملات (`actual_tps = 0.0`)](#1-المولد-متوقف-ولا-ينتج-أي-معاملات)
2. [فشل الاتصال بوسيط كافكا (`Broker Connection Failures`)](#2-فشل-الاتصال-بوسيط-كافكا)
3. [لا توجد ملفات جديدة تظهر في S3 Bronze (`No Objects in S3`)](#3-لا-توجد-ملفات-جديدة-تظهر-في-s3)
4. [توقف موصل Debezium عن التقاط المستخدمين الجدد](#4-توقف-موصل-debezium-عن-التقاط-التعديلات)
5. [تراكم سجلات الـ WAL وامتلاء قرص PostgreSQL](#5-تراكم-سجلات-الـ-wal-وامتلاء-قرص-postgresql)
6. [فشل الاتصال بـ Redis وتحول النظام لـ In-Memory Fallback](#6-فشل-الاتصال-بـ-redis)
7. [تراكم استهلاك كافكا في مسار الاحتيال (`High Consumer Lag`)](#7-تراكم-استهلاك-كافكا-في-مسار-الاحتيال)
8. [فشل تحميل موديل الاحتيال (`LightGBM Model Loading Error`)](#8-فشل-تحميل-موديل-الاحتيال)
9. [وصول معاملات إلى مجلد العزل (`bronze/quarantine/`)](#9-وصول-معاملات-إلى-مجلد-العزل)
10. [فشل خط سير Airflow التزايدي (`DAG 1 Failures`)](#10-فشل-خط-سير-airflow-التزايدي)

---

## 2. الدليل التشخيصي والتصحيحي خطوة بخطوة

### 1. المولد متوقف ولا ينتج أي معاملات
* **الأعراض:** استدعاء `GET /api/v1/generator/status` يرجع بـ `"actual_tps": 0.0` أو `"state": "STOPPED"`.
* **الأسباب المرجحة:**
  1. الخدمة لم تُشغل بعد أو توقفت بسبب الوصول إلى الحد الأقصى للأحداث (`max_events`).
  2. فشل الاتصال بكافكا أو بالداتابيز عند الإقلاع.
* **أوامر التشخيص:**
  ```bash
  curl -s http://localhost:8000/api/v1/generator/status | jq .
  curl -s http://localhost:8000/ready | jq .
  ```
* **الإجراء التصحيحي:**
  * إعادة تشغيل المولد عبر الـ API:
    ```bash
    curl -X POST http://localhost:8000/api/v1/generator/start -H "X-API-Key: dev-api-key"
    ```
* **التحقق:** فحص تزايد عداد `total_tx_generated` بعد 5 ثوانٍ.

---

### 2. فشل الاتصال بوسيط كافكا
* **الأعراض:** ظهور رسائل `Broker: Leader not available` أو `Fail to resolve broker endpoint` في السجلات.
* **الأسباب المرجحة:** خطأ في اسم الـ Bootstrap Server، أو عدم فتح المنفذ `9092` في Security Group، أو أن الخدمة خارج الـ VPC.
* **أوامر التشخيص:**
  ```bash
  nc -zv b-1.sicstreamingmsk.cjm6ia.c3.kafka.eu-west-3.amazonaws.com 9092
  ```
* **الإجراء التصحيحي:**
  * التأكد من متغير البيئة `KAFKA_BOOTSTRAP_SERVERS`.
  * التحقق من أن Security Group الخاصة بالـ Broker تسمح بمرور الحركة من Security Group الخاصة بحاوية الـ Generator على المنفذ `9092`.

---

### 3. لا توجد ملفات جديدة تظهر في S3
* **الأعراض:** مجلد `bronze/transactions/` لا يستقبل ملفات جديدة رغم أن المولد شغال.
* **الأسباب المرجحة:** موصل Aiven S3 Sink متوقف أو يواجه مشكلة في صلاحيات IAM أو في شهادات STS.
* **أوامر التشخيص:**
  ```bash
  # فحص حالة الموصل
  curl -s http://172.31.10.236:8083/connectors/aiven-s3-sink/status | jq .
  ```
* **الإجراء التصحيحي:**
  * إذا ظهر خطأ متعلق بـ `Access Denied` أو `STS`: تشغيل سكريبت إصلاح الـ STS:
    ```bash
    python scripts/fix_s3_sink_sts.py
    ```
  * إعادة تشغيل الـ Task الفاشل في الموصل:
    ```bash
    curl -X POST http://172.31.10.236:8083/connectors/aiven-s3-sink/tasks/0/restart
    ```

---

### 4. توقف موصل Debezium عن التقاط التعديلات
* **الأعراض:** لا تظهر أحداث جديدة في موضوعي `users` أو `cards` عند إنشاء مستخدمين جدد.
* **الأسباب المرجحة:** تعطل موصل Debezium، أو مشكلة في صلاحيات المستخدم في قاعدة البيانات، أو تلف الـ Replication Slot.
* **أوامر التشخيص:**
  ```bash
  curl -s http://172.31.10.236:8083/connectors/debezium-postgres-source/status | jq .
  ```
* **الإجراء التصحيحي:**
  * إذا كان الـ Task في حالة `FAILED`: تشغيل سكريبت معالجة إعدادات Debezium:
    ```bash
    python scripts/fix_debezium_config.py
    ```
  * التأكد من أن الـ Publication سليم في PostgreSQL:
    ```sql
    SELECT * FROM pg_publication_tables WHERE pubname = 'debezium_publication';
    ```

---

### 5. تراكم سجلات الـ WAL وامتلاء قرص PostgreSQL
* **الأعراض:** انخفاض المساحة المتبقية لقرص RDS واقترابها من الصفر.
* **السبب:** الـ Replication Slot `debezium_cdc_slot` غير نشط بسبب توقف Debezium، فيمتنع خادم PostgreSQL عن مسح الـ WAL.
* **التشخيص:**
  ```sql
  SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS lag 
  FROM pg_replication_slots;
  ```
* **الإجراء التصحيحي:**
  * تشغيل حاوية Kafka Connect فوراً لتبدأ في قراءة السجلات وتفريغ الـ Slot.
  * **إجراء الطوارئ القصوى:** إذا كادت قاعدة البيانات أن تتوقف بسبب امتلاء القرص تماماً:
    ```sql
    SELECT pg_drop_replication_slot('debezium_cdc_slot');
    ```
    *(تنبيه: إسقاط الـ Slot سيفرغ القرص فوراً، ولكنه سيتطلب عمل Initial Snapshot لـ Debezium عند إعادة تشغيله).*

---

### 6. فشل الاتصال بـ Redis
* **الأعراض:** ظهور تحذير: `Redis connection failed; switching to In-Memory fallback mode` في سجلات خدمة الاحتيال.
* **الأسباب المرجحة:** خطأ في رابط الـ URL (استخدام `redis://` بدلاً من `rediss://` المشفر)، أو خطأ في كلمة المرور (AUTH Token)، أو قيود في Security Group.
* **التشخيص:**
  ```bash
  python -c "import redis; r = redis.Redis.from_url('rediss://default:PASS@ENDPOINT:6379'); print(r.ping())"
  ```
* **الإجراء التصحيحي:**
  * التأكد من أن الرابط يبدأ بـ `rediss://` (مع حرف s للتشفير).
  * فحص صلاحيات الوصول والتأكد من تطابق كلمة المرور مع AWS Secrets Manager.

---

### 7. تراكم استهلاك كافكا في مسار الاحتيال (`High Consumer Lag`)
* **الأعراض:** تزايد الـ Lag في موضوع `transactions` لمجموعة `fraud-scoring-group` وتأخر التنبيهات.
* **الأسباب المرجحة:** ارتفاع معدل الـ TPS عن قدرة خيط المعالجة الفردي، أو بطء استجابة Redis.
* **التشخيص:** فحص زمن الاستجابة عبر:
  ```bash
  python scripts/verify_realtime_scoring.py
  ```
* **الإجراء التصحيحي:**
  * زيادة عدد الـ Partitions لموضوع `transactions` (من 3 إلى 6).
  * تشغيل حاوية إضافية لخدمة `fraud-consumer` في ECS Fargate (Horizontal Auto-scaling) لتشارك الحمل.

---

### 8. فشل تحميل موديل الاحتيال (`LightGBM Error`)
* **الأعراض:** انهيار الحاوية عند الإقلاع بـ `ImportError: libgomp.so.1: cannot open shared object file`.
* **السبب:** غياب مكتبة OpenMP المشتركة التي تحتاجها نواة LightGBM C++ في بيئة Linux.
* **الإجراء التصحيحي:**
  * التأكد من تثبيت `libgomp1` في الـ Dockerfile (كما هو موثق في `Dockerfile.fraud-consumer` سطر 31).

---

### 9. وصول معاملات إلى مجلد العزل (`bronze/quarantine/`)
* **الأعراض:** ظهور ملفات جديدة تحت `bronze/quarantine/transactions/`.
* **السبب:** وجود معاملات تخالف القواعد المالية (مثل مبالغ سالبة في غير سيناريو الارتجاع).
* **التشخيص والإصلاح:**
  * قراءة سبب الرفض من الملف عبر AWS CLI:
    ```bash
    aws s3 cp s3://sic-pipeline-storage/bronze/quarantine/transactions/.../quarantine.jsonl - | head -n 1 | jq .rejection_reason
    ```
  * فحص كود المولد للتأكد من عدم وجود تضارب في أوزان السيناريوهات.

---

### 10. فشل خط سير Airflow التزايدي (`DAG 1 Failures`)
* **الأعراض:** تحول مهمة `task_extract_transactions` إلى اللون الأحمر (FAILED) في واجهة Airflow.
* **الأسباب المرجحة:** فشل قراءة أو تحديث جدول DynamoDB `ingestion_watermarks` بسبب قيود صلاحيات الـ IAM Role، أو خطأ في عنوان الـ API.
* **التشخيص:** فحص سجلات المهمة في Airflow والبحث عن `ClientError` الصادر من DynamoDB.
* **الإجراء التصحيحي:**
  * التأكد من أن الـ IAM Role الخاص بـ Airflow يملك صلاحيات: `dynamodb:GetItem` و `dynamodb:UpdateItem` على جدول `ingestion_watermarks`.
