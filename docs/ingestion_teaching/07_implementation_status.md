# مصفوفة حالة التنفيذ والأدلة الرقمية: Data Ingestion Pipeline
## التقرير الهندسي الشامل لحالة مكونات المنظومة ومسارات البيانات

---

## 1. جدول حالة المكونات ومسارات الاستيعاب (Component Status Matrix)

| المكون / مسار البيانات | حالة التنفيذ | مستوى التحقق | الدليل الفعلي المباشر | الملفات أو موارد AWS المرتبطة | الاختبارات المنفذة | الفجوة المتبقية لبيئة الإنتاج الكاملة |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **محرك التوليد البنكي** | **منفذ بالكامل** | **مختبر محلياً ومختبر على AWS** | يعمل كخدمة FastAPI ويبث المعاملات بهيكل `EventEnvelope`. | [`src/synthetic_streaming/generator_service.py`](file:///home/syd/Downloads/archive/src/synthetic_streaming/generator_service.py) | `tests/unit/test_publisher.py`<br>`scripts/verify_realtime_scoring.py` | لا توجد فجوة وظيفية. |
| **المسار 1: PostgreSQL CDC** | **منفذ بالكامل** | **مختبر حياً على AWS** | موصل Debezium يقرأ من `debezium_cdc_slot` ويعيد التوجيه عبر RegexRouter لـ `users` و `cards`. | موصل: `debezium-postgres-source`<br>RDS: `containerdb.cjm6iag8qrwi.eu-west-3.rds.amazonaws.com` | فحص الـ REST API في `scripts/fix_debezium_config.py`<br>تأكيد استخراج 3,126 مستخدم و 9,551 كارت | مراقبة حجم الـ WAL لتفادي امتلاء قرص RDS عند توقف الموصل. |
| **المسار 2: بث المعاملات المباشر** | **منفذ بالكامل** | **مختبر حياً على AWS** | إرسال المعاملات لحظياً بمفتاح `user_id` إلى قناة `transactions` على Amazon MSK. | كلاستر MSK: `sic-streaming-msk`<br>القناة: `transactions` (3 Partitions) | `tests/unit/test_publisher.py`<br>سجلات CloudWatch لمستهلك Fargate | لا توجد فجوة. التدفق مستمر ومثبت. |
| **المسار 3: استخراج المرجع الدفعي** | **منفذ بالكامل** | **مختبر حياً على AWS** | سحب لقطات الجداول المرجعية بـ SQLAlchemy وتحويلها لـ Parquet مضغوط بـ Snappy في S3. | كود: [`src/ingestion/batch_reference_extractor.py`](file:///home/syd/Downloads/archive/src/ingestion/batch_reference_extractor.py)<br>مهمة: `synthetic-ingestion-extractor:5` | `tests/unit/test_batch_reference_extractor.py`<br>تأكيد وجود ملفات `mcc` (27KB) و `cities` (38KB) في S3 | جدولة المهمة بـ EventBridge تعمل حالياً بمعدل 5 دقائق (يفضل تعديلها لتشغيل يومي). |
| **تفريغ S3 Bronze Lakehouse** | **منفذ بالكامل** | **مختبر حياً على AWS** | موصل Aiven S3 Sink يفرغ رسائل `transactions`, `users`, `cards` كملفات GZIP JSONL. | السلة: `s3://sic-pipeline-storage`<br>الموصل: `aiven-s3-sink` | كود الضبط: [`scripts/fix_s3_sink_sts.py`](file:///home/syd/Downloads/archive/scripts/fix_s3_sink_sts.py)<br>فحص ملفات الـ Bronze بالأمر `aws s3 ls` | عدم وجود سياسة دورة حياة تلقائية (S3 Lifecycle Policy) لأرشفة البيانات القديمة. |
| **المعالجة الفورية لكشف الاحتيال** | **منفذ بالكامل** | **مختبر حياً على AWS** | مستهلك Fargate يستهلك `transactions`، يقيمها بموديل LightGBM، ويحفظ الـ State في Redis. | المهمة: `sic-fraud-consumer:2`<br>السجل: `/ecs/synthetic-data-provider` | 22 اختبار وحدة ناجح (`test_ml_fraud.py`)<br>سجلات CloudWatch الحية بزمن 6.15–8.95ms | التنبيهات المكررة واردة عند سقوط المستهلك قبل الـ Commit (At-least-once). |
| **Redis Feature Store** | **منفذ بالكامل** | **مختبر حياً على AWS** | كلاستر ElastiCache Redis 7.1.0 مشفر بـ TLS ومرتبط بـ Fargate عبر `rediss://`. | الكلاستر: `sic-feature-store-001`<br>المعرف: `sic-feature-store` | قراءة السجلات الحية لـ Fargate<br>اختبارات التكافؤ `test_state_store_parity.py` | يعمل بنود واحدة (`cache.t4g.micro`) لترشيد تكاليف الـ Demo؛ يحتاج Multi-AZ في الإنتاج التجاري. |
| **طوابير الرسائل الميتة (DLQ)** | **منفذ جزئياً** | **مختبر محلياً** | توجيه الرسائل الفاسدة لقناة `transaction_dlq` ولموصل S3 لقناة `sic_sink_dlq`. | القنوات: `transaction_dlq`, `sic_sink_dlq` | اختبارات التحقق من استيعاب الأخطاء `test_malformed_event_handling` | لم يتم بناء مستهلك تلقائي لإعادة معالجة رسائل الـ DLQ (تتطلب فحصاً يدوياً). |
| **طبقات Silver & Gold Lakehouse**| **مقترح / كود تجريبي** | **مختبر محلياً / لم ينشر حياً** | كود بناء جداول Delta Lake ومعالجة الـ Aggregations بـ PySpark موجود في المستودع. | ملف: `scripts/build_silver_gold_lakehouse.py` | تجارب التحويل المحلي على عينات بيانات | لم يتم نشر كلاستر Databricks / EMR دائم على AWS لترشيد التكاليف. |

---

## 2. ملخص الأدلة الاختبارية والتحقق الرقمي

### 1. اختبارات الوحدة والتكامل المحلية (Pytest)
- **الأمر:** `pytest tests/unit/test_ml_fraud.py tests/unit/test_state_store_parity.py tests/unit/test_batch_reference_extractor.py -v`
- **النتيجة:** اجتياز **23 اختباراً** بنسبة نجاح **100%**.
- **الأدلة المحققة:**
  - التحقق من سلامة استخراج 34 ميزة لموديل كشف الاحتيال.
  - التحقق من دقة حساب المسافات الجغرافية (Haversine) وسرعات السفر.
  - التحقق من صحة استخراج الجداول المرجعية وتحويلها لـ Apache Parquet ببيانات مطابقة لقاعدة البيانات (914 MCC و 1,433 مدينة).

### 2. قياسات الأداء اللحظية (Latency Benchmark)
- **محلياً (In-Memory Fallback):** متوسط **0.52 ms** لكل معاملة (1,927 معاملة في الثانية لكل نواة معالج).
- **حياً على AWS Fargate (مع شبكة الـ VPC وتشفير الـ TLS):** يتراوح بين **6.15 ms و 8.95 ms** للمعاملة، وهو أقل من عتبة الـ SLA المحددة بـ 10ms.

### 3. التحقق المباشر من موارد AWS
- كلاستر **Amazon MSK** بحالة `ACTIVE` على إصدار Kafka 3.8.x.
- كلاستر **ElastiCache Redis** بحالة `available` مع تفعيل `TransitEncryptionMode: required`.
- سلة **Amazon S3** (`sic-pipeline-storage`) تحتوي على ملفات GZIP JSONL حية في مسارات `bronze/transactions/`, `bronze/users/`, و `bronze/cards/`، وملفات Parquet في `bronze/mcc/` و `bronze/cities/`.
