# الدليل المرجعي السريع لمنصة استيعاب البيانات (Quick Reference Sheet)
## مذكرات المهندس الميدانية: الأوامر، المتغيرات، الـ Endpoints، والمواضيع السحابية

---

> [!NOTE]
> **مسار الملف:** `docs/REFERENCE.md`  
> **الهدف:** توفير مرجع سريع مركز (Cheat Sheet) لكافة الأرقام، المسارات، الأوامر، والمتغيرات المعتمدة فعلياً في الكود المصدري لمشروع SIC.

---

## 1. مداخل المنظومة وحزم الكود (Entry Points)

* **خدمة المولد والـ API:** `src/synthetic_streaming/api.py` (FastAPI التطبيق الرئيسي).
* **خيط التوليد الخلفي:** `src/synthetic_streaming/generator_service.py` (كلاس `GeneratorService`).
* **مستهلك الاحتيال اللحظي:** `src/ml/fraud_consumer.py` (أمر: `python -m ml.fraud_consumer`).
* **مستخرج المعاملات التزايدي:** `src/ingestion/extractor.py` (كلاس `TransactionExtractor`).
* **مستخرج المرجعيات اليومي:** `src/ingestion/batch_reference_extractor.py` (كلاس `BatchReferenceExtractor`).
* **مهام الجدولة (Airflow DAGs):**
  * `airflow/dags/dag_01_incremental_ingestion.py` (جدول: `*/15 * * * *`).
  * `airflow/dags/dag_02_batch_reference.py` (جدول: `0 0 * * *`).

---

## 2. جدول متغيرات البيئة الأساسية (Environment Variables)

```bash
# البيئة وقواعد البيانات
APP_ENV=development                                      # development / production
DATABASE_URL=postgresql://user:pass@host:5432/dbname    # رابط اتصال PostgreSQL
SECRET_ID=synthetic-data-provider/database              # معرف AWS Secrets Manager

# واجهة الـ API والتحكم
API_HOST=0.0.0.0
API_PORT=8000
API_KEY=dev-api-key                                     # رمز المصادقة في ترويسة X-API-Key

# محرك التوليد اللحظي
GENERATOR_RATE=10.0                                     # معدل الـ TPS المستهدف
GENERATOR_BATCH_SIZE=50
AUTO_SEED=true
BASELINE_DATA_DIR=data                                  # مسار ملفات الـ CSV الأساسية

# شبكة البث الحي و Apache Kafka
ENABLE_KAFKA=true
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_SECURITY_PROTOCOL=PLAINTEXT                       # PLAINTEXT أو SSL أو SASL_SSL
KAFKA_TRANSACTIONS_TOPIC=transactions
KAFKA_USERS_TOPIC=users
KAFKA_CARDS_TOPIC=cards
KAFKA_FRAUD_ALERTS_TOPIC=fraud_alerts
KAFKA_DLQ_TOPIC=transaction_dlq

# متجر الخصائص اللحظي وخدمة الاحتيال
REDIS_URL=rediss://default:token@endpoint:6379          # رابط Redis المشفر بـ TLS
FRAUD_MODEL_PATH=models/v2/fraud_lgbm_model.joblib

# بحيرة البيانات وتكامل السحابة
S3_BUCKET=sic-pipeline-storage
AWS_REGION=eu-west-3
DYNAMODB_TABLE=ingestion_watermarks                     # جدول علامات الإزاحة التزايدي
DATABRICKS_JOB_ID=815854574980923                       # معرف مهمة بحيرة البيانات
```

---

## 3. مواضيع كافكا المعتمدة (Kafka Topics Catalog)

| اسم الموضوع | البارتشنز | مفتاح التوزيع (Partition Key) | دور الموضوع في المنصة |
| :--- | :--- | :--- | :--- |
| **`transactions`** | **3** | `user_id` (String) | البث المباشر لكافة المعاملات المالية اللحظية |
| **`users`** | **3** | `user_id` (String) | أحداث تعديل المستخدمين الصادرة من Debezium CDC |
| **`cards`** | **3** | `user_id` (String) | أحداث تعديل البطاقات الصادرة من Debezium CDC |
| **`fraud_alerts`** | **1** | `transaction_id` (String) | تنبيهات المعاملات المشبوهة الصادرة من موديل الاحتيال |
| **`transaction_dlq`** | **1** | `event_id` (String) | طابور عزل المعاملات التالفة المعمارية |
| **`sic_sink_dlq`** | **1** | المفتاح الأصلي للرسالة | طابور عزل الأخطاء الخاص بموصل Aiven S3 Sink |

---

## 4. جداول قاعدة بيانات PostgreSQL (`containerdb`)

* **`public.users`:** السجل الديموغرافي للعملاء (PK: `user_id`).
* **`public.cards`:** بيانات بطاقات الدفع الصادرة (Composite PK: `user_id, card_index`).
* **`public.merchant_category_codes`:** 914 كود تصنيف تجاري (PK: `mcc`).
* **`public.city_reference`:** 1,433 مدينة بإحداثياتها الجغرافية (PK: `city_id`).
* **`public.transactions`:** الجدول التاريخي للدفعات واستعلامات الـ API القديمة (PK: `id`).

---

## 5. هيكلية مسارات بحيرة البيانات: `s3://sic-pipeline-storage/`

* `bronze/transactions/partition={P}/{start_offset}.json.gz`: معاملات البث اللحظي.
* `bronze/users/partition={P}/{start_offset}.json.gz`: أحداث CDC المستخدمين.
* `bronze/cards/partition={P}/{start_offset}.json.gz`: أحداث CDC البطاقات.
* `bronze/mcc/year={Y}/month={M}/mcc_{timestamp}.snappy.parquet`: لقطات المتاجر اليومية.
* `bronze/cities/year={Y}/month={M}/cities_{timestamp}.snappy.parquet`: لقطات المدن اليومية.
* `bronze/quarantine/transactions/year={Y}/month={M}/day={D}/quarantine_{run_id}.jsonl`: عزل البيانات التالفة.

---

## 6. أهم أوامر التشغيل والتحقق السريع (Operational Commands)

```bash
# 1. فحص الجاهزية التشغيلية للـ API:
curl -s http://localhost:8000/ready | jq .

# 2. فحص حالة المولد ومعدل الـ TPS:
curl -s http://localhost:8000/api/v1/generator/status | jq .

# 3. تعديل سرعة التوليد اللحظي:
curl -X PATCH http://localhost:8000/api/v1/generator/config \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-api-key" \
  -d '{"target_tps": 25.0}'

# 4. فحص موصلات كافكا كونكت:
curl -s http://172.31.10.236:8083/connectors | jq .
curl -s http://172.31.10.236:8083/connectors/debezium-postgres-source/status | jq .
curl -s http://172.31.10.236:8083/connectors/aiven-s3-sink/status | jq .

# 5. قياس سرعة استنتاج الاحتيال و Redis:
python scripts/verify_realtime_scoring.py

# 6. تشغيل سيناريو التحقق الشامل (Airflow -> S3 -> Databricks):
python scripts/run_end_to_end_orchestration.py

# 7. إدارة جداول EventBridge السحابية:
python scripts/manage_eventbridge_schedules.py status

# 8. تشغيل كافة اختبارات الوحدة:
pytest tests/unit/ -v
```
