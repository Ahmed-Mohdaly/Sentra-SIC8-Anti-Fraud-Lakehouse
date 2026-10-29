# كتيب التشغيل اليومي للمنظومة (Operational Runbook)
## دليل مهندسي العمليات: أوامر المراقبة، إدارة المعدلات، والتشغيل السحابي

---

> [!NOTE]
> **مسار الملف:** `docs/operations/operational-runbook.md`  
> **الجمهور المستهدف:** مهندسو العمليات (SRE)، مهندسو البيانات، ومسؤولو الدعم الفني للمنصة.  
> **المبدأ:** كافة الأوامر الواردة في هذا الدليل مجربة ومطابقة للسكريبتات ونقاط الـ API الفعلية.

---

## 1. الفحص الصباحي السريع لصحة المنظومة (Daily Health Sweep)

قم بتنفيذ هذا التسلسل كل صباح للتحقق من سلامة كافة أجزاء المنظومة:

```bash
# 1. فحص جاهزية واجهة برمجة التطبيقات وقاعدة البيانات
curl -s http://13.38.219.168:8000/ready | jq .
# النتيجة المتوقعة: "status": "ready"

# 2. فحص حالة وتدفق المولد اللحظي
curl -s http://13.38.219.168:8000/api/v1/generator/status | jq .
# النتيجة المتوقعة: "state": "RUNNING", "actual_tps" حول 10.0, "total_tx_failed": 0

# 3. فحص حالة موصلي كافكا كونكت (Debezium و S3 Sink)
curl -s http://172.31.10.236:8083/connectors/debezium-postgres-source/status | jq .connector.state
curl -s http://172.31.10.236:8083/connectors/aiven-s3-sink/status | jq .connector.state
# النتيجة المتوقعة لكلاهما: "RUNNING"

# 4. فحص استنتاج الاحتيال وسرعة قراءة Redis
python scripts/verify_realtime_scoring.py
# النتيجة المتوقعة: قراءة Redis < 2ms، واستنتاج LightGBM < 1ms
```

---

## 2. إدارة المولد ومعدل الـ TPS (Generator Rate Management)

### 2.1 بدء وإيقاف التوليد:
```bash
# بدء التوليد
curl -X POST http://localhost:8000/api/v1/generator/start \
  -H "X-API-Key: dev-api-key" | jq .

# إيقاف التوليد مع تفريغ الرسائل المعلقة (Flush)
curl -X POST http://localhost:8000/api/v1/generator/stop \
  -H "X-API-Key: dev-api-key" | jq .
```

### 2.2 تعديل سرعة التوليد (Scaling TPS Up/Down):
لتغيير السرعة أثناء التشغيل دون إعادة تشغيل الحاوية:
```bash
# رفع السرعة إلى 50 معاملة في الثانية
curl -X PATCH http://localhost:8000/api/v1/generator/config \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-api-key" \
  -d '{"target_tps": 50.0}' | jq .

# التحقق من أن السرعة الجديدة دخلت حيز التنفيذ:
curl -s http://localhost:8000/api/v1/generator/status | jq .actual_tps
```

---

## 3. التحقق من وصول البيانات إلى S3 Bronze (S3 Verification)

للتحقق من أن موصل Aiven S3 Sink يكتب الملفات بنجاح في بحيرة البيانات:

```bash
# 1. استعراض أحدث ملفات المعاملات المضغوطة في Partition 0
aws s3 ls s3://sic-pipeline-storage/bronze/transactions/partition=0/ --human-readable | tail -n 5

# 2. استعراض ملفات المستخدمين المنقولة عبر Debezium CDC
aws s3 ls s3://sic-pipeline-storage/bronze/users/partition=0/ --human-readable | tail -n 3

# 3. التحقق من خلو مجلد العزل (Quarantine)
aws s3 ls s3://sic-pipeline-storage/bronze/quarantine/transactions/
# إذا لم يرجع الأمر أي ملفات، فذلك يعني أن جميع المعاملات اجتازت بوابات الجودة بنسبة 100%!
```

---

## 4. تشغيل الاختبارات الشاملة (End-to-End Execution)

عند الرغبة في اختبار محاكاة خط سير Airflow و Databricks بالكامل:

```bash
# تشغيل سكريبت المحاكاة الشامل
python scripts/run_end_to_end_orchestration.py
```
**ما يفعله السكريبت:**
1. يفحص جاهزية الـ API عبر `/ready`.
2. يستخرج الدفعة التزايدية ويحفظ Parquet في S3 مع تحديث DynamoDB.
3. يفحص سلامة كائن S3 المتولد (ShortCircuit check).
4. يطلق مهمة Databricks Multi-Task Job `815854574980923`.
5. يطبع رابط المراقبة الحي لمهمة Databricks ويتابع حالتها حتى تنتهي بنجاح (`TERMINATED / SUCCESS`).

---

## 5. إعادة تشغيل الموصلات الفاشلة (Kafka Connect Recovery)

إذا توقف موصل كافكا كونكت بسبب خلل مؤقت في الشبكة:

```bash
# 1. إعادة تشغيل مهمة موصل Debezium
curl -X POST http://172.31.10.236:8083/connectors/debezium-postgres-source/tasks/0/restart

# 2. إعادة تشغيل مهمة موصل S3 Sink
curl -X POST http://172.31.10.236:8083/connectors/aiven-s3-sink/tasks/0/restart

# 3. التأكد من عودة الحالة إلى RUNNING
curl -s http://172.31.10.236:8083/connectors/aiven-s3-sink/status | jq .tasks[0].state
```
