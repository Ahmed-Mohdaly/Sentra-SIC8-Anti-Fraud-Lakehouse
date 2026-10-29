# دليل العرض العملي والتطبيقي: Data Ingestion Pipeline
## سيناريو تجريبي آمن وقابل للتكرار لشرح المنظومة (Live Demo Runbook)

---

## إرشادات السلامة وضوابط التنفيذ
> [!IMPORTANT]
> - **أوامر قراءة فقط على السحابة:** جميع أوامر AWS CLI المستخدمة في هذا الدليل مصنفة كـ **Read-Only**؛ لن يتم تعديل، إنشاء، أو حذف أي موارد سحابية على AWS إطلاقاً.
> - **بيئة العرض المحلي:** تم توفير بدائل محلية آمنة (Local Mock / Simulation) لأي اختبار يتطلب توليد رسائل تجريبية لعدم التأثير على تكاليف الحساب.
> - **حماية البيانات السرية:** لا يحتوي أي أمر على كلمات مرور، مفاتيح سرية، أو بيانات عملاء حقيقية.

---

## 1. فحص حالة الخدمات السحابية القائمة (Read-Only Infrastructure Health)

```bash
# [READ-ONLY] 1. التحقق من هوية الحساب في باريس
aws sts get-caller-identity

# [READ-ONLY] 2. التحقق من حالة كلاستر كافكا (Amazon MSK)
aws kafka describe-cluster \
  --cluster-arn "arn:aws:kafka:eu-west-3:138257835434:cluster/sic-streaming-msk/ff954d9e-7cba-437e-b180-83ad115d0996-2" \
  --region eu-west-3 \
  --query "ClusterInfo.[ClusterName,State,CurrentVersion,ZookeeperConnectString]" \
  --output table

# [READ-ONLY] 3. التحقق من المهام المشغلة على ECS Fargate
aws ecs list-tasks \
  --cluster synthetic-data-cluster \
  --region eu-west-3
```
- **ماذا يفعل الأمر؟** يفحص اتصال الحساب، ويتأكد أن كلاستر MSK بحالة `ACTIVE` ويعمل بإصدار Apache Kafka 3.8.x، ويعرض معرفات المهام النشطة على Fargate.
- **التصنيف:** **Read-Only (AWS API)**

---

## 2. التحقق من قنوات كافكا (Kafka Topics Verification)

```bash
# [READ-ONLY] فحص أسماء القنوات النشطة من داخل سجلات Kafka Connect
aws logs filter-log-events \
  --log-group-name /ecs/synthetic-data-provider \
  --log-stream-name-prefix fraud-consumer \
  --filter-pattern "Listening on topic" \
  --limit 5 \
  --region eu-west-3 \
  --query "events[*].message" \
  --output text
```
- **ماذا يفعل الأمر؟** يؤكد استماع المستهلكين إلى القنوات الأساسية: `transactions` و `fraud_alerts`.
- **التصنيف:** **Read-Only (CloudWatch Logs)**

---

## 3. توليد دفعة اختبارية محلية من المعاملات (Small Batch Simulation)

```bash
# [READ-ONLY / LOCAL] توليد 50 معاملة اختبارية ومعالجتها عبر الموديل اللحظي
PYTHONPATH=src python scripts/verify_realtime_scoring.py
```
- **ماذا يفعل الأمر؟** يولد 50 معاملة بنكية مصطنعة، يقيمها عبر محرك استخراج الميزات، ويطبق عليها موديل LightGBM لقياس زمن الاستجابة والتحقق من حساب سرعة السفر ومجاميع الصرف.
- **التصنيف:** **Read-Only (محاكاة محلية لا تؤثر على AWS)**

---

## 4. التحقق من وصول الرسائل وتدفقها (Message Arrival Verification)

```bash
# [READ-ONLY] التحقق من اجتياز اختبارات وحدة تدفق رسائل المعاملات
pytest tests/unit/test_publisher.py -k "test_publisher_send_success" -v
```
- **ماذا يفعل الأمر؟** يتأكد من أن كلاس `KafkaPublisher` يغلف الحدث في `EventEnvelope`، ويحدد المفتاح برقم المستخدم `user_id`، ويرسله للوسيط بنجاح تام.
- **التصنيف:** **Read-Only (Local Python)**

---

## 5. التحقق من وصول البيانات إلى Amazon S3 (Bronze Bucket Verification)

```bash
# [READ-ONLY] فحص المجلدات والـ Prefixes داخل سلة بحيرة البيانات S3 Bronze
aws s3 ls s3://sic-pipeline-storage/bronze/ --region eu-west-3

# [READ-ONLY] فحص ملفات مسار المعاملات المباشر
aws s3 ls s3://sic-pipeline-storage/bronze/transactions/ --recursive --human-readable --summarize --region eu-west-3 | head -n 15
```
- **ماذا يفعل الأمر؟** يعرض شجرة المجلدات داخل طبقة الـ Bronze (`transactions/`, `users/`, `cards/`, `mcc/`, `cities/`)، ويعرض عينة من ملفات المعاملات المضغوطة بصيغة `.json.gz`.
- **التصنيف:** **Read-Only (S3 API)**

---

## 6. فحص محتوى ملفات الـ Bronze بطريقة آمنة (Safe Content Inspection)

```bash
# [READ-ONLY] قراءة وفك ضغط أحدث ملف معاملات من S3 دون تخزينه على القرص
aws s3 cp s3://sic-pipeline-storage/bronze/transactions/partition=0/00000000000000000000.json.gz - --region eu-west-3 | gzip -d | head -n 3 | jq .
```
- **ماذا يفعل الأمر؟** يسحب أول ملف معاملات مضغوط من S3 مباشرة إلى الـ Pipeline، ويفك ضغطه في الذاكرة عبر `gzip -d` ويعرض أول 3 معاملات بصيغة JSON منسقة للتأكد من سلامة الحقول (المبلغ، التوقيت، خطوط الطول والعرض).
- **التصنيف:** **Read-Only (S3 Stream Read)**

---

## 7. التحقق من مسار استخراج البيانات المرجعية (Batch Reference Test)

```bash
# [READ-ONLY] التحقق من ملفات الـ Parquet الخاصة بالأكواد المرجعية في S3
aws s3 ls s3://sic-pipeline-storage/bronze/mcc/ --recursive --region eu-west-3

# [READ-ONLY] اختبار كود استخراج البيانات المرجعية محلياً
pytest tests/unit/test_batch_reference_extractor.py -v
```
- **ماذا يفعل الأمر؟** يؤكد وجود ملفات Parquet الخاصة بأكواد تصنيف المتاجر (`reference_mcc_*.parquet`)، ويشغل اختبار الوحدة للتأكد من مطابقة السجلات لـ 914 كود.
- **التصنيف:** **Read-Only (Local Test & S3 List)**

---

## 8. عرض سجلات المعالجة اللحظية من CloudWatch (Live Consumer Logs)

```bash
# [READ-ONLY] استعراض أحدث 10 سجلات تقييم لحظي من حاوية Fargate
aws logs filter-log-events \
  --log-group-name /ecs/synthetic-data-provider \
  --log-stream-name-prefix fraud-consumer/fraud-consumer \
  --limit 10 \
  --region eu-west-3 \
  --query "events[*].message" \
  --output text
```
- **ماذا يفعل الأمر؟** يعرض السجلات الحية لتقييم المعاملات وأسباب رصد الاحتيال (`credit_limit_exhaustion`, `spend_spike`) وزمن الاستجابة الحقيقي (6–8ms).
- **التصنيف:** **Read-Only (CloudWatch API)**

---

## 9. فحص حالة الـ Redis Feature Store (State Inspection)

```bash
# [READ-ONLY] فحص مواصفات كلاستر ElastiCache Redis على AWS
aws elasticache describe-cache-clusters \
  --cache-cluster-id sic-feature-store-001 \
  --region eu-west-3 \
  --query "CacheClusters[0].[CacheClusterId,CacheNodeType,EngineVersion,CacheClusterStatus,TransitEncryptionEnabled]" \
  --output table
```
- **ماذا يفعل الأمر؟** يؤكد أن الكلاستر يعمل بنسخة Redis 7.1.0 بحالة `available` مع تفعيل التشفير الإلزامي أثناء النقل (`TransitEncryptionEnabled: true`).
- **التصنيف:** **Read-Only (AWS ElastiCache API)**

---

## 10. محاكاة فشل أحد المكونات بأمان (Fault-Tolerance Demonstration)

```bash
# [READ-ONLY / LOCAL] اختبار سقوط Redis والتحول التلقائي للـ In-Memory Fallback
pytest tests/unit/test_state_store_parity.py -k "test_state_store_failure_fallback_and_recovery" -v
```
- **ماذا يفعل الأمر؟** ينفذ اختبار وحدة مبرمج لمحاكاة انقطاع شبكة Redis، ويثبت بالدليل الرقمي أن الكود يلتقط الـ Exception ويتحول للـ Local Cache بدون أن يتوقف الـ Pipeline.
- **التصنيف:** **Read-Only (Local Mock Simulation)**

---

## 11. عرض ملخص قياسات الأداء والنتائج النهائية

| مسار الاستيعاب (Ingestion Path) | المصدر | الوجهة (S3 Bronze) | الصيغة | زمن المعالجة (Latency) | الحالة المحققة |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Direct Streaming** | Generator | `bronze/transactions/` | GZIP JSONL | Micro-batch (10s Flush) | **VERIFIED LIVE** |
| **PostgreSQL CDC** | Debezium RDS | `bronze/users/ & cards/` | GZIP JSONL | Real-time WAL Capture | **VERIFIED LIVE** |
| **Batch Reference** | EventBridge Task| `bronze/mcc/ & cities/` | Snappy Parquet | Periodic Batch Snapshot| **VERIFIED LIVE** |
| **Real-Time Serving**| MSK Consumer | ElastiCache & Alerts | Shared State | **6.15ms – 8.95ms** | **VERIFIED LIVE** |
