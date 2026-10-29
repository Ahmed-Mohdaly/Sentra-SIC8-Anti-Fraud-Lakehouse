# حالة وتتبع منظومة التوثيق التقني الشامل (Documentation Status)
## منصة البيانات المالية التخليقية — Synthetic Financial Data Platform (SIC)

> [!NOTE]
> هذا الملف هو لوحة التحكم الحية (Living Dashboard) لتتبع حالة اكتمال الوثائق التقنية، الملفات التي تم فحصها كودياً، التناقضات المكتشفة بين النظري والتنفيذي، ومستويات التحقق الفعلي.

---

## 1. فهرس تتبع الملفات وحالة الإنجاز (Document Coverage Matrix)

| المسار / الملف | الموضوع الأساسي | الحالة (Status) | الملفات المفحوصة كودياً (Source Evidence) | الملاحظات ونطاق التحقق |
| :--- | :--- | :--- | :--- | :--- |
| [`docs/README.md`](file:///home/syd/Downloads/archive/docs/README.md) | الفهرس الرئيسي وخريطة القراءة للمبتدئين | 🟢 مكتمل | كامل المستودع | محدث بروابط تشعبية مباشرة ومسارات القراءة الـ 4 |
| [`docs/DOCUMENTATION_STATUS.md`](file:///home/syd/Downloads/archive/docs/DOCUMENTATION_STATUS.md) | لوحة التتبع وحصر الفروقات والتحقق | 🟢 مكتمل | ملفات المشروع بالكامل | لوحة التتبع الحية ومصفوفة الإنجاز |
| [`docs/00-project-overview.md`](file:///home/syd/Downloads/archive/docs/00-project-overview.md) | النظرة العامة والمشكلة ودورة حياة الحدث | 🟢 مكتمل | `src/`, `config/`, `docs/architecture.md` | شرح شامل مع Mermaid Diagram ودورة حياة المعاملة |
| [`docs/01-repository-guide.md`](file:///home/syd/Downloads/archive/docs/01-repository-guide.md) | دليل المستودع شجرة بشجرة ومجلد بمجلد | 🟢 مكتمل | شجرة المستودع الفعلية | تصنيف كل مجلد وملف تنفيذي وروابط الكود |
| [`docs/architecture/system-architecture.md`](file:///home/syd/Downloads/archive/docs/architecture/system-architecture.md) | المعمارية الكلية ومخططات C4 وتدفق البيانات | 🟢 مكتمل | `compose.yaml`, `scripts/`, `src/` | تفصيل الحدود والشبكات ومخططات C4 |
| [`docs/architecture/ingestion-paths.md`](file:///home/syd/Downloads/archive/docs/architecture/ingestion-paths.md) | تفصيل المسارات الأربعة المستقلة | 🟢 مكتمل | `generator_service.py`, `extractor.py` | مقارنة المسار اللحظي، CDC، المرجعي، والاستنتاج |
| [`docs/architecture/decisions-and-tradeoffs.md`](file:///home/syd/Downloads/archive/docs/architecture/decisions-and-tradeoffs.md) | قرارات التصميم المعماري والـ Tradeoffs | 🟢 مكتمل | الكود والإعدادات | توثيق 8 قرارات معمارية بأدلتها البرمجية |
| [`docs/services/generator-service.md`](file:///home/syd/Downloads/archive/docs/services/generator-service.md) | محرك المولد، الـ Worker، والـ 8 سيناريوهات | 🟢 مكتمل | `src/synthetic_streaming/generator_service.py` | تفصيل أوزان وحسابات السيناريوهات والـ RateController |
| [`docs/services/generator-api.md`](file:///home/syd/Downloads/archive/docs/services/generator-api.md) | توثيق نقاط FastAPI والـ Probes التشغيلية | 🟢 مكتمل | `src/synthetic_streaming/api.py` | كافة نقاط الـ Endpoints والـ Schemas وأوامر curl |
| [`docs/data-sources/postgresql-data-model.md`](file:///home/syd/Downloads/archive/docs/data-sources/postgresql-data-model.md) | نموذج قاعدة بيانات PostgreSQL والـ DDL | 🟢 مكتمل | `src/synthetic_streaming/database.py`, `sql/schema.sql` | جداول users, cards, mcc, cities, transactions |
| [`docs/data-sources/debezium-cdc.md`](file:///home/syd/Downloads/archive/docs/data-sources/debezium-cdc.md) | تقنية Debezium CDC وفك تشفير الـ WAL | 🟢 مكتمل | `docker/kafka-connect/register_connectors.sh` | Publication و Replication Slots والـ Multi-container Pod |
| [`docs/streaming/kafka-fundamentals-in-this-project.md`](file:///home/syd/Downloads/archive/docs/streaming/kafka-fundamentals-in-this-project.md) | أساسيات كافكا وتطبيقها في المشروع | 🟢 مكتمل | `kafka_publisher.py`, `config.py` | البارتشنز ومفاتيح التوزيع والـ acks والمجموعات |
| [`docs/streaming/kafka-topics-and-schemas.md`](file:///home/syd/Downloads/archive/docs/streaming/kafka-topics-and-schemas.md) | التوبيكس الـ 5 والـ Event Envelopes | 🟢 مكتمل | `event_schema.py`, `config.py` | Schema JSON ومواصفات كل Topic بالتفصيل |
| [`docs/streaming/kafka-connect-and-s3-sink.md`](file:///home/syd/Downloads/archive/docs/streaming/kafka-connect-and-s3-sink.md) | موصل Aiven S3 Sink وتفريغ الـ Micro-batch | 🟢 مكتمل | `register_connectors.sh`, `s3-sink.md` | تفريغ 50 سجلاً / 5 ثوانٍ وتنسيق المسارات |
| [`docs/storage/s3-bronze-layer.md`](file:///home/syd/Downloads/archive/docs/storage/s3-bronze-layer.md) | طبقة بحيرة البيانات S3 Bronze وتقسيمها | 🟢 مكتمل | `extractor.py`, `s3-sink.md` | هيكلية الفولدرات وعزل الـ Quarantine والتحويلات |
| [`docs/storage/data-formats-and-layout.md`](file:///home/syd/Downloads/archive/docs/storage/data-formats-and-layout.md) | مقارنة JSONL.GZ مع Snappy Parquet | 🟢 مكتمل | `extractor.py`, `batch_reference_extractor.py`| Row vs Columnar ومشاكل Small files وحلول الدلتا |
| [`docs/fraud-detection/fraud-scoring-pipeline.md`](file:///home/syd/Downloads/archive/docs/fraud-detection/fraud-scoring-pipeline.md) | دورة حياة التنبؤ بالاحتيال الفوري | 🟢 مكتمل | `src/ml/fraud_consumer.py` | تدفق المعالجة من الاستلام للـ Commit في 11 مرحلة |
| [`docs/fraud-detection/feature-engineering.md`](file:///home/syd/Downloads/archive/docs/fraud-detection/feature-engineering.md) | قاموس الـ 34 Feature والعمليات الرياضية | 🟢 مكتمل | `src/ml/feature_engineering.py` | Haversine, Log, Z-score, Speed وتطابق التدريب |
| [`docs/fraud-detection/online-feature-store.md`](file:///home/syd/Downloads/archive/docs/fraud-detection/online-feature-store.md) | متجر الخصائص Redis وحساب الـ Sliding Windows | 🟢 مكتمل | `src/ml/state_store.py` | ZSET, Hash, TTL 48h, In-Memory Fallback و Pipelines |
| [`docs/fraud-detection/model-training-and-inference.md`](file:///home/syd/Downloads/archive/docs/fraud-detection/model-training-and-inference.md) | تدريب واستنتاج موديل LightGBM v2 | 🟢 مكتمل | `models/v2/model_metadata.json`, `scripts/` | عتبة 0.0453، ROC-AUC 0.8746، وزمن 0.52ms |
| [`docs/orchestration/eventbridge-batch-ingestion.md`](file:///home/syd/Downloads/archive/docs/orchestration/eventbridge-batch-ingestion.md) | جدولة EventBridge واستخراج المرجعيات اليومي | 🟢 مكتمل | `scripts/manage_eventbridge_schedules.py` | Cron 02:00 UTC ومهمة Fargate واستخراج Arrow |
| [`docs/orchestration/airflow-dags.md`](file:///home/syd/Downloads/archive/docs/orchestration/airflow-dags.md) | تفصيل DAG 1 (التزايدي) و DAG 2 (المرجعي) | 🟢 مكتمل | `airflow/dags/dag_01_incremental_ingestion.py` | فحص DynamoDB Watermark و Databricks Trigger |
| [`docs/orchestration/databricks-processing.md`](file:///home/syd/Downloads/archive/docs/orchestration/databricks-processing.md) | معالجة بحيرة البيانات وتحويلات Bronze إلى Gold | 🟢 مكتمل | `databricks/stream/*.ipynb` | AutoLoader `cloudFiles` و Delta Lake و Z-Ordering |
| [`docs/reliability/data-quality-and-dlq.md`](file:///home/syd/Downloads/archive/docs/reliability/data-quality-and-dlq.md) | بوابات جودة البيانات وتوجيه الـ DLQ | 🟢 مكتمل | `extractor.py` (DataQualityGate) | عزل البيانات في S3 quarantine و DLQ والتنظيف |
| [`docs/reliability/fault-tolerance-and-delivery-guarantees.md`](file:///home/syd/Downloads/archive/docs/reliability/fault-tolerance-and-delivery-guarantees.md) | ضمانات التسليم والتعافي من الانهيار | 🟢 مكتمل | `kafka_publisher.py`, `fraud_consumer.py` | Offset Guard و At-least-once و Idempotency |
| [`docs/operations/monitoring-and-observability.md`](file:///home/syd/Downloads/archive/docs/operations/monitoring-and-observability.md) | مؤشرات الأداء، المراقبة، والـ Health Checks | 🟢 مكتمل | `api.py`, `scripts/verify_realtime_scoring.py` | TPS, Lag, Latency, Structured Logs |
| [`docs/operations/troubleshooting.md`](file:///home/syd/Downloads/archive/docs/operations/troubleshooting.md) | دليل استكشاف الأعطال وحلها بالأعراض | 🟢 مكتمل | نصوص الخطأ والأكواد الفعلية | Runbook لعلاج 10 أعطال تشغيلية شائعة بالأوامر |
| [`docs/getting-started/prerequisites-and-setup.md`](file:///home/syd/Downloads/archive/docs/getting-started/prerequisites-and-setup.md) | المتطلبات وإعداد البيئة التطويرية | 🟢 مكتمل | `pyproject.toml`, `.env.example` | Python 3.11, Docker, AWS CLI والمتغيرات |
| [`docs/getting-started/local-development.md`](file:///home/syd/Downloads/archive/docs/getting-started/local-development.md) | التشغيل المحلي عبر Docker Compose | 🟢 مكتمل | `compose.yaml` | أوامر التشغيل والـ Smoke Tests والتحقق |
| [`docs/deployment/aws-deployment.md`](file:///home/syd/Downloads/archive/docs/deployment/aws-deployment.md) | البنية التحتية ونشر الخدمات على AWS | 🟢 مكتمل | ملفات الـ scripts والـ Dockerfiles | ECS, RDS, MSK, ElastiCache, S3 والشبكات |
| [`docs/operations/operational-runbook.md`](file:///home/syd/Downloads/archive/docs/operations/operational-runbook.md) | دليل التشغيل اليومي لمهندسي العمليات | 🟢 مكتمل | السكريبتات الفعلية في `scripts/` | أوامر Start/Stop/Verify والفحوصات الدورية |
| [`docs/testing/testing-strategy.md`](file:///home/syd/Downloads/archive/docs/testing/testing-strategy.md) | استراتيجية الاختبارات (Unit, Integration, Load) | 🟢 مكتمل | مجلد `tests/` كاملاً | تغطية الاختبارات ومحاكاة الحمل واختبارات التعافي |
| [`docs/testing/end-to-end-validation.md`](file:///home/syd/Downloads/archive/docs/testing/end-to-end-validation.md) | سيناريو التحقق الشامل من البداية للنهاية | 🟢 مكتمل | `scripts/run_end_to_end_orchestration.py` | تتبع معاملة خطوة بخطوة بالتقارير الموثقة |
| [`docs/security/security-and-secrets.md`](file:///home/syd/Downloads/archive/docs/security/security-and-secrets.md) | الأمان، التشفير، وإدارة المفاتيح | 🟢 مكتمل | `config.py`, `Dockerfile.fraud-consumer` | TLS, KMS, Non-root containers, Token Sanitization |
| [`docs/operations/cost-and-capacity.md`](file:///home/syd/Downloads/archive/docs/operations/cost-and-capacity.md) | تحليل التكاليف والسعات الحوسبية السحابية | 🟢 مكتمل | `scripts/audit_phase2_cost_model.py` | تدقيق التكلفة الشهرية (~$337) وحدود التوسع |
| [`docs/learning/end-to-end-walkthrough.md`](file:///home/syd/Downloads/archive/docs/learning/end-to-end-walkthrough.md) | قصة تتبع معاملة واحدة من الألف للياء | 🟢 مكتمل | الكود بالكامل | دليل تعليمي تفصيلي لحدث واحد في مجوهرات باريس |
| [`docs/learning/interview-preparation.md`](file:///home/syd/Downloads/archive/docs/learning/interview-preparation.md) | أسئلة وإجابات المقابلات الفنية المعمارية | 🟢 مكتمل | System Design Scenarios | 10 أسئلة تخصصية عميقة مع إجاباتها المعمارية |
| [`docs/REFERENCE.md`](file:///home/syd/Downloads/archive/docs/REFERENCE.md) | المرجع السريع للأوامر والإعدادات والمفاهيم | 🟢 مكتمل | كافة ملفات الإعداد | Cheat Sheet سريع لكافة المسارات والأوامر والـ Topics |

---

## 2. سجل التناقضات والفروقات المكتشفة (Inconsistencies & Discrepancies Log)

خلال الفحص الهندسي العميق للكود المصدري، تم رصد عدة تناقضات واختلافات دقيقة بين التوصيف النظري القديم والتطبيق الفعلي في الكود:

### 1. مسار المعاملات اللحظي مقابل مسار الاستخراج التزايدي في Airflow
* **الادعاء النظري:** التوثيق المختصر يصف أحياناً استخراج المعاملات كمسار وحيد.
* **الواقع الكودي المكتشف:** يوجد في المشروع **مساران متوازيان للمعاملات**:
  1. **المسار اللحظي (Streaming Path):** يعمل عبر `generator_service.py` ويبث الأحداث مباشرة إلى كافكا (`transactions` topic) دون كتابة أي صف في PostgreSQL نهائياً (**Zero-DB-Inserts**).
  2. **مسار الاستخراج التزايدي (Incremental Batch Extraction via Airflow DAG 1):** ينفذ `ingestion.extractor.TransactionExtractor` لسحب المعاملات المخزنة في جدول `transactions` عبر الـ API (`GET /api/v1/transactions?since_id=X`)، ويحفظ علامات الإزاحة في **Amazon DynamoDB** (`ingestion_watermarks`)، ثم يرفع ملف Parquet إلى S3 Bronze، ويشغل مهمة Databricks.
* **الأثر المعماري:** المسار الأول مخصص للتدفق عالي السرعة (High-Velocity Event Streaming)، والمسار الثاني مخصص لترحيل الدفعات التزايدية من واجهة الـ REST API التقليدية. كلاهما موثق باستقلالية تامة.

### 2. آلية حفظ الـ Watermark في الاستخراج التزايدي
* **التحقق:** تم فحص `src/ingestion/extractor.py` وثبت أن الـ Watermark يُخزن بالفعل في جدول **Amazon DynamoDB** باسم `ingestion_watermarks` من خلال الكلاس `IngestionStateStore` مع شرط حماية ذري (`ConditionExpression`) لمنع الرجوع بالـ Offset إلى الوراء عند حدوث تنافسية.

### 3. عدد مواضيع كافكا وتسميات الـ Dead Letter Queue (DLQ)
* **المكتشف في الكود:** 
  * مواضيع كافكا التشغيلية الأساسية: `transactions`, `users`, `cards`, `fraud_alerts`, بالإضافة إلى `transaction_dlq` المعرّف في `config.py`.
  * موصل Aiven S3 Sink في `register_connectors.sh` معرّف ليستخدم موضوع DLQ خاص به باسم `sic_sink_dlq`.
  * تم توثيق كلا الموضوعين لتوضيح الفرق بين DLQ التطبيق البرمجي و DLQ موصل كافكا كونكت.

### 4. بنية متجر الخصائص اللحظي في Redis
* **المكتشف في الكود:** متجر الخصائص (`src/ml/state_store.py`) لا يستخدم مجرد مفاتيح عادية، بل يستخدم **Redis Sorted Sets (ZSET)** لحساب النوافذ المتحركة (1 ساعة و 24 ساعة) مع تنظيف دوري للبيانات الأقدم من 7 أيام (`zremrangebyscore`)، ويستخدم **Redis Hashes** لحساب المتوسط التراكمي والانحراف المعياري للإنفاق.

### 5. إصدار وعدد خصائص موديل كشف الاحتيال
* **المكتشف في الكود:** الموديل المعتمد فعلياً في الإنتاج هو الإصدار `v2_remediated` (`models/v2/fraud_lgbm_model.joblib`) مع ملف البيانات الوصفية `models/v2/model_metadata.json`. عدد الخصائص هو **34 خاصية بالضبط**، والعتبة المثلى المحددة إحصائياً هي **0.0453**، وزمن الاستنتاج هو **0.52 ميلي ثانية**.

---

## 3. العناصر قيد التحقق السحابي (Runtime Verification Limitations)
* يتم فحص صحة المكونات عبر الكود المصدري واختبارات الـ Unit والـ Integration المتوفرة محلياً.
* الاتصال الحي بخدمات AWS السحابية الفعلية (مثل Amazon MSK Cluster أو Databricks Workspace) يعتمد على توافر مفاتيح الاعتماد السحابية للبيئة أثناء وقت التنفيذ.
