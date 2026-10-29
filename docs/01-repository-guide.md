# دليل تشريح مستودع الكود (Repository Architecture Guide)
## الخريطة الهندسية الشاملة لمجلدات وملفات مشروع SIC

---

> [!NOTE]
> **مسار الملف:** `docs/01-repository-guide.md`  
> **الهدف:** استعراض شجرة المشروع الفعلية مجلداً بمجلد وملفاً بملف، مع تصنيف كل عنصر: هل هو كود تشغيلي إنتاجي (Runtime)، بنية تحتية (Infrastructure)، اختبارات (Testing)، أدوات سكرام وتشغيل (Scripts)، أم ملفات بيانات وتوثيق (Data & Docs).

---

## 1. الشجرة الهيكلية الكاملة للمستودع (Actual Repository Tree)

تم استخراج هذه الشجرة الحقيقية من بيئة العمل مباشرة:

```text
/home/syd/Downloads/archive/
│
├── .env.example                               # قالب متغيرات البيئة والأسرار
├── .dockerignore                              # استثناءات بناء صور الدوكر
├── .gitignore                                 # استثناءات مستودع الجيت
├── Dockerfile                                 # بناء صورة المولد وواجهة الـ API الأساسية
├── Dockerfile.fraud-consumer                  # بناء صورة مستهلك كشف الاحتيال اللحظي (LightGBM)
├── compose.yaml                               # تشغيل البيئة التطويرية المتكاملة محلياً (Docker Compose)
├── pyproject.toml                             # إعدادات المشروع وحزم بايثون
├── README.md                                  # الوصف الأصلي لمشروع المولد
├── generator.db                               # قاعدة بيانات SQLite محلية لتجارب المولد السريعة
├── requirements-fraud-consumer.txt            # متطلبات بايثون المخصصة لخدمة الاحتيال وموديل LightGBM
│
├── src/                                       # النواة البرمجية للمنظومة (Core Runtime Code)
│   ├── synthetic_streaming/                   # حزمة توليد البيانات، الـ API التشغيلي، وناشر كافكا
│   ├── ingestion/                             # حزمة الاستخراج التزايدي والمرجعي إلى S3
│   └── ml/                                    # حزمة كشف الاحتيال اللحظي، متجر الخصائص، والمستهلك
│
├── airflow/                                   # خطوط سير العمل وجدولة المهام (Orchestration DAGs)
│   └── dags/                                  # مجلد تعريف الـ DAGs المجدولة
│       ├── dag_01_incremental_ingestion.py   # خط سير الاستخراج التزايدي كل 15 دقيقة وتشغيل Databricks
│       └── dag_02_batch_reference.py          # خط سير استخراج المرجعيات اليومي الساعة 00:00 UTC
│
├── docker/                                    # تكوينات الحاويات والخدمات المساندة
│   └── kafka-connect/                         # بناء وتسجيل موصلات Kafka Connect (Debezium + Aiven)
│       ├── Dockerfile                         # إعداد صورة Kafka Connect مع الموصلات
│       ├── register_connectors.sh             # سكريبت تسجيل موصلي Debezium و S3 Sink عبر REST API
│       ├── docker-entrypoint-wrapper.sh       # سكريبت بدء تشغيل الحاوية
│       └── s3-connector.zip                   # أرشيف موصل Aiven S3 Sink الجاهز
│
├── databricks/                                # دفاتر معالجة وتحويل البيانات في Databricks
│   ├── stream/                                # دفاتر معالجة البث وقراءة S3 Bronze عبر AutoLoader
│   └── *.ipynb                                # دفاتر استكشاف البيانات والـ Profiling والـ Silver Layers
│
├── models/                                    # النماذج الرياضية المدربة وبياناتها الوصفية
│   ├── fraud_lgbm_model.joblib                # الإصدار v1 من موديل الاحتيال (Legacy)
│   ├── model_metadata.json                    # بيانات الإصدار v1
│   └── v2/                                    # الإصدار v2 المعتمد إنتاجياً (Production Model)
│       ├── fraud_lgbm_model.joblib            # الموديل المعالج من التسريب (Remediated Model)
│       └── model_metadata.json                # بيانات التدريب، الـ 34 Feature، والعتبة المثلى
│
├── data/                                      # ملفات البيانات التاريخية الأساسية (Cold-Start Baselines)
│   ├── sd254_users.csv                        # بيانات المستخدمين التاريخية الأساسية
│   ├── sd254_cards.csv                        # بيانات البطاقات التاريخية الأساسية
│   └── ...                                    # ملفات المعاملات والمرجعيات التاريخية
│
├── scripts/                                   # سكريبتات التشغيل، الاختبار، والمراقبة (Operational Tools)
│   ├── run_end_to_end_orchestration.py        # المحاكي الكامل لدورة Airflow -> S3 -> Databricks
│   ├── run_functional_pipeline_test.py        # فحص شامل للمسارات الثلاثة وسلامة كافكا وقاعدة البيانات
│   ├── run_cloud_stress_test.py               # اختبار حمل وإجهاد المنصة السحابية
│   ├── train_fraud_model_remediated.py        # سكريبت تدريب موديل LightGBM v2 ومعالجة التسريب
│   ├── verify_realtime_scoring.py             # فحص سرعة الاستنتاج اللحظي وزمن الاستجابة
│   ├── manage_eventbridge_schedules.py        # تفعيل/تعطيل ومراقبة جداول EventBridge
│   ├── fix_debezium_config.py                 # تصحيح وتحديث إعدادات موصل Debezium على Fargate
│   ├── fix_s3_sink_sts.py                     # معالجة إعدادات الأمان ونقاط AWS STS لموصل S3 Sink
│   └── ...                                    # سكريبتات إعداد البيانات وقواعد البيانات
│
├── sql/                                       # نصوص ومخططات قواعد البيانات العلائقية
│   └── init_schema.sql                        # استعلامات DDL لإنشاء جداول Postgres (users, cards, etc.)
│
├── tests/                                     # حزمة الاختبارات الشاملة (Testing Suite)
│   ├── unit/                                  # اختبارات الوحدة (22 ملف اختبار لمختلف الكلاسات)
│   ├── integration/                           # اختبارات التكامل والترابط (Kafka & E2E)
│   └── load/                                  # اختبارات قياس معدلات الأداء والـ Throughput
│
└── docs/                                      # منظومة التوثيق التقني والهندسي الشامل (Documentation)
```

---

## 2. تفصيل المجلدات التشغيلية الأساسية (Runtime Subsystems)

### 2.1 مجلد النواة البرمجية: `src/`

#### أ. حزمة المولد والبث: `src/synthetic_streaming/`
هذه الحزمة هي المسؤولة عن تشغيل خدمة توليد المعاملات، الـ API، والاتصال بكافكا:

| اسم الملف | المسؤولية والوظيفة (Responsibility) | التصنيف | الاعتماديات الأساسية |
| :--- | :--- | :--- | :--- |
| `config.py` | إدارة إعدادات Pydantic، متغيرات البيئة، إعدادات الأمان (SASL/TLS)، وأوزان السيناريوهات. | **Runtime Core** | `pydantic`, `pydantic_settings` |
| `api.py` | تطبيق FastAPI يقدم نقاط التحكم التشغيلي، الـ Health Probes، وسحب المعاملات التزايدي. | **Runtime Core** | `fastapi`, `sqlalchemy` |
| `generator_service.py` | المنسق العام لدورة حياة المولد؛ يشغل الـ Worker Thread، يتحكم بالـ Rate، وينشر الأحداث. | **Runtime Core** | `RateController`, `TransactionsGenerator` |
| `rate_controller.py` | منظم زمني عالي الدقة يعتمد على `time.monotonic()` للحفاظ على الـ Target TPS دون طفرات. | **Runtime Core** | مكتبات بايثون القياسية |
| `kafka_publisher.py` | طبقة التجريد لناشر كافكا؛ تدعم `confluent-kafka` الإنتاجي والناشر الوهمي في الذاكرة. | **Runtime Core** | `confluent_kafka` |
| `scenario_engine.py` | محرك حساب السيناريوهات المالية الثمانية ومحاكاة مبالغها وأخطائها المنطقية. | **Runtime Core** | `ScenarioWeights` |
| `transactions_generator.py`| تجميع مكونات المعاملة، الربط مع المستخدم والبطاقة، وتطبيق بوابات الفحص. | **Runtime Core** | `ScenarioEngine`, `SourceSimulator` |
| `users_generator.py` | توليد كيانات المستخدمين الديموغرافية وتحديث أرقام الهوية في قاعدة البيانات. | **Runtime Core** | `UserModel`, `database.py` |
| `cards_generator.py` | توليد بطاقات الدفع المتوافقة مع المستخدمين وتحديد حدودها الائتمانية وتواريخها. | **Runtime Core** | `CardModel`, `database.py` |
| `classification_and_geo.py`| مطابقة أكواد المتاجر والمدن الجغرافية وتوليد وسائل الدفع المتوافقة. | **Runtime Core** | `MerchantCategoryCodeModel` |
| `database.py` | تعريف نماذج SQLAlchemy العلائقية (`UserModel`, `CardModel`, إلخ) ودورات الاتصال. | **Runtime Core** | `sqlalchemy` |
| `event_schema.py` | هياكل Pydantic للأحداث وغلاف الحدث الموحد (`EventEnvelope`). | **Runtime Core** | `pydantic` |
| `relationship_validator.py`| صمام أمان يفحص في الذاكرة أن المستخدم يملك البطاقة قبل إصدار المعاملة. | **Runtime Core** | داخلي |
| `baseline_loader.py` | قراءة بيانات الـ CSV التأسيسية لتغذية المولد بأرقام ومحددات أولية عند التشغيل. | **Runtime Core** | `data/` |
| `source_simulator.py` | محاكاة قنوات الدفع (POS, Web, Wallet, ATM) بأوزان إحصائية محددة. | **Runtime Core** | `SourceSystemWeights` |
| `worker.py` | غلاف إضافي لتشغيل مهام التوليد كـ CLI Command منفصل. | **Tool/CLI** | `generator_service.py` |

#### ب. حزمة الاستخراج والترحيل: `src/ingestion/`
هذه الحزمة متخصصة في سحب البيانات وترحيلها كملفات Parquet إلى S3:

| اسم الملف | المسؤولية والوظيفة (Responsibility) | التصنيف | الاعتماديات الأساسية |
| :--- | :--- | :--- | :--- |
| `extractor.py` | محرك الاستخراج التزايدي للمعاملات من واجهة الـ API، وحفظ الـ Watermark في DynamoDB. | **Batch Ingestion** | `boto3`, `pyarrow`, `httpx` |
| `batch_reference_extractor.py` | استخراج لقطات الجداول المرجعية (MCC والمدن) يومياً من Postgres إلى S3 Parquet. | **Batch Ingestion** | `boto3`, `pyarrow`, `sqlalchemy` |
| `ingest_reference_csvs.py` | أداة لتهيئة قاعدة بيانات Postgres المرجعية بتحميل ملفات الـ CSV الأولية. | **Setup Tool** | `database.py` |

#### ج. حزمة كشف الاحتيال الفوري: `src/ml/`
هذه الحزمة تدير مسار الذكاء الاصطناعي اللحظي:

| اسم الملف | المسؤولية والوظيفة (Responsibility) | التصنيف | الاعتماديات الأساسية |
| :--- | :--- | :--- | :--- |
| `fraud_consumer.py` | مستهلك كافكا الرئيسي؛ يقرأ المعاملات، يسحب الميزات، ويشغل الموديل، وينشر التنبيهات. | **Real-Time ML** | `confluent_kafka`, `StateStore`, `LightGBM` |
| `feature_engineering.py` | محرك هندسة الميزات الرياضي؛ يحسب الـ 34 خاصية (المسافات الجغرافية والنوافذ الزمنية). | **Real-Time ML** | `numpy`, `math` |
| `state_store.py` | متجر الخصائص اللحظي؛ يدير الاتصال بـ Redis مع وجود بديل في الذاكرة (In-Memory Fallback). | **Real-Time ML** | `redis`, `json` |

---

## 3. مجلد خطوط سير العمل: `airflow/dags/`

يحتوي على الـ DAGs المجدولة التي تعمل في بيئة إنتاج Apache Airflow:
1. **[`dag_01_incremental_ingestion.py`](file:///home/syd/Downloads/archive/airflow/dags/dag_01_incremental_ingestion.py):**
   * **الجدول:** كل 15 دقيقة (`*/15 * * * *`).
   * **الدور:** يفحص جاهزية الـ API السحابي عبر `/ready`، يشغل `TransactionExtractor` لسحب المعاملات التزايدية وتخزينها في S3 Bronze Parquet، ثم يفحص وجود بيانات جديدة (ShortCircuit) لتشغيل مهمة Databricks Workflow رقم `815854574980923`.
2. **[`dag_02_batch_reference.py`](file:///home/syd/Downloads/archive/airflow/dags/dag_02_batch_reference.py):**
   * **الجدول:** يومياً في تمام منتصف الليل UTC (`0 0 * * *`).
   * **الدور:** يستخرج لقطات الجداول المرجعية (MCC والمدن) مباشرة من RDS Postgres ويرفعها Snappy Parquet إلى S3 مع فحص وجود الملفات وسلامة أحجامها.

---

## 4. مجلد الحاويات والتكامل: `docker/`

* **`docker/kafka-connect/Dockerfile`:** يبني صورة مخصصة تستند إلى `confluentinc/cp-kafka-connect:7.6.0` لتحميل إضافات Debezium CDC و Aiven S3 Sink Connector.
* **`docker/kafka-connect/register_connectors.sh`:** سكريبت تشغيلي شديد الأهمية؛ ينتظر حتى يفتح المنفذ `8083` لـ Kafka Connect REST API، ثم يسجل إعدادات موصل Debezium وموصل S3 Sink بطريقة Idempotent (`PUT /connectors/{name}/config`).

---

## 5. مجلد النماذج الرياضية: `models/`

* **`models/v2/` (المجلد النشط إنتاجياً):**
  * `fraud_lgbm_model.joblib`: الموديل المعتمد لتقييم الاحتيال اللحظي والمبني بمحرك LightGBM.
  * `model_metadata.json`: يحتوي على التوثيق الرياضي الدقيق: تاريخ التدريب، قائمة الـ 34 خاصية بالترتيب الصارم، العتبة المثلى (`0.0453`)، ونتائج مصفوفة الارتباك (Recall 81.18%, ROC-AUC 0.8746).
* **الملفات في جذر `models/`:** تمثل الإصدار الأولي v1، وتُحفظ كمرجع مقارنة تاريخي (Historical Baseline).

---

## 6. مجلد السكريبتات التشغيلية: `scripts/`

يضم سكريبتات العمليات، الفحوصات الشاملة، والتحقق:
* **`run_end_to_end_orchestration.py`:** يشغل دورة كاملة تحاكي خط سير Airflow ويفحص جاهزية الـ API ويشغل مهمة Databricks.
* **`run_functional_pipeline_test.py`:** سكريبت شامل يختبر كافة المسارات الثلاثة، يتحقق من كتابة Debezium في كافكا، ويؤكد هبوط ملفات S3.
* **`run_cloud_stress_test.py`:** يضخ آلاف المعاملات لاختبار تحمل المنصة على السحابة وقياس الـ Latency.
* **`verify_realtime_scoring.py`:** يختبر سرعة استنتاج موديل الاحتيال اللحظي ويقيس أزمنة القراءة من Redis.
* **`manage_eventbridge_schedules.py`:** إدارة ومراقبة مهام الجدولة السحابية على Amazon EventBridge.
* **`audit_phase2_cost_model.py`:** حساب وتحليل تكاليف البنية التحتية والموارد السحابية.

---

## 7. مجلد الاختبارات: `tests/`

مقسم إلى ثلاثة مستويات:
* **`tests/unit/` (22 ملف اختبار):** يختبر كل وحدة برمجية باستقلالية تامة (المولدات، بوابات الجودة، الـ API، حساب المسافات، خوارزميات التوقيت، متجر الخصائص).
* **`tests/integration/` (ملفان):** يختبر الترابط الحي مع وسطاء الرسائل (`test_kafka_integration.py`) ودورة الحياة الشاملة للمنصة (`test_pipeline_e2e.py`).
* **`tests/load/` (ملف واحد):** يختبر ثبات معدل الـ TPS تحت الضغط وعدم حدوث انهيار في الذاكرة.

---

## 8. حالة الملفات والتبعيات (Status & Dependencies Classification)

| المكون | الحالة التشغيلية | المكونات التي تعتمد عليه | المكونات التي يعتمد عليها |
| :--- | :--- | :--- | :--- |
| `src/synthetic_streaming/` | **Active / Production** | الـ API، الـ Dockerfile، واختبارات الـ Integration | PostgreSQL, Kafka, Pydantic |
| `src/ingestion/` | **Active / Production** | Airflow DAGs, EventBridge | S3, DynamoDB, PostgreSQL, PyArrow |
| `src/ml/` | **Active / Production** | `Dockerfile.fraud-consumer` | Kafka, Redis, LightGBM |
| `airflow/dags/` | **Active / Production** | مهندسو البيانات وعمليات الجدولة اليومية | `src/ingestion/`, Databricks API |
| `models/v2/` | **Active / Production** | `src/ml/fraud_consumer.py` | LightGBM Runtime, OpenMP (`libgomp1`) |
| `generator.db` | **Local / Scratch** | المطورون محلياً للاختبارات السريعة | SQLite Engine |
| `data/` | **Static / Baseline** | `BaselineLoader`, تهيئة البيئة | ملفات CSV ثابتة |
