# المتطلبات الأساسية وإعداد البيئة (Prerequisites & Setup)
## دليل تجهيز بيئة العمل المحلية والسحابية لتشغيل منصة SIC

---

> [!NOTE]
> **مسار الملف:** `docs/getting-started/prerequisites-and-setup.md`  
> **الملفات المصدرية المرتبطة:**  
> - [`pyproject.toml`](file:///home/syd/Downloads/archive/pyproject.toml)
> - [`.env.example`](file:///home/syd/Downloads/archive/.env.example)
> - [`requirements-fraud-consumer.txt`](file:///home/syd/Downloads/archive/requirements-fraud-consumer.txt)

---

## 1. المتطلبات البرمجية الأساسية (System Requirements)

لتشغيل المنظومة وتطويرها محلياً أو على السحابة، يجب توفر الأدوات التالية:
1. **Python 3.11+:** الإصدار المعتمد لكافة الحزم والحاويات البرمجية في المشروع (`python:3.11-slim-bookworm`).
2. **Docker & Docker Compose v2.20+:** لتشغيل قواعد البيانات والخدمات المحلية.
3. **AWS CLI v2:** لإدارة الخدمات السحابية والاتصال بـ MSK و S3 و RDS.
4. **أدوات سطر الأوامر المساعدة:** `curl`, `jq`, `netcat` (`nc`).

---

## 2. إعداد بيئة بايثون الافتراضية وتثبيت الحزم (Python Environment)

يُوصى دائماً بإنشاء بيئة افتراضية معزولة:

```bash
# 1. الانتقال إلى مجلد المشروع
cd /home/syd/Downloads/archive

# 2. إنشاء بيئة بايثون افتراضية
python3.11 -m venv .venv

# 3. تفعيل البيئة الافتراضية
source .venv/bin/activate

# 4. ترقية pip وتثبيت الحزم الأساسية بصيغة قابلة للتعديل (Editable Mode)
pip install --upgrade pip setuptools wheel
pip install -e ".[dev]"

# 5. تثبيت حزم مسار الاحتيال الإضافية وموديل LightGBM
pip install -r requirements-fraud-consumer.txt
```

---

## 3. إعداد متغيرات البيئة والأسرار (`.env`)

يحتوي المستودع على ملف إعداد نموذجي [`.env.example`](file:///home/syd/Downloads/archive/.env.example). قم بنسخه لإنشاء ملف `.env` المحلي الخاص بك:

```bash
cp .env.example .env
```

### شرح أهم متغيرات البيئة:
```bash
# بيئة التشغيل
APP_ENV=development
LOG_LEVEL=INFO

# الاتصال بقاعدة بيانات PostgreSQL
DATABASE_URL=postgresql://postgres:postgrespassword@localhost:5432/synthetic_db

# إعدادات واجهة الـ API
API_HOST=0.0.0.0
API_PORT=8000
API_KEY=dev-api-key

# إعدادات المولد التلقائي
GENERATOR_RATE=10.0
GENERATOR_BATCH_SIZE=50
AUTO_SEED=true
BASELINE_DATA_DIR=data

# إعدادات كافكا و Amazon MSK
ENABLE_KAFKA=true
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_SECURITY_PROTOCOL=PLAINTEXT
KAFKA_USERS_TOPIC=users
KAFKA_CARDS_TOPIC=cards
KAFKA_TRANSACTIONS_TOPIC=transactions
KAFKA_FRAUD_ALERTS_TOPIC=fraud_alerts
KAFKA_DLQ_TOPIC=transaction_dlq

# إعدادات متجر الخصائص اللحظي (Redis)
REDIS_URL=redis://localhost:6379

# إعدادات التخزين وبحيرة البيانات على AWS S3
S3_BUCKET=sic-pipeline-storage
AWS_REGION=eu-west-3
```

> [!CAUTION]
> لا تقم أبداً بدفع ملف `.env` الحقيقي أو أي مفاتيح اعتماد سحابية سرية (AWS Secret Access Keys) إلى مستودع الجيت؛ تأكد دائماً من وجود `.env` داخل ملف `.gitignore`.

---

## 4. التحقق من سلامة البيئة والتثبيت (Sanity Check)

لتأكيد اكتمال التثبيت وصحة الحزم، شغل الفحص السريع التالي:

```bash
# 1. التحقق من استيراد حزم المنظومة بنجاح
python -c "import synthetic_streaming, ingestion, ml; print('✅ Core modules imported successfully!')"

# 2. التحقق من توافر موديل LightGBM وملف البيانات الوصفية
python -c "import joblib, json; m = joblib.load('models/v2/fraud_lgbm_model.joblib'); meta = json.load(open('models/v2/model_metadata.json')); print(f'✅ LightGBM Model loaded! Version: {meta[\"model_version\"]}, Features: {len(meta[\"feature_names\"])}')"

# 3. تشغيل عينة من اختبارات الوحدة السريعة
pytest tests/unit/test_config.py tests/unit/test_rate_controller.py -v
```
إذا اجتازت هذه الأوامر بنجاح، فأنت جاهز تماماً للانتقال إلى مرحلة التشغيل والتطوير المحلي!
