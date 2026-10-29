# التطوير والتشغيل المحلي (Local Development Guide)
## دليل تشغيل المنظومة واختبارها محلياً باستخدام Docker Compose

---

> [!NOTE]
> **مسار الملف:** `docs/getting-started/local-development.md`  
> **الملف المصدري الأساسي:** [`compose.yaml`](file:///home/syd/Downloads/archive/compose.yaml)  
> **البيئة المحلية:** Docker Compose v2 (PostgreSQL 16, Redis 7, FastAPI Generator)

---

## 1. المعمارية المحلية مقابل المعمارية السحابية (Local vs Cloud Scope)

لتسهيل التطوير السريع على أجهزة المهندسين دون الحاجة لحساب AWS أو دفع تكاليف سحابية، تم توفير بيئة محلية متكاملة عبر Docker Compose:

```text
┌──────────────────────────────┬──────────────────────────────┬──────────────────────────────────────────┐
│ الخدمة / المكون              │ البيئة المحلية (Docker)      │ البيئة السحابية (AWS Production)         │
├──────────────────────────────┼──────────────────────────────┼──────────────────────────────────────────┤
│ خدمة المولد والـ API         │ synthetic-generator-api :8000│ AWS ECS Fargate (Task Definition :2)     │
│ قاعدة البيانات التشغيلية     │ synthetic-db (PostgreSQL 16) │ Amazon RDS PostgreSQL 18.3 Multi-AZ      │
│ متجر الخصائص اللحظي          │ synthetic-redis (Redis 7)    │ AWS ElastiCache for Redis (TLS & KMS)    │
│ وسيط الرسائل (Streaming)     │ MockPublisher بالذاكرة أو كافكا│ Amazon MSK Cluster (Apache Kafka 3.x)    │
│ بحيرة البيانات (Storage)     │ مجلد data/ أو LocalStack     │ Amazon S3 Bucket: sic-pipeline-storage   │
│ جدولة العمليات (Orchestration)│ سكريبتات بايثون المحلية      │ Amazon EventBridge + Apache Airflow      │
└──────────────────────────────┴──────────────────────────────┴──────────────────────────────────────────┘
```

---

## 2. خطوات التشغيل المحلي خطوة بخطوة (Local Startup Sequence)

### 2.1 إطلاق الحاويات عبر Docker Compose
يحدد ملف [`compose.yaml`](file:///home/syd/Downloads/archive/compose.yaml) تبعيات الإقلاع بحيث تنتظر خدمة الـ API فحص سلامة قاعدة البيانات (`service_healthy`):

```bash
# 1. إطلاق الحاويات في الخلفية
docker compose up -d

# 2. متابعة السجلات اللحظية للتأكد من اكتمال الإقلاع
docker compose logs -f
```

### 2.2 التأكد من حالة الحاويات
```bash
docker compose ps
```
يجب أن تظهر الحاويات الثلاث في حالة `healthy`:
* `synthetic-db`: على المنفذ `5432:5432`
* `synthetic-generator-api`: على المنفذ `8000:8000`
* `synthetic-redis`: على المنفذ `6379:6379`

---

## 3. اختبارات الدخان والتحقق السريع (Smoke Tests)

بمجرد تشغيل الحاويات، نفذ الفحوصات السريعة التالية للتأكد من اكتمال الجاهزية:

### 3.1 فحص جاهزية واجهة برمجة التطبيقات:
```bash
curl -s http://localhost:8000/ready | jq .
```
الرد المتوقع:
```json
{
  "status": "ready",
  "database": {
    "status": "healthy",
    "engine": "postgresql"
  }
}
```

### 3.2 فحص اتصال Redis:
```bash
docker compose exec redis redis-cli ping
```
الرد المتوقع: `PONG`.

### 3.3 استعلام جداول قاعدة البيانات المحلية:
```bash
docker compose exec db psql -U postgres -d synthetic_db -c "\dt"
```
يجب أن تظهر الجداول الخمسة: `users`, `cards`, `merchant_category_codes`, `city_reference`, `transactions`.

---

## 4. التحكم في المولد واختبار استخراج البيانات محلياً

```bash
# 1. استعراض حالة المولد
curl -s http://localhost:8000/api/v1/generator/status | jq .

# 2. توليد دفعة تجريبية مكونة من 10 معاملات وحفظها في PostgreSQL
curl -X POST http://localhost:8000/api/v1/generator/generate \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-api-key" \
  -d '{"count": 10, "run_id": "local-test-01"}' | jq .

# 3. سحب المعاملات التوليدية عبر Keyset Pagination
curl -s "http://localhost:8000/api/v1/transactions?limit=5&order=asc" | jq .

# 4. تشغيل اختبارات الوحدة محلياً للتأكد من سلامة الكود
pytest tests/unit/ -v
```

---

## 5. الإيقاف النظيف للبيئة المحلية (Teardown)

عند الانتهاء من العمل أو الرغبة في تصفير البيانات:

```bash
# إيقاف الحاويات مع الحفاظ على البيانات
docker compose down

# إيقاف الحاويات ومسح مجلدات التخزين (Volumes) للبدء من الصفر
docker compose down -v
```
