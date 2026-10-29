# توثيق واجهة برمجة التطبيقات التشغيلية (Generator REST API)
## الدليل المرجعي الكامل لنقاط FastAPI، الفحوصات السحابية، والتحكم التشغيلي

---

> [!NOTE]
> **مسار الملف:** `docs/services/generator-api.md`  
> **الملف المصدري الأساسي:** [`src/synthetic_streaming/api.py`](file:///home/syd/Downloads/archive/src/synthetic_streaming/api.py)  
> **المنفذ الافتراضي:** `8000` (FastAPI / Uvicorn)

---

## 1. نظرة عامة ونظام المصادقة (Authentication & Authorization)

توفر واجهة برمجة التطبيقات (Transaction & Generator API Plane) ثلاث وظائف حيوية:
1. **فحوصات الجاهزية السحابية (Liveness & Readiness Probes):** تستخدمها مجموعات ECS Fargate و Kubernetes و Airflow للتحقق من سلامة الخدمة.
2. **التحكم التشغيلي بالبث اللحظي (Operational Control):** تشغيل وإيقاف المولد وتعديل معدل الـ TPS أثناء التشغيل دون إعادة تشغيل الحاوية.
3. **سحب المعاملات التزايدي (Incremental Querying):** توفير استعلامات Keyset Pagination لخطوط المعالجة الدفعية.

### آلية المصادقة (Security Token Verification)
النقاط الإدارية الحساسة محمية بـ Dependency المسمى `verify_auth`:
* يقبل الرمز إما عبر الترويسة `X-API-Key: <token>` أو عبر ترويسة Bearer: `Authorization: Bearer <token>`.
* يتم التحقق بمطابقة الرمز مع المتغير `cfg.api_key` (الافتراضي في التطوير: `dev-api-key`).
* في حالة عدم إرسال الرمز: `401 Unauthorized`.
* في حالة إرسال رمز خاطئ: `403 Forbidden`.

---

## 2. جدول المرجع السريع لكافة الـ Endpoints (API Reference Table)

| المسار (Path) | الطريقة (Method) | المصادقة | التصنيف | الوصف السريع |
| :--- | :--- | :--- | :--- | :--- |
| `/health` | `GET` | غير مطلوبة | Operational | فحص حيوية التطبيق وقاعدة البيانات (Liveness Probe) |
| `/ready` | `GET` | غير مطلوبة | Operational | فحص جاهزية الاتصال بكافكا والداتابيز (Readiness Probe) |
| `/api/v1/generator/status` | `GET` | غير مطلوبة | Generator Control | عرض حالة المولد، معدل الـ TPS الحقيقي، وعدد المعاملات |
| `/api/v1/generator/start` | `POST` | `X-API-Key` | Generator Control | بدء خيط التوليد والبث اللحظي المستمر في الخلفية |
| `/api/v1/generator/stop` | `POST` | `X-API-Key` | Generator Control | إيقاف التوليد وتفريغ الذاكرة المؤقتة (Flush) للناشر |
| `/api/v1/generator/config` | `GET` | غير مطلوبة | Configuration | استعراض إعدادات المولد مع إخفاء وتشفير الأسرار |
| `/api/v1/generator/config` | `PATCH` | `X-API-Key` | Configuration | تعديل معدل الـ TPS ونسبة المستخدمين الجدد Dynamic |
| `/api/v1/generator/generate`| `POST` | `X-API-Key` | Generator Control | توليد وحفظ دفعة متزامنة من N معاملة في PostgreSQL |
| `/api/v1/transactions` | `GET` | غير مطلوبة | Historical DB | استعلام وسحب المعاملات التزايدي بنظام Keyset Pagination |
| `/api/v1/transactions/{id}` | `GET` | غير مطلوبة | Historical DB | استرجاع معاملة تاريخية واحدة عبر معرفها |
| `/api/v1/reference/cities` | `GET` | غير مطلوبة | Reference Data | استعلام والبحث في جدول مرجعيات المدن والإحداثيات |

---

## 3. التوثيق التفصيلي لكل Endpoint مع أمثلة `curl`

### 3.1 فحص الجاهزية التشغيلية: `GET /ready`
تستخدمه مهام Airflow و ECS للتأكد من أن الخدمة جاهزة لتلقي ومعالجة البيانات قبل توجيه الحركة إليها.
* **المدخلات:** لا توجد.
* **حالات الرد:**
  * `200 OK`: قاعدة البيانات متصلة وناشر كافكا يعمل وجاهز.
  * `503 Service Unavailable`: في حالة فشل الاتصال بالداتابيز أو وسيط كافكا.
* **مثال الطلب والرد:**
```bash
curl -s http://localhost:8000/ready | jq .
```
```json
{
  "status": "ready",
  "database": {
    "status": "healthy",
    "engine": "postgresql",
    "latency_ms": 3.42
  },
  "publisher": {
    "status": "healthy",
    "type": "KafkaEventPublisher",
    "buffered_messages": 0
  },
  "generator_state": "RUNNING"
}
```

---

### 3.2 فحص حالة وإحصائيات المولد: `GET /api/v1/generator/status`
يعرض مؤشرات الأداء اللحظية وعدد الرسائل التي تم تأكيد وصولها.
```bash
curl -s http://localhost:8000/api/v1/generator/status | jq .
```
```json
{
  "state": "RUNNING",
  "target_tps": 10.0,
  "actual_tps": 9.98,
  "worker_id": 0,
  "run_id": "run-0-1761765600",
  "total_tx_generated": 14250,
  "total_prereq_events_generated": 1425,
  "total_tx_delivered": 14248,
  "total_tx_failed": 0,
  "uptime_seconds": 1427.5
}
```

---

### 3.3 تعديل الإعدادات أثناء التشغيل: `PATCH /api/v1/generator/config`
يسمح برفع أو خفض سرعة التوليد فورياً دون توقف.
* **جسم الطلب (Request Body):**
```json
{
  "target_tps": 50.0,
  "new_user_transaction_ratio": 0.15
}
```
* **أمر التنفيذ:**
```bash
curl -X PATCH http://localhost:8000/api/v1/generator/config \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-api-key" \
  -d '{"target_tps": 50.0, "new_user_transaction_ratio": 0.15}' | jq .
```
* **الرد (`200 OK`):**
```json
{
  "message": "Configuration updated successfully",
  "updated": {
    "target_tps": 50.0,
    "new_user_transaction_ratio": 0.15
  }
}
```

---

### 3.4 بدء وإيقاف التوليد المستمر: `/start` و `/stop`
```bash
# تشغيل المولد
curl -X POST http://localhost:8000/api/v1/generator/start \
  -H "X-API-Key: dev-api-key" | jq .

# إيقاف المولد وتفريغ الناشر
curl -X POST http://localhost:8000/api/v1/generator/stop \
  -H "X-API-Key: dev-api-key" | jq .
```

---

### 3.5 الاستعلام التزايدي عن المعاملات: `GET /api/v1/transactions`
تعتمد عليه خطوط الاستخراج التزايدي في Airflow (`dag_01_incremental_ingestion.py`).
* **معايير الاستعلام (Query Parameters):**
  * `since_id` (int): يرجع السجلات ذات الـ `id > since_id` (علامة الإزاحة).
  * `limit` (int, 1..1000): الحد الأقصى للصفحة (الافتراضي: 50).
  * `order` (str): يجب أن يكون `asc` عند استخدام `since_id` للحفاظ على الترتيب الرتيب.
  * `is_fraud` (str): تصفية حسب حالة الاحتيال (`Yes` أو `No`).
* **أمر التنفيذ:**
```bash
curl -s "http://localhost:8000/api/v1/transactions?since_id=1000&limit=2&order=asc" | jq .
```
* **نموذج الرد:**
```json
{
  "items": [
    {
      "id": 1001,
      "transaction_id": "tx-1001-abc",
      "event_id": "01928374-1234-7abc-9def-0123456789ab",
      "event_type": "transaction.created",
      "user_id": 42,
      "card_index": 0,
      "amount": "125.50",
      "amount_raw": "$125.50",
      "use_chip": "Chip Transaction",
      "transaction_entry_method": "chip",
      "merchant_name": "4892104921",
      "merchant_city": "Springfield",
      "merchant_state": "IL",
      "merchant_latitude": 39.7817,
      "merchant_longitude": -89.6501,
      "zip": "62701",
      "mcc": 5411,
      "errors": null,
      "is_fraud": "No",
      "scenario": "normal_spend",
      "source_system": "pos_terminal",
      "run_id": "run-0-1761765600",
      "created_at": "2026-10-29T18:00:00+00:00"
    }
  ],
  "limit": 2,
  "count": 1,
  "has_more": false,
  "next_cursor": null,
  "max_id": 1001,
  "pagination": {
    "limit": 2,
    "count": 1,
    "has_more": false,
    "next_cursor": null,
    "max_id": 1001
  }
}
```

---

## 4. الفروقات الجوهرية بين بيانات الـ API وبيانات الـ Streaming

> [!IMPORTANT]
> **تنبيه معماري حاسم:**  
> المعاملات الحية المتدفقة من المولد لا تُكتب في جدول `transactions` بقاعدة البيانات (مبدأ **Zero-DB-Inserts**)، ولذلك لا تظهر عند استدعاء `GET /api/v1/transactions`.  
> هذا الـ Endpoint مخصص حصرياً لقراءة **البيانات التاريخية التأسيسية (Historical Baseline)** أو الدفعات التي يطلب المستخدم توليدها يدوياً وتخزينها في قاعدة البيانات عبر `POST /api/v1/generator/generate`.  
> للحصول على المعاملات اللحظية الحية، يجب استهلاك موضوع كافكا `transactions` مباشرة أو قراءة ملفات `bronze/transactions/` من بحيرة البيانات على S3.
