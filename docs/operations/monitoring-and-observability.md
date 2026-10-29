# المراقبة والمقاييس التشغيلية (Monitoring & Observability)
## الدليل التشغيلي للمراقبة السحابية، السجلات الهيكلية، ومؤشرات الأداء (KPIs)

---

> [!NOTE]
> **مسار الملف:** `docs/operations/monitoring-and-observability.md`  
> **الملفات المصدرية المرتبطة:**  
> - [`src/ingestion/extractor.py`](file:///home/syd/Downloads/archive/src/ingestion/extractor.py) (كلاس `JSONFormatter`)
> - [`src/synthetic_streaming/api.py`](file:///home/syd/Downloads/archive/src/synthetic_streaming/api.py)
> - [`scripts/verify_realtime_scoring.py`](file:///home/syd/Downloads/archive/scripts/verify_realtime_scoring.py)
> - [`scripts/run_cloud_stress_test.py`](file:///home/syd/Downloads/archive/scripts/run_cloud_stress_test.py)

---

## 1. منظومة السجلات الهيكلية (Structured JSON Logging)

تعتمد المنصة بالكامل على تسجيل الأحداث بصيغة **JSON قياسية بدون أسرار (No Credentials)** عبر كلاس `JSONFormatter`:

```json
{
  "timestamp": "2026-10-29T19:35:00.123456+00:00",
  "level": "INFO",
  "logger": "ingestion_extractor",
  "message": "Starting incremental extraction cycle",
  "run_id": "extract-1761765600-a1b2c3",
  "watermark_before": 14200,
  "batch_limit": 500,
  "status": "STARTED"
}
```

### مزايا السجلات الهيكلية:
1. **سهولة التحليل والفلترة:** يمكن لمحركات السجلات مثل **Amazon CloudWatch Logs** أو Datadog فلترة الحقول الرقمية (مثل `watermark_before` و `records_written`) لإنشاء رسوم بيانية وتنبيهات فورية.
2. **تتبع خط سير الحدث (Correlation Tracing):** يحمل كل حدث معرف `run_id` و `event_id` يتيح تتبع المعاملة عبر كافة المكونات (من المولد إلى كافكا إلى S3 إلى Databricks).

---

## 2. نقاط الفحص السحابية الحية (Health & Liveness Probes)

توفر واجهة برمجة التطبيقات نقطتين رئيسيتين للفحص:
* **`GET /health` (Liveness Probe):**
  * يختبر اتصال قاعدة بيانات PostgreSQL وحالة خيط التوليد الداخلي.
  * لو كان هناك عطل في اتصال الداتابيز، يرجع فوراً بـ `HTTP 503`، مما يدفع خدمة AWS ECS لإعادة تشغيل الحاوية تلقائياً.
* **`GET /ready` (Readiness Probe):**
  * يفحص اتصال قاعدة البيانات **وجاهزية ناشر كافكا**.
  * تستخدمه مهام Airflow و Load Balancers للتأكد من أن الخدمة قادرة على استقبال الطلبات بنجاح.

---

## 3. أهم المقاييس التشغيلية الحية (Key Performance Indicators)

```text
┌──────────────────────────────┬─────────────────────────┬───────────────────┬──────────────────────────────────────┐
│ المؤشر التشغيلي (KPI)        │ المصدر / الأداة         │ القيمة المقاسة    │ الحالة المقبولة (Threshold SLA)      │
├──────────────────────────────┼─────────────────────────┼───────────────────┼──────────────────────────────────────┤
│ معدل التوليد الفعلي          │ GET /generator/status   │ 9.98 – 10.02 TPS  │ تطابق مع Target TPS بنسبة ±2%        │
│ زمن قراءة خصائص Redis        │ verify_realtime_scoring │ 1.11 ms           │ أقل من 3.0 ms                        │
│ زمن استنتاج LightGBM         │ verify_realtime_scoring │ 0.52 ms           │ أقل من 1.0 ms                        │
│ زمن دورة فحص الاحتيال الكلية │ verify_realtime_scoring │ 6.26 – 8.15 ms    │ أقل من 10.0 ms (SLA حاسم)            │
│ تراكم استهلاك كافكا (Lag)    │ Kafka Consumer Group    │ 0 – 50 رسائل      │ أقل من 500 رسالة                     │
│ حجم المعاملات المعزولة (DLQ) │ S3 bronze/quarantine/   │ 0 ملفات (طبيعي)   │ إطلاق تنبيه فوري إذا وجد أي ملف      │
│ زمن استخراج Airflow التزايدي │ Airflow Task Logs       │ 1.2 – 2.5 ثوانٍ   │ أقل من 30 ثانية لكل دفعة             │
└──────────────────────────────┴─────────────────────────┴───────────────────┴──────────────────────────────────────┘
```

---

## 4. مراقبة موصلات كافكا كونكت عبر REST API

يمكن التحقق من سلامة موصلي Debezium و S3 Sink عبر أوامر `curl` المباشرة على المنفذ الداخلي `8083`:

```bash
# 1. استعراض حالة موصل Debezium CDC
curl -s http://172.31.10.236:8083/connectors/debezium-postgres-source/status | jq .

# نموذج الإخراج السليم:
# {
#   "name": "debezium-postgres-source",
#   "connector": { "state": "RUNNING", "worker_id": "172.31.10.236:8083" },
#   "tasks": [ { "id": 0, "state": "RUNNING", "worker_id": "172.31.10.236:8083" } ]
# }

# 2. استعراض حالة موصل Aiven S3 Sink
curl -s http://172.31.10.236:8083/connectors/aiven-s3-sink/status | jq .
```
إذا كانت حالة الـ Task هي `FAILED`، يُظهر الرد نص الخطأ الاستثنائي الكامل (Traceback) الذي يوضح سبب الفشل بدقة.

---

## 5. التوصيات المستقبلية لتوسيع المراقبة (Future Observability Recommendations)

تعتمد المنصة حالياً على الفحوصات المباشرة والسجلات؛ وللتطوير المستقبلي في بيئات الإنتاج الكبرى يوصى بما يلي:
1. **تصدير مقاييس Prometheus / Grafana:** تفعيل `prometheus_fastapi_instrumentator` في خدمة الـ API لتصدير معدلات الـ TPS والـ Latency في صورة رسوم بيانية حية.
2. **تنبيهات Amazon CloudWatch Alarms:**
   * تنبيه عند تجاوز الـ Consumer Lag في موضوع `transactions` لأكثر من 1,000 رسالة لمدة دقيقتين.
   * تنبيه عند وصول أي كائن جديد إلى المجلد `bronze/quarantine/` على S3.
   * تنبيه عند تراجع المساحة المتبقية لقرص RDS بسبب تراكم سجلات الـ WAL.
