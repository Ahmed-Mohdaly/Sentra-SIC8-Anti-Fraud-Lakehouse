# مواصفات مواضيع كافكا وهياكل البيانات (Kafka Topics & Schemas)
## الدليل المرجعي لمواضيع كافكا، عقود البيانات (Data Contracts)، وغلاف الأحداث

---

> [!NOTE]
> **مسار الملف:** `docs/streaming/kafka-topics-and-schemas.md`  
> **الملفات المصدرية المرتبطة:**  
> - [`src/synthetic_streaming/event_schema.py`](file:///home/syd/Downloads/archive/src/synthetic_streaming/event_schema.py)
> - [`src/synthetic_streaming/config.py`](file:///home/syd/Downloads/archive/src/synthetic_streaming/config.py)
> - [`docker/kafka-connect/register_connectors.sh`](file:///home/syd/Downloads/archive/docker/kafka-connect/register_connectors.sh)

---

## 1. الفهرس الشامل للمواضيع في Amazon MSK (Topics Catalog)

يحتوي العنقود السحابي على ستة مواضيع أساسية (5 تشغيلية + 1 DLQ لموصل كافكا كونكت):

```text
┌───────────────────┬────────────┬────────────────────┬───────────────────────┬───────────────────────────────┐
│ اسم الموضوع       │ البارتشنز  │ مفتاح التوزيع      │ المُنتج (Producer)    │ المُستهلك (Consumer)          │
├───────────────────┼────────────┼────────────────────┼───────────────────────┼───────────────────────────────┤
│ transactions      │ 3          │ user_id (String)   │ GeneratorService      │ Aiven S3 Sink, Fraud Consumer │
│ users             │ 3          │ user_id (String)   │ Debezium CDC          │ Aiven S3 Sink                 │
│ cards             │ 3          │ user_id (String)   │ Debezium CDC          │ Aiven S3 Sink                 │
│ fraud_alerts      │ 1          │ transaction_id     │ FraudStreamingConsumer│ Downstream Alerting / Ops     │
│ transaction_dlq   │ 1          │ event_id           │ Extractor / Validator │ Quarantine Storage / Audit    │
│ sic_sink_dlq      │ 1          │ Original Key       │ Aiven S3 Sink         │ S3 Dead Letter Queue          │
└───────────────────┴────────────┴────────────────────┴───────────────────────┴───────────────────────────────┘
```

---

## 2. تفصيل كل موضوع وعقد البيانات المعتمد (Data Contracts)

### 2.1 موضوع المعاملات المالية: `transactions`
* **المسؤولية:** القناة الرئيسية لتدفق كافة المعاملات المالية اللحظية المولدة.
* **البارتشنز:** 3 بارتشنز، عامل التكرار: 2.
* **مفتاح الرسالة (Key):** `user_id` كنص (e.g. `"1042"`).
* **التسلسل (Serialization):** UTF-8 JSON مضغوط خفيفاً في الذاكرة عبر غلاف [`EventEnvelope`](file:///home/syd/Downloads/archive/src/synthetic_streaming/event_schema.py#L137-L155).
* **مدة الاحتفاظ (Retention):** 7 أيام (أو 10GB لكل بارتشن).
* **مثال رسالة حقيقية (Sample Payload):**

```json
{
  "event_id": "7b8d4f12-9c3a-4e2b-8a1f-0e2d3c4b5a6f",
  "event_type": "transaction.created",
  "schema_version": 1,
  "event_timestamp": "2026-10-29T19:25:00.104250+00:00",
  "source_system": "pos_terminal",
  "run_id": "run-0-1761765600",
  "scenario": "normal_spend",
  "payload": {
    "User": 1042,
    "Card": 0,
    "Year": 2026,
    "Month": 10,
    "Day": 29,
    "Time": "19:25",
    "Amount": "$34.50",
    "Use Chip": "Chip Transaction",
    "Merchant Name": 458921389012,
    "Merchant City": "Springfield",
    "Merchant State": "IL",
    "Zip": "62701",
    "MCC": 5411,
    "Errors?": null,
    "Is Fraud?": "No",
    "Transaction Entry Method": "chip",
    "Merchant Latitude": 39.7817,
    "Merchant Longitude": -89.6501
  }
}
```

---

### 2.2 موضوع مستخدمي الـ CDC: `users`
* **المسؤولية:** استقبال سجلات إضافة وتعديل المستخدمين الجدد من سجلات الـ WAL بقاعدة بيانات PostgreSQL.
* **المُنتج:** Debezium PostgreSQL Source Connector عبر أداة التحويل المنطقي `ByLogicalTableRouter`.
* **البارتشنز:** 3 بارتشنز.
* **مفتاح الرسالة:** المعرف الرقمي للعميل (`user_id`).
* **مثال رسالة حقيقية (Debezium Change Event):**

```json
{
  "before": null,
  "after": {
    "user_id": 1042,
    "person": "Jane Smith",
    "current_age": 38,
    "retirement_age": 65,
    "birth_year": 1988,
    "birth_month": 5,
    "gender": "Female",
    "address": "123 Elm Street",
    "apartment": "4B",
    "city": "Springfield",
    "state": "IL",
    "zipcode": 62701,
    "latitude": 39.7817,
    "longitude": -89.6501,
    "per_capita_income": "$32000",
    "yearly_income": "$68000",
    "total_debt": "$12500",
    "fico_score": 745,
    "num_credit_cards": 2,
    "created_at": 1761765599000
  },
  "source": {
    "version": "2.5.4.Final",
    "connector": "postgresql",
    "name": "sic",
    "ts_ms": 1761765599100,
    "db": "postgres",
    "schema": "public",
    "table": "users",
    "txId": 584931,
    "lsn": 249019800
  },
  "op": "c",
  "ts_ms": 1761765599200
}
```

---

### 2.3 موضوع بطاقات الـ CDC: `cards`
* **المسؤولية:** استقبال أحداث إصدار وتحديث بطاقات الائتمان والدفع.
* **المُنتج:** Debezium PostgreSQL Source Connector.
* **البارتشنز:** 3 بارتشنز.
* **مفتاح الرسالة:** `user_id`.
* **مثال رسالة حقيقية (Card CDC Payload):**

```json
{
  "before": null,
  "after": {
    "user_id": 1042,
    "card_index": 0,
    "card_brand": "Visa",
    "card_type": "Debit",
    "card_number": "4000123456789010",
    "expires": "12/2029",
    "cvv": "452",
    "has_chip": "YES",
    "cards_issued": 1,
    "credit_limit": "$5000",
    "acct_open_date": "05/2018",
    "year_pin_last_changed": 2021,
    "card_on_dark_web": "No",
    "created_at": 1761765599050
  },
  "op": "c",
  "ts_ms": 1761765599250
}
```

---

### 2.4 موضوع تنبيهات الاحتيال اللحظية: `fraud_alerts`
* **المسؤولية:** استقبال التنبيهات الفورية الناتجة عن موديل كشف الاحتيال LightGBM عندما يتجاوز احتمال الاحتيال العتبة المعتمدة (`0.0453`).
* **المُنتج:** كلاس [`FraudStreamingConsumer`](file:///home/syd/Downloads/archive/src/ml/fraud_consumer.py#L350-L400).
* **البارتشنز:** 1 بارتشن (للتنبيهات السريعة المركزة).
* **مفتاح الرسالة:** معرف المعاملة الفريد `transaction_id`.
* **مثال رسالة حقيقية (Enriched Fraud Alert Payload):**

```json
{
  "alert_id": "alt-01928374-9999-7abc-bdef-fedcba987654",
  "alert_timestamp": "2026-10-29T19:25:00.654321+00:00",
  "transaction_id": "tx-1042-high-risk-99",
  "user_id": 1042,
  "card_index": 0,
  "amount": 3450.00,
  "fraud_probability": 0.8942,
  "decision_threshold": 0.0453,
  "decision": "HIGH_RISK_BLOCK",
  "latency_inference_ms": 0.52,
  "explainability_reasons": [
    "Distance to home exceeds 4,500 km (Haversine)",
    "Calculated travel speed 940 km/h (Physically impossible velocity)",
    "Amount is 12x higher than user's 30-day moving average",
    "Transaction occurred during unusual nocturnal hours"
  ],
  "feature_snapshot": {
    "distance_to_home_km": 4512.4,
    "travel_speed_kmh": 940.2,
    "spend_deviation_ratio": 12.4,
    "tx_count_last_1h": 4.0
  }
}
```

---

### 2.5 مواضيع طوابير المعاملات المعزولة (DLQ Topics)

#### أ. موضوع التطبيق البرمجي: `transaction_dlq`
* **المسؤولية:** استقبال المعاملات التي تفشل في اجتياز اختبارات بوابات جودة البيانات بالذاكرة (مثل وجود مبالغ سالبة شاذة أو أكواد MCC مجهولة).
* **المفتاح:** `event_id`.

#### ب. موضوع موصل التخزين: `sic_sink_dlq`
* **المسؤولية:** معرّف داخل موصل Aiven S3 Sink (`errors.deadletterqueue.topic.name = "sic_sink_dlq"`).
* **الدور:** إذا واجه موصل كافكا كونكت رسالة تالفة بتنسيق غير صالح لا يمكن تحويله لـ JSONL أو تجاوزت أخطاء الـ Serialization، يتم تحويلها تلقائياً لهذا الموضوع مع إرفاق ترويسات سياق الخطأ (`context.headers.enable = true`).
