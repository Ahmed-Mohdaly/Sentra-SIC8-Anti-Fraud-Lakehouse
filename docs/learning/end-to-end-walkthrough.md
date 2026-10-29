# رحلة تتبع معاملة واحدة من الألف إلى الياء (End-to-End Walkthrough)
## الدليل التعليمي المصور: تتبع حدث مالي حقيقي عبر كافة طبقات المنظومة

---

> [!NOTE]
> **مسار الملف:** `docs/learning/end-to-end-walkthrough.md`  
> **الهدف:** أخذ مهندس البيانات في جولة تفصيلية خطوة بخطوة تتبع مسار **معاملة مالية واحدة** (`event_id = "e4f8a2c1-..."`) منذ لحظة إنشائها في ذاكرة المولد وحتى استقرارها في جداول بحيرة البيانات وتقييمها في محرك كشف الاحتيال.

---

## 1. بطاقة تعريف المعاملة قيد التتبع

* **العميل:** أحمد عبد الله (`user_id = 1042`, العمر: 42 سنة, منزل الإقامة: Springfield, IL).
* **البطاقة المستخدمة:** `card_index = 0` (Visa Debit, حدها الائتماني: $5,000).
* **المعاملة الحالية:** شراء بقيمة **$1,450.00** عبر الإنترنت من متجر مجوهرات في باريس، فرنسا (`MCC = 5944`).
* **السيناريو المختار:** `unusual_location` مدمج بقيمة مرتفعة (`high_value`).

---

## 2. المحطة 1: التوليد في الذاكرة (Memory Generation)

1. **المنظم الزمني:** في خيط [`RateController`](file:///home/syd/Downloads/archive/src/synthetic_streaming/rate_controller.py)، يصل المؤقت إلى الخانة الزمنية المخصصة (`now >= next_slot`).
2. **محرك السيناريوهات:** يستدعي [`ScenarioEngine`](file:///home/syd/Downloads/archive/src/synthetic_streaming/scenario_engine.py) لتوليد مبلغ المعاملة ($1,450.00) وموقع التاجر (`Online / International`).
3. **صمام الأمان الحسابي:** يفحص [`RelationshipValidator`](file:///home/syd/Downloads/archive/src/synthetic_streaming/relationship_validator.py) أن المستخدم 1042 يملك البطاقة 0 بالفعل في الذاكرة.
4. **تغليف الحدث:** يُنشأ كائن [`EventEnvelope`](file:///home/syd/Downloads/archive/src/synthetic_streaming/event_schema.py#L137-L155):
   ```json
   {
     "event_id": "e4f8a2c1-7b9d-4e3a-8f1c-0a2b3c4d5e6f",
     "event_type": "transaction.created",
     "source_system": "ecommerce_gateway",
     "payload": {
       "User": 1042, "Card": 0, "Amount": "$1450.00",
       "Use Chip": "Online Transaction", "MCC": 5944
     }
   }
   ```

---

## 3. المحطة 2: النشر إلى شبكة كافكا (Amazon MSK Stream)

1. **تحديد البارتشن:** يُرسل `key = "1042"`. تطبق كافكا خوارزمية التجزئة:  
   `Murmur2Hash("1042") % 3 = Partition 1`.
2. **الكتابة المتينة:** يرسل الناشر الرسالة بـ `acks="all"`. يكتب وسيط كافكا Leader الرسالة وينسخها للوسيط الثاني (Follower) ويمنحها رقم الإزاحة: **`Offset = 18450`**.
3. **مبدأ الـ Zero-DB-Inserts:** تم نشر المعاملة دون لمس قاعدة بيانات PostgreSQL نهائياً!

---

## 4. المحطة 3: مسار التخزين في بحيرة البيانات (Aiven S3 Sink)

1. موصل كافكا كونكت يقرأ الرسالة من `Partition 1` عند الإزاحة `18450` ويضعها في مصفوفة التجميع.
2. عند اكتمال 50 رسالة (من إزاحة 18450 إلى 18499)، يُغلق الموصل الحزمة.
3. يتم ضغط السجلات بخوارزمية Gzip ورفع الملف إلى S3:  
   `s3://sic-pipeline-storage/bronze/transactions/partition=1/00000000000000018450.json.gz`.

---

## 5. المحطة 4: مسار كشف الاحتيال الفوري (Real-Time ML Pipeline)

بالتوازي وفي نفس اللحظة:
1. **استهلاك الحدث:** مستهلك الاحتيال [`FraudStreamingConsumer`](file:///home/syd/Downloads/archive/src/ml/fraud_consumer.py) يقرأ الرسالة من `transactions`.
2. **استرجاع الخصائص من Redis (في 1.11ms):**
   * جلب بروفايل العميل من `profile:user:1042`: متوسط إنفاقه التاريخي = $45.00، منزله في إلينوي.
   * جلب النوافذ السابقة من `tx_window:1042:0`: آخر معاملة كانت منذ 20 دقيقة في شيكاغو.
3. **حساب متجه الميزات الـ 34:**
   * `distance_to_home_km`: 6,650 كم (المسافة بين شيكاغو وباريس).
   * `travel_speed_kmh`: 19,950 كم/ساعة (سرعة مستحيلة فيزيائياً!).
   * `spend_deviation_ratio`: $1450 / 45 = 32.2$ ضعف متوسط الإنفاق!
4. **استنتاج الموديل الرياضي (في 0.52ms):**
   * يمرر المتجه لموديل LightGBM: النتيجة **احتمال الاحتيال = 0.94**.
   * بما أن $0.94 \ge 0.0453$ (العتبة المثلى)، تُصنف المعاملة فوراً كـ **HIGH RISK FRAUD**.
5. **إصدار التنبيه الفوري:**
   * يرسل المستهلك تنبيهاً ذكياً غنياً بالأسباب إلى موضوع `fraud_alerts`.
6. **تحديث الحالة ودرع الإزاحة:**
   * تحديث سجلات Redis بـ TTL 48 ساعة.
   * تثبيت إزاحة كافكا بنجاح (`commit`).

---

## 6. المحطة 5: المعالجة التحليلية في Databricks Lakehouse

1. **القراءة عبر AutoLoader:** تكتشف دفاتر ملاحظات Databricks الملف الجديد `00000000000000018450.json.gz` فور هبوطه في S3.
2. **فك الغلاف والتنظيف:** يتم استخراج الحقول، فك ضغط العملة إلى `1450.00 DECIMAL(12, 2)`، ودمجها مع كود المتجر `5944` من جدول المرجعيات `silver_mcc`.
3. **الكتابة في Delta Lake:** تُحفظ المعاملة في جدول **`silver_transactions`** مع تطبيق Z-Order على `user_id` لتكون جاهزة لاستعلامات محللي البيانات ولوحات PowerBI و Tableau.
