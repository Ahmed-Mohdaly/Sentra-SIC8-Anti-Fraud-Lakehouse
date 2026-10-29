# الأمان، التشفير، وإدارة الأسرار (Security & Secrets Management)
## الدليل الأمني الشامل لحماية البنية التحتية السحابية والحوكمة المصرفية في منصة SIC

---

> [!NOTE]
> **مسار الملف:** `docs/security/security-and-secrets.md`  
> **المبدأ الأمني:** الدفاع المتعدد في العمق (Defense in Depth) ومبدأ الامتياز الأقل (Least Privilege)

---

## 1. إدارة الأسرار والمفاتيح السحابية (AWS Secrets Manager)

لا يتم تضمين أي كلمات مرور لقواعد البيانات أو مفاتيح تشفير داخل الكود المصدري أو ملفات التكوين الملتزمة في الجيت.  
تعتمد المنصة على **AWS Secrets Manager**:
* **المعرف المعتمد:** `synthetic-data-provider/database`
* **الاسترجاع الآمن:** يقوم كلاس [`BatchReferenceExtractor`](file:///home/syd/Downloads/archive/src/ingestion/batch_reference_extractor.py#L85-L98) باستدعاء `boto3.client('secretsmanager').get_secret_value(...)` عند بدء التشغيل في الذاكرة دون تخزين السر على أقراص الخادم.
* **دعم التدوير الدوري (Secret Rotation):** يتم دعم تدوير كلمة مرور PostgreSQL تلقائياً عبر Lambda دون توقف الخدمات.

---

## 2. التشفير في السكون وأثناء النقل (Encryption at Rest & In-Transit)

```text
┌──────────────────────────────┬──────────────────────────────┬──────────────────────────────────────────┐
│ الطبقة / الخدمة              │ التشفير أثناء النقل (In-Transit)│ التشفير في السكون (At Rest)              │
├──────────────────────────────┼──────────────────────────────┼──────────────────────────────────────────┤
│ بحيرة البيانات (Amazon S3)   │ HTTPS / TLS 1.3              │ SSE-S3 (AES-256) / AWS KMS Customer Keys │
│ متجر الخصائص (Redis)         │ TLS إلزامي عبر `rediss://`    │ AWS KMS Encryption for In-Memory Storage │
│ وسيط الرسائل (Amazon MSK)    │ TLS Port 9094 / PLAINTEXT VPC│ AWS KMS Storage Encryption               │
│ قاعدة البيانات (PostgreSQL)  │ SSL Mode `require`           │ Amazon EBS KMS Encrypted Volumes         │
└──────────────────────────────┴──────────────────────────────┴──────────────────────────────────────────┘
```

* **التشفير في متجر الخصائص:** في كود [`src/ml/state_store.py`](file:///home/syd/Downloads/archive/src/ml/state_store.py#L73-L82)، يتم الاتصال عبر رابط يبدأ حصرياً بـ `rediss://` (مع رمز الحماية المعتمد في AWS Secrets Manager).

---

## 3. أمن الحاويات والتشغيل بدون صلاحيات الجذر (Non-Root Execution)

تطبيقاً لمعايير أمان الحاويات السحابية (CIS Benchmarks)، تم ضبط ملفات الـ Dockerfile للعمل تحت مستخدم غير مميز:
* في [`Dockerfile.fraud-consumer`](file:///home/syd/Downloads/archive/Dockerfile.fraud-consumer#L37-L56):
  ```dockerfile
  # إنشاء مستخدم ومجموعة غير متميزة (UID/GID 10001)
  RUN groupadd -g 10001 appgroup && \
      useradd -u 10001 -g appgroup -s /bin/bash -m appuser

  USER 10001:10001
  ```
  هذا يمنع أي مهاجم في حالة اختراق الحاوية من السيطرة على الخادم المضيف أو الوصول لموارد النظام الجذرية.

---

## 4. حماية البيانات المالية الحساسة وحجب السجلات (Data Redaction)

1. **البيانات التخليقية (Synthetic Governance):** أرقام البطاقات المصرفية (PAN) وكود الأمان (CVV) المولدة هي أرقام تخليقية مخصصة للأغراض الرياضية وتلتزم بصيغ الفحص ولكنها غير صالحة للاستخدام في الشبكات البنكية الحقيقية.
2. **منع تسريب الأسرار في السجلات:** يقوم كلاس `JSONFormatter` في [`src/ingestion/extractor.py`](file:///home/syd/Downloads/archive/src/ingestion/extractor.py#L31-L45) بتطهير رسائل السجلات وإسقاط أي ترويسات تتضمن كلمات المرور أو مفاتيح الـ API قبل طباعتها في CloudWatch.
3. **حماية واجهة الـ API:** تطبيق المصادقة عبر الترويسة `X-API-Key` على كافة العمليات التشغيلية (Start, Stop, Config Patch) لمنع التعديل غير المصرح به على معدلات التوليد.
