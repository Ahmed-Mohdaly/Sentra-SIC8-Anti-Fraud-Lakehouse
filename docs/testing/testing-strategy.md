# استراتيجية الاختبارات والتحقق (Testing Strategy)
## الدليل الشامل لاختبارات الوحدة، التكامل، النماذج، ومحاكاة الأحمال

---

> [!NOTE]
> **مسار الملف:** `docs/testing/testing-strategy.md`  
> **مجلد الاختبارات:** [`tests/`](file:///home/syd/Downloads/archive/tests/)  
> **إجمالي الاختبارات:** 25 ملف اختبار موزع بين Unit, Integration, Load

---

## 1. هرم الاختبارات في المنظومة (Testing Pyramid)

تعتمد المنصة هرم اختبارات متدرج يضمن موثوقية كل طبقة قبل دمجها:

```text
                  ▲
                 / \
                /   \     Load & Stress Tests (Load Harness, Cloud Stress)
               /─────\
              /       \    Integration Tests (Kafka, Multi-Source E2E)
             /─────────\
            /           \   Unit & Contract Tests (22 Test Modules: Pydantic, ML, DB)
           ───────────────
```

---

## 2. تفصيل فئات الاختبارات وطريقة تشغيلها

### 2.1 اختبارات الوحدة والعقود البرمجية (`tests/unit/`)
تختبر كل وظيفة برمجية بمعزل عن الشبكة أو الخدمات الخارجية باستخدام Mocks و In-Memory Databases:

| اسم ملف الاختبار | المكون المفحوص | ما يتم اختباره |
| :--- | :--- | :--- |
| `test_rate_controller.py` | `RateController` | ثبات الـ TPS، منع الطفرات الانفجارية، وتعديل السرعة الحية. |
| `test_scenarios.py` | `ScenarioEngine` | التوزيع الإحصائي للسيناريوهات الثمانية وأسباب الرفض. |
| `test_ml_fraud.py` | `FraudStreamingConsumer` | مطابقة متجه الميزات الـ 34، استنتاج LightGBM، والعتبة. |
| `test_ml_leakage_and_parity.py`| `FeatureExtractor` | ضمان التطابق التام بين ميزات التدريب وميزات البث اللحظي. |
| `test_state_store_parity.py` | `StateStore` | مطابقة حسابات النوافذ المتحركة بين Redis والبديل المحلي. |
| `test_extractor.py` | `TransactionExtractor` | بوابات جودة البيانات، عزل السجلات السالبة، وتحديث DynamoDB. |
| `test_event_schema.py` | `EventEnvelope` | مطابقة عقود البيانات، صحة الإحداثيات الجغرافية، والـ PAN. |
| `test_api.py` & `test_transaction_api.py` | `FastAPI` | نقاط الـ Probes، التوثيق، و Keyset Cursor Pagination. |

```bash
# أمر تشغيل اختبارات الوحدة بالكامل:
pytest tests/unit/ -v
```

---

### 2.2 اختبارات التكامل والترابط الحي (`tests/integration/`)
تختبر تفاعل المكونات مع وسطاء الرسائل وقواعد البيانات الحقيقية:
* **`test_kafka_integration.py`:** يختبر نشر الرسائل واستقبالها عبر كافكا، التحقق من مفاتيح التوزيع، واختبار سلوك الـ Rebalance.
* **`test_pipeline_e2e.py`:** يشغل دورة حياة مصغرة؛ يولد المستخدمين، يكتبهم في قاعدة البيانات، يضخ المعاملات، ويتحقق من استهلاكها بواسطة مستهلك الاحتيال.

```bash
# أمر تشغيل اختبارات التكامل:
pytest tests/integration/ -v
```

---

### 2.3 اختبارات الأداء ومحاكاة الأحمال (`tests/load/` & `scripts/`)
تختبر قدرة المنصة على استيعاب الضغط العالي وقياس الـ Latency:
* **`tests/load/test_load_harness.py`:** إطار عمل خفيف لاختبار توليد 10,000 معاملة وقياس استهلاك الذاكرة.
* **`scripts/run_cloud_stress_test.py`:** سكريبت سحابي متقدم يضخ آلاف المعاملات بمعدلات تصاعدية (10 إلى 100+ TPS) ويقيس:
  * نسبة تأكيد التسليم (Delivery Success Rate).
  * زمن استجابة الـ End-to-End Latency ومعدل الـ P99.
  * استقرار استهلاك الـ CPU في حاويات ECS Fargate.

```bash
# تشغيل اختبار الحمل السحابي:
python scripts/run_cloud_stress_test.py --target-tps 50 --duration-seconds 60
```
