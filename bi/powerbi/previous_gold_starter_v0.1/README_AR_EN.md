# SIC8 — Role 3 Gold Starter Fixture (v0.1)

**الغرض:** يبدأ Role 3 تنفيذ `Gold Modeling + KPIs + Dashboard` بالتوازي مع Role 2، من غير ما يستنى الـSpark Pipeline الحقيقية.

**تنبيه مهم:** كل القيم هنا **Synthetic examples hand-authored for tests**؛ **ليست مستخرجة من Kaggle ولا من Live AWS/S3**، ومش دليل إن Fraud Engine اكتشف حاجة فعلًا. أسماء وأشكال Silver هنا **proposed**, وتحتاج مراجعة/اعتماد مع Role 1/2 قبل التنفيذ الرسمي.

## Contents / محتويات الحزمة

| File | Grain / وظيفته |
|---|---|
| `fixtures/silver_transactions.csv` | 16 unique transaction events: 12 synthetic stream examples + 4 illustrative historical examples |
| `fixtures/silver_users.csv` | One row per user — Type 1 mock/current state only |
| `fixtures/silver_cards.csv` | One row per card — Type 1 mock/current state only |
| `fixtures/silver_mcc.csv` | Reference categories by MCC; not a merchant registry |
| `fixtures/silver_fraud_alerts.csv` | One row **per detection**; same event may have multiple alerts |
| `fixtures/silver_ground_truth_EVALUATION_ONLY.csv` | Separate labels for **evaluation only**, never join to detector input |
| `01_load_fixture_databricks.py` | Suggested Databricks notebook code to load fixture files into a sandbox schema as Delta |
| `02_gold_model_databricks.sql` | Gold dimensions, facts, KPI queries; reference draft, not deployed |
| `03_verify_fixture_local.py` | Uses Python stdlib only to validate fixture and expected metrics locally |
| `GOLD_MODEL_DRAFT_AR_EN.md` | Proposed fact grains, dimensions, SCD/CDC decisions and tests |

## Run / طريقة الاستخدام

1. فكّ الـZIP وارفع مجلد `fixtures/` إلى موقع مناسب في Databricks Volume / DBFS accessible to your workspace. **مافيش Volume path حقيقي مثبت حاليًا**؛ خلي Role 4 يحدد المكان والصلاحيات.
2. افتح `01_load_fixture_databricks.py` وعدّل `FIXTURE_DIR`, `TARGET_SCHEMA` فقط بما يناسب الـWorkspace. شغّله في Databricks Python notebook (باستخدام Spark وDelta).
3. شغّل `02_gold_model_databricks.sql` في Notebook SQL أو Databricks SQL بعد اختيار نفس الـCatalog/Schema. الكود بيعمل **Views** في الـSandbox عشان ما يحجزش Production Tables.
4. راجع `EXPECTED_RESULTS.md`. وجرّب `python 03_verify_fixture_local.py` محليًا للتأكد من الـCSV fixture نفسها.

## Agreements before production / قرارات مهمة

- **Fact grains:** `gold_fact_transactions` صف واحد لكل `event_id`، و`gold_fact_fraud_alerts` صف لكل `detection_id`. الاتنين مش نفس الـGrain.
- `dim_users`, `dim_cards` في الـFixture بيعرضوا **Type 1 current state**. الـSCD Type 2 وValidity Intervals (`valid_from`, `valid_to`) لازم تتحدد لاحقًا من CDC actual semantics.
- **Actual identity:** `user_id` مش بالضرورة bank account ID. `card_id` في Fixture هو canonical namespaced ID. `account_id` مافيش له دليل historical، ومش بَنخمّنه.
- **Money:** USD chosen **for mock only**. Role 1 report shows EUR in a live example while previous canonical proposed USD. قبل الدمج الحقيقي لازم Currency Policy واضحة.
- **Terminal failures:** `terminal_failure_type` field **proposed fixture extension**, not proven available in Role 1 live payload. `DECLINED` ليس بالضرورة Terminal failure.
- **Historical:** `transaction_id` empty means not supplied as source truth, `time_quality` flags timezone assumption; don't mix historical vs stream KPIs blindly.
- **Truth labels:** isolated **sidecar**; if Rule 2 detector reads `is_fraud`, that's invalid leakage. Gold evaluation after detections only.
- **Metrics:** Alert count != flagged unique transactions; refund affects signed/net amount and should not be claimed as purchase. Other financial metrics need explicit definition.
- **Dim merchant and terminal:** existing docs provide MCC/cities, not a verified normalized merchant/terminal master. `dim_merchants` or `dim_terminals` remain proposed pending actual source.

## Suggested minimal Role 3 handoff

**يبدأ الآن:** Gold SQL views + Dashboard layout + KPI definitions tested on fixture.

**يسلّم Role 2 → R3 لاحقًا:** source-to-Silver schema, unique IDs, precision semantics, timestamp quality flags, fraud alerts, label sidecar, and reconciliation results. Replace the fake fixture inputs with real Silver data only after Contract Review.
