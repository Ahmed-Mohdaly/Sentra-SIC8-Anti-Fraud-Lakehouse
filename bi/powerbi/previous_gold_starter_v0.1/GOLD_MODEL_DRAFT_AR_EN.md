# Gold Modeling Draft / تصور مبدئي للـ Gold

**Status: PROPOSED — NOT AN APPROVED PRODUCTION CONTRACT.**

## 1) Fact Grain — أهم قرار قبل الكود

| Asset | Grain / كل Row يمثل | Key for fixture | نقطة محتاجة اعتماد |
|---|---|---|---|
| `gold_fact_transactions` | حدث معاملة واحد، بدون تضاعف بسبب Alerts | `event_id` | هل الـProduction Grain سيكون Event أم Business `transaction_id`؟ وما قواعد Retry/Replay؟ |
| `gold_fact_fraud_alerts` | Detection occurrence واحدة لكل Rule firing | `detection_id` | قواعد إعادة إصدار Alert ونفس الـWindow والـRule version |
| `gold_fraud_evaluation` | Aggregate by source (Demo View وليس Fact Table) | `source_system` | هل التقييم event-level ولا scenario-level؟ وما هو evaluation universe؟ |
| `gold_terminal_kpi` | Aggregate of eligible terminal attempts | `source_system` | Terminal ID + authoritative failure taxonomy من Role 1 |

**Rule:** `fact_transactions` و`fact_fraud_alerts` علاقتهم One-to-Many. مينفعش `SUM(transaction.amount)` على Join مباشرة مع الـAlerts؛ ده هيرفع المبلغ عند الـMultiple Alerts.

## 2) Dimensions — الأبعاد في النسخة المبدئية

- `gold_dim_users`: صف لكل user حاليًا (**Type 1 mock**). `user_sk` stable deterministic demo hash، مش enterprise SCD2 surrogate design نهائي.
- `gold_dim_cards`: صف لكل card حاليًا (**Type 1 mock**). لازم `card_id` يبقى namespaced وتتفادى `(user_id)` joins اللي تعمل Fan-out.
- `gold_dim_mcc`: MCC categorization فقط؛ **مش** `dim_merchants` معرّفة بالكامل.
- `dim_terminal` و`dim_merchants`: **Pending** لأن Role 1 handover ما أثبتش وجود Registry موحد ومفاتيح ثابتة لكل واحد، حتى لو Transactions فيها Terminal/merchant attributes.
- `dim_date` ممكن يتضاف بعد تثبيت Event Time policy، مش ضروري لأول Fixture.

## 3) SCD Type 2 / CDC

من وثيقة Role 1، Users/Cards بييجوا عن طريق **PostgreSQL CDC → Debezium → S3**. ده يخلّي SCD Type 2 اختيار منطقي (لمعرفة Attributes وقت المعاملة)، لكن تصميم الـValid-time fields وCDC ordering والـDelete/snapshot policy محتاج يتثبت بناءً على Payloads فعلية واختبارات، مش مجرد افتراض.

## 4) Handoff R2 → R3

`silver_transactions` + `silver_fraud_alerts` + `silver_users` + `silver_cards` + `silver_mcc` + `ground_truth` منفصلة. لازم اتفاق مسبق على: أنواع الـFields، Nullable policy، Cardinality، ID stability، source/time/currency semantics، Fraud label sidecar access، وterminal failure taxonomy.

## 5) Acceptance / اختبارات إن الـGold صحيح

1. `COUNT(fact_transactions) == 16` للـFixture ولا توجد `event_id` duplicates.
2. `COUNT(fact_fraud_alerts) == 4` لكن `COUNT(DISTINCT event_id)` للـFlagged == 3.
3. SIM event-level `TP=2`, `FP=1`, `FN=1`, `TN=8` ⇒ precision/recall كل منهم 2/3.
4. Terminal failures `2 / 7`, وليس 3 / 7 لأن `DECLINED` ليس Failure بالضرورة.
5. Approved signed amount USD: `793.00` (متضمن Refund بـ`-15.00`).
6. مفيش joins تضاعف transaction grain أو labels متاحة للـDetection Engine.

**Next step:** Role 3 يبني الـDashboard/KPIs على الـFixture دي، وبعد مراجعة الـCanonical Schema وSource payloads مع Role 1/2 يبدل المصدر بـReal Silver بدون تغيير Business Logic إلا لما يكون فيه Contract Change مُوثّق.
