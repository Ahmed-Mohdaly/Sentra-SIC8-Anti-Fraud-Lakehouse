# Expected Gold Results — Synthetic Fixture v0.1

These are **expected counts for invented test rows**, not production/Databricks measurements.

| Test | Expected |
|---|---:|
| Total transactions, one per event_id | **16** |
| SIMULATOR transactions | **12** |
| HISTORICAL mock transactions | **4** |
| Fraud alerts (fact rows) | **4** |
| Unique alert-flagged transactions | **3** |
| A single event with two different alerts | **SIM-E003** |
| SIM truth positives | **3** |
| SIM TP / FP / FN / TN (event-level) | **2 / 1 / 1 / 8** |
| SIM precision and recall | **2/3 = 66.67%** each |
| SIM terminal attempts with `terminal_id` | **7** |
| SIM terminal device/network failures | **2** |
| SIM terminal failure rate | **2/7 = 28.57%** |
| SIM approved net amount (USD, signed) | **793.00** |

- **Don't join `fact_transactions` with all alerts then `SUM(amount)`**: `SIM-E003` would be counted twice.
- **Don't count every declined payment as a terminal failure**: SIM-E005 is DECLINED but has no terminal failure classification.
- **Don't take absolute values of signed refunds**: SIM-E009 is an explicit REFUND of -15 USD.
- **Don't include historical ground truth in live-simulator precision/recall**: calculate/source-label them separately.
- **Don't call historical merchant location exact**: the sample provides no verified GPS and no geo rule input proof; alerts are MOCK detection outputs.
- **`is_fraud` appears ONLY in evaluation-only CSV**, never in Silver transaction inputs.

## Proposed acceptance checks

1. `gold_fact_transactions` row count equals unique `event_id` count (16), no duplicates.
2. `gold_fact_fraud_alerts` row count is 4; event-level flagged count is 3.
3. Each alert references an existing event; each card references an existing user.
4. `dim_cards` one record per `card_id`; no unintended fan-out after joins.
5. The KPI queries match the expected values above before pointing at live Delta Silver.
6. Gold run should be **repeatable** without double-counting when source inputs are reloaded (views are safe for fixture).
