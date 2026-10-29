# Gold model contracts (draft - not deployed)

| Table | Grain | Natural key | Truth/access note |
|---|---|---|---|
| `streaming_fact_transactions` | one unique logical transaction per source/run | `source_system,transaction_id` (after R1 confirmation); `event_id` immutable ingest key | **no ground-truth labels** |
| `streaming_fact_alerts` | one rule/model alert | `alert_id` | multiple alerts may reference same event |
| `streaming_fact_predictions` | one model/version scoring attempt per eligible event | `event_id,model_version,scoring_run_id` | store unavailable state for missing model; do not invent score |
| `streaming_scenario_truth` | one event/scenario family pair | `run_id,event_id,scenario_family` | **evaluation-only access**, never join into detector input |
| `streaming_terminal_attempts` | one terminal outcome/attempt | approved terminal attempt ID | `DECLINED` not necessarily terminal failure |
| `dim_users_scd2` | user version with validity interval | `(source_system,user_id,valid_from)` | never overlap per entity |
| `dim_cards_scd2` | card version with validity interval | `(source_system,user_id,card_index,valid_from)` | no raw PAN/CVV |
| `historical_fact_transactions` | one stable historical record | preserved approved historical stable ID | historical `actual_is_fraud` is source truth only |

R2 owns SQL/model grains. R3 reconciles DAX with these definitions. Historical and streaming have **separate KPI universes**. Historical `Chip -> ATM` is labeled `PROJECT_ASSUMPTION`, not verified ATM observation. Currency totals by currency only. Distinct events not equal alert rows.
