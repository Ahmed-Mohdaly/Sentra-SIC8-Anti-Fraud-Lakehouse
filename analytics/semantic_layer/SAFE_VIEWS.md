# Governed serving contract

**Not yet created in Databricks.** R2 proposes read-only Gold SQL views; R4 permits access via a role with `SELECT` only. Allowlist: `v_kpi_daily`, `v_terminal_health`, `v_alert_explanations`, `v_event_trace`. Exclude `scenario_truth` direct access except restricted evaluation view and prevent PAN/CVV exposure. Set `LIMIT 1000`, warehouse timeout and audit query IDs. The optional Genie space must point to these governed views rather than raw Bronze or arbitrary generated SQL. UI must visibly show fixture vs live and freshness.
