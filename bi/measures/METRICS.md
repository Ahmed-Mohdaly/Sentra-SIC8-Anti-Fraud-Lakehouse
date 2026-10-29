# KPIs / BI contract

- **Account velocity**: approved positive attempts with event_time in `(t - 10 minutes, t]`, distinct event IDs, per account; doesn't treat historical card index as verified bank account.
- **Fraud detection rate**: number of truth-positive unique simulation event IDs with an alert / number of truth-positive simulation event IDs in the same run/window. Scenario-grain metric is separate. No truth => N/A.
- **Terminal failure rate**: explicit terminal failure attempts / all eligible terminal attempts. Declines with no terminal hardware/communication fault do not count in numerator.
- **Currency**: totals grouped by currency, no USD+EUR sum without verified FX conversion.
- **Alert rate**: distinct flagged events / eligible scored transactions, unlike alert_count / transactions.
- **Historical labels**: show `SOURCE_TRUTH`, never claim they are detected predictions.
