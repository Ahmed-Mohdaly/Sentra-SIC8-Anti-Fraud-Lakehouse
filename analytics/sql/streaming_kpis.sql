-- DRAFT / READ ONLY: run ONLY after R2 establishes verified Gold tables.
-- Replace workspace.sic8_gold table names from actual published data dictionary.
-- Do not join alerts with transactions before summing amounts (fan-out risk).

-- KPI 1: sliding 10-minute event-time velocity, cardinality by account.
-- Strict convention: (event_time - 10 minutes, event_time] in UTC, valid approved positive attempts only.
SELECT a.account_id, a.event_id, a.event_time,
       COUNT(b.event_id) AS approved_positive_attempts_10m
FROM workspace.sic8_gold.streaming_fact_transactions a
LEFT JOIN workspace.sic8_gold.streaming_fact_transactions b
  ON a.account_id = b.account_id
 AND b.event_time > a.event_time - INTERVAL 10 MINUTES
 AND b.event_time <= a.event_time
 AND b.status = 'APPROVED' AND b.amount > 0
WHERE a.source_scope = 'SIMULATION'
GROUP BY a.account_id, a.event_id, a.event_time;

-- KPI 2: event-grain detection rate against injected truth ONLY. One truth-positive event
-- in denominator whether one or many rule alerts matched. The scenario-grain rate is separate.
WITH truth AS (
  SELECT DISTINCT event_id, run_id
  FROM workspace.sic8_gold.streaming_scenario_truth
), detected AS (
  SELECT DISTINCT event_id
  FROM workspace.sic8_gold.streaming_fact_alerts
)
SELECT t.run_id,
       COUNT(*) AS truth_positive_events,
       COUNT(d.event_id) AS detected_truth_positive_events,
       CASE WHEN COUNT(*) > 0 THEN COUNT(d.event_id) * 1.0 / COUNT(*) END AS event_detection_rate
FROM truth t LEFT JOIN detected d ON t.event_id = d.event_id GROUP BY t.run_id;

-- KPI 3: terminal failure rate, not equivalent to payment declines.
SELECT run_id,
       COUNT(*) AS terminal_attempts,
       COUNT(CASE WHEN terminal_failure_reason IS NOT NULL THEN 1 END) AS terminal_failures,
       CASE WHEN COUNT(*) > 0 THEN
       COUNT(CASE WHEN terminal_failure_reason IS NOT NULL THEN 1 END) * 1.0 / COUNT(*) END AS terminal_failure_rate
FROM workspace.sic8_gold.streaming_fact_transactions
WHERE terminal_id IS NOT NULL
GROUP BY run_id;
