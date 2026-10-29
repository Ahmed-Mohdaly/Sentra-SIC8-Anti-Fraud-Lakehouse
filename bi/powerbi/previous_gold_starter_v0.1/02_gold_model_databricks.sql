-- Databricks SQL / Spark SQL; SQL DRAFT (not deployed).
-- Run AFTER 01_load_fixture_databricks.py in a schema where you have CREATE VIEW permission.
-- USE SCHEMA sic8_fixture;  -- Enable if your session isn't already using that schema.
-- The canonical fixture inputs live in schema sic8_fixture within your current catalog.

CREATE OR REPLACE VIEW sic8_fixture.gold_dim_users AS
SELECT sha2(concat('user:', user_id), 256) AS user_sk,
       user_id, source_system, home_city, user_segment
FROM sic8_fixture.silver_users;

CREATE OR REPLACE VIEW sic8_fixture.gold_dim_cards AS
SELECT sha2(concat('card:', card_id), 256) AS card_sk,
       card_id, sha2(concat('user:', user_id), 256) AS user_sk,
       card_brand, card_type, card_status
FROM sic8_fixture.silver_cards;

CREATE OR REPLACE VIEW sic8_fixture.gold_dim_mcc AS
SELECT mcc, category, description FROM sic8_fixture.silver_mcc;

-- Fact 1 TEST GRAIN: ONE ROW per fixture event_id (NOT one row per alert).
-- Production choice between event_id/business transaction_id needs explicit contract approval.
CREATE OR REPLACE VIEW sic8_fixture.gold_fact_transactions AS
SELECT t.event_id, t.transaction_id, t.source_system,
       sha2(concat('user:', t.user_id), 256) AS user_sk,
       sha2(concat('card:', t.card_id), 256) AS card_sk,
       t.user_id, t.card_id, t.channel, t.payment_method,
       t.transaction_type, t.event_time, to_date(t.event_time) AS event_date,
       t.time_quality, t.amount, t.currency, t.outcome_status,
       t.terminal_id, t.terminal_failure_type, t.mcc, t.merchant_city
FROM sic8_fixture.silver_transactions t;

-- Fact 2 grain: ONE ROW per alert detection_id; repeated event_id is valid here.
CREATE OR REPLACE VIEW sic8_fixture.gold_fact_fraud_alerts AS
SELECT a.detection_id, a.event_id, a.rule_id, a.rule_version,
       a.detected_at, a.severity
FROM sic8_fixture.silver_fraud_alerts a;

-- Gold business KPI: alerts vs DISTINCT affected transactions (never join-and-sum amount on alert rows).
CREATE OR REPLACE VIEW sic8_fixture.gold_kpi_overview AS
WITH flagged AS (
   SELECT DISTINCT event_id FROM sic8_fixture.silver_fraud_alerts
),
per_source AS (
   SELECT t.source_system, count(*) AS transactions,
          sum(CASE WHEN f.event_id IS NOT NULL THEN 1 ELSE 0 END) AS flagged_transactions,
          sum(CASE WHEN t.outcome_status='APPROVED' THEN t.amount ELSE CAST(0 AS DECIMAL(18,2)) END) AS approved_net_amount_usd
   FROM sic8_fixture.silver_transactions t
   LEFT JOIN flagged f ON t.event_id=f.event_id
   WHERE t.currency='USD'
   GROUP BY t.source_system
),
alerts_by_source AS (
   SELECT t.source_system, count(*) AS alerts
   FROM sic8_fixture.silver_fraud_alerts a
   JOIN sic8_fixture.silver_transactions t ON a.event_id=t.event_id
   GROUP BY t.source_system
)
SELECT p.source_system, p.transactions, p.flagged_transactions,
       coalesce(a.alerts,0) AS alerts, p.approved_net_amount_usd
FROM per_source p LEFT JOIN alerts_by_source a ON p.source_system=a.source_system;

-- Gold evaluation: DO NOT use the label table in detection code.
-- One prediction = any alert for that event_id (collapse multiple rules).
CREATE OR REPLACE VIEW sic8_fixture.gold_fraud_evaluation AS
WITH predicted AS (SELECT DISTINCT event_id FROM sic8_fixture.silver_fraud_alerts),
base AS (
 SELECT t.source_system, t.event_id,
        CASE WHEN g.is_fraud=true THEN 1 ELSE 0 END AS actual,
        CASE WHEN p.event_id IS NOT NULL THEN 1 ELSE 0 END AS predicted
 FROM sic8_fixture.silver_ground_truth_evaluation_only g
 JOIN sic8_fixture.silver_transactions t ON t.event_id=g.event_id
 LEFT JOIN predicted p ON p.event_id=g.event_id
),
cm AS (
 SELECT source_system,
  sum(CASE WHEN actual=1 AND predicted=1 THEN 1 ELSE 0 END) AS tp,
  sum(CASE WHEN actual=0 AND predicted=1 THEN 1 ELSE 0 END) AS fp,
  sum(CASE WHEN actual=1 AND predicted=0 THEN 1 ELSE 0 END) AS fn,
  sum(CASE WHEN actual=0 AND predicted=0 THEN 1 ELSE 0 END) AS tn
 FROM base GROUP BY source_system
)
SELECT *,
       CAST(tp AS DOUBLE)/nullif(tp+fp,0) AS precision,
       CAST(tp AS DOUBLE)/nullif(tp+fn,0) AS recall
FROM cm;

-- Terminal device/network failures: DECLINED transactions still count as attempts,
-- but a normal decline is NOT automatically a terminal failure.
CREATE OR REPLACE VIEW sic8_fixture.gold_terminal_kpi AS
SELECT source_system,
       count(*) AS terminal_attempts,
       sum(CASE WHEN terminal_failure_type IN ('DEVICE_ERROR','NETWORK_TIMEOUT') THEN 1 ELSE 0 END) AS terminal_failures,
       CAST(sum(CASE WHEN terminal_failure_type IN ('DEVICE_ERROR','NETWORK_TIMEOUT') THEN 1 ELSE 0 END) AS DOUBLE)
           /nullif(count(*),0) AS terminal_failure_rate
FROM sic8_fixture.silver_transactions
WHERE terminal_id IS NOT NULL AND terminal_id <> ''
GROUP BY source_system;

-- Basic checks / المتوقع يظهر هنا:
SELECT * FROM sic8_fixture.gold_kpi_overview ORDER BY source_system;
SELECT * FROM sic8_fixture.gold_fraud_evaluation ORDER BY source_system;
SELECT * FROM sic8_fixture.gold_terminal_kpi ORDER BY source_system;
SELECT count(*) AS txn_fact_rows FROM sic8_fixture.gold_fact_transactions;
SELECT count(*) AS alert_fact_rows FROM sic8_fixture.gold_fact_fraud_alerts;

-- Follow-up, NOT DONE: replace these illustrative views with Delta tables +
-- SCD2 dimensions and tested MERGE strategy after CDC contract/schema version agreement.
