# Lineage / trace explorer contract

Track identifiers: `run_id`, `event_id`, `transaction_id`, `source_system`, Kafka `topic/partition/offset`, S3 object key/etag, raw ingestion timestamp, parser version, Silver merge commit, rule/version, model/version or explicit unavailability, alert_id, Gold publication and dashboard freshness. One stage may be missing or delayed; show `UNKNOWN` not success. Source truth and labels must never appear in scorer inputs. `apps/analyst_portal/api/main.py` offers local SQLite fixture trace with explicit missing stages.
