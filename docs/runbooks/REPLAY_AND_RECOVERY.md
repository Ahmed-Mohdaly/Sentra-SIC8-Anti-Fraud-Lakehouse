# Replay / recovery gate (not yet cloud tested)

1. Record dataset snapshot IDs, Kafka topic/partition/offset, event IDs, run ID, job/checkpoint and target Delta version.
2. Reprocess approved small test run, never reset production checkpoint without review.
3. Ensure raw duplicates are retained as attempts, but Silver/Gold business event count stays unique by approved identity; write idempotent Delta MERGE.
4. Track late events and source CDC updates with source position and SCD2 validity; quarantine invalid and malformed, not silently drop.
5. Verify dashboard KPIs, Gold commit, and reconciliation after restart; record evidence IDs, time-to-recover and actual logs.
6. Preserve previous table versions and do not use DROP/OVERWRITE for demonstration.
