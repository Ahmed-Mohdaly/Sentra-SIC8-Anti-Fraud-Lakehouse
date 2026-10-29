# Evidence register

Do NOT check in fake terminal output or screenshots. Every record: `evidence_id`, `purpose`, `owner`, `source`, `environment`, `commit_sha`, UTC timestamp, `expected_result`, `observed_result`, command/run log path, and `status`. Allowed statuses: VERIFIED, TEAM_REPORTED, IMPLEMENTED_UNTESTED, PLANNED, BLOCKED. Local pytest and local SQLite demo are not proof of live AWS/Databricks operation. Create separate owner evidence files in `evidence/r1..r4` and `evidence/integration` when available.
