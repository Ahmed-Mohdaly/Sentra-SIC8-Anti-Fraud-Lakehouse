# Operating scripts

This directory is intentionally a handoff for R4's **actual** runbooks and cloud-safe status/replay scripts; no unreviewed remote mutations shipped here. Local replay can be demonstrated by invoking `make demo-local` twice, which must not increase SQLite logical event counts. Cloud replay must explicitly opt-in and record checkpoints/Delta versions.
