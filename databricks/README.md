# R2 — Databricks

1. Preserve the attached `historical/Untitled-Copy1.ipynb` as original exploratory work.
2. Import the actual executed Databricks notebooks `01..07` when the owner exports them, record hashes and execution evidence in `notebooks/MANIFEST.md`.
3. Confirm exact `workspace.sic8_silver.*` tables through Catalog Explorer, do not recreate with guessed DDL.
4. Inspect existing S3 object schemas; R1 `.json.gz` `value` may be nested struct or JSON string. Historical timestamps are local NTZ with unknown timezone.
5. Use UC Volumes / external locations with permissions granted by R4; no secrets in notebook.
6. Code in `src/r2_spark/spark_jobs.py` is a **starter**, never tested against live Databricks; implementation of production CDC watermarks and robust stateful streaming still required.
7. Validate type casting, quarantine, replay and SCD2 under safe new schema before enabling writes.
