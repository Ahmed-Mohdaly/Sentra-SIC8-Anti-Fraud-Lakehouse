# Databricks deployments are OFF by default

Published, **user-reported verified** historical Silver objects in `workspace.sic8_silver` must not be dropped/overwritten; this repo does not create them. Historical Gold `workspace.sic8_gold` is *prepared, not publication-verified*. `spark_jobs.py` rejects writes by default. A R2/R4-reviewed PR with exact target table and explicit approval is needed before any Delta MERGE, CREATE TABLE, or migration. Test first in an isolated staging schema.
