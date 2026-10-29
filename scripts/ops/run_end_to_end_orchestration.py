#!/usr/bin/env python3
"""Run End-to-End Lakehouse Orchestration Pipeline (Airflow -> Databricks).

Executes the exact sequence orchestrated by Airflow:
1. Probe FastAPI Transaction Plane Readiness
2. Run Incremental Extraction & Land Bronze Parquet on S3
3. Verify S3 Parquet Landing
4. Trigger Databricks Lakehouse Workflow Job (815854574980923)
5. Track Run Status & Print Live Monitoring Link
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock

# Ensure src/ is importable
SRC_DIR = Path(__file__).resolve().parent.parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# Set environment
os.environ.setdefault("API_BASE_URL", "http://13.38.219.168:8000")
os.environ.setdefault("DATABRICKS_HOST", "https://dbc-d569636e-57c6.cloud.databricks.com")
os.environ.setdefault("DATABRICKS_TOKEN", "")
os.environ.setdefault(
    "AIRFLOW_CONN_DATABRICKS_DEFAULT",
    "databricks://@dbc-d569636e-57c6.cloud.databricks.com?token="
)

import importlib.util

dag_path = Path(__file__).resolve().parent.parent / "airflow" / "dags" / "dag_01_incremental_ingestion.py"
spec = importlib.util.spec_from_file_location("dag_01_module", str(dag_path))
dag_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dag_mod)

DATABRICKS_JOB_ID = dag_mod.DATABRICKS_JOB_ID
task_check_api_readiness = dag_mod.task_check_api_readiness
task_extract_transactions = dag_mod.task_extract_transactions
task_verify_s3_landing = dag_mod.task_verify_s3_landing
from airflow.providers.databricks.hooks.databricks import DatabricksHook


def main() -> None:
    print("=" * 70)
    print("🚀 SIC LAKEHOUSE END-TO-END ORCHESTRATION RUNNER")
    print(f"Target Databricks Job ID: {DATABRICKS_JOB_ID}")
    print("=" * 70)

    # --------------------------------------------------------------------------
    # STEP 1: Check API Readiness
    # --------------------------------------------------------------------------
    print("\n[STEP 1/4] Probing FastAPI Transaction Plane Readiness on AWS...")
    api_url = task_check_api_readiness()
    print(f"✅ API is healthy and ready at: {api_url}")

    # --------------------------------------------------------------------------
    # STEP 2: Extract Transactions to S3 Bronze
    # --------------------------------------------------------------------------
    print("\n[STEP 2/4] Running Incremental Extraction & S3 Landing...")
    ti = MagicMock()
    ti.xcom_pull.return_value = api_url
    summary = task_extract_transactions(ti=ti)
    print(f"✅ Extraction completed: status={summary.get('status')}, records_written={summary.get('records_written')}")

    # --------------------------------------------------------------------------
    # STEP 3: Verify S3 Landing
    # --------------------------------------------------------------------------
    print("\n[STEP 3/4] Verifying S3 Bronze Parquet Landing...")
    ti.xcom_pull.return_value = summary
    proceed = task_verify_s3_landing(ti=ti)
    print(f"ℹ️ S3 landing check result: proceed_flag={proceed}")

    # --------------------------------------------------------------------------
    # STEP 4: Trigger Databricks Lakehouse Job
    # --------------------------------------------------------------------------
    print(f"\n[STEP 4/4] Triggering Databricks Multi-Task Job ({DATABRICKS_JOB_ID})...")
    hook = DatabricksHook(databricks_conn_id="databricks_default")
    run_response = hook.run_now({"job_id": DATABRICKS_JOB_ID})
    run_id = run_response.get("run_id") if isinstance(run_response, dict) else run_response
    print(f"🎉 SUCCESS! Databricks Job Launched! Run ID: {run_id}")

    run_url = f"https://dbc-d569636e-57c6.cloud.databricks.com/#job/{DATABRICKS_JOB_ID}/run/{run_id}"
    print(f"\n🔗 LIVE DATABRICKS MONITORING URL:\n   {run_url}\n")

    # Poll status for the first 30 seconds to show state transition
    print("📡 Polling initial Databricks run status...")
    for _ in range(6):
        time.sleep(5)
        state_dict = hook.get_run_state(run_id)
        life_cycle = state_dict.life_cycle_state
        result = state_dict.result_state
        print(f"   [Status] LifeCycle: {life_cycle} | Result: {result or 'IN_PROGRESS'}")
        if life_cycle in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            break

    print("\n" + "=" * 70)
    print("✅ ORCHESTRATION TRIGGER COMPLETE!")
    print(f"Open Databricks to monitor progress: {run_url}")
    print("=" * 70)


if __name__ == "__main__":
    main()
