"""DAG 1: Incremental Transaction Ingestion to S3 Bronze Lakehouse.

Orchestrates incremental extraction from the FastAPI Transaction Plane into
S3 Bronze Parquet using keyset pagination and DynamoDB watermarking.
Schedule: Every 15 minutes.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

try:
    from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
except ImportError:
    # Graceful fallback for local development without databricks provider installed
    from airflow.operators.empty import EmptyOperator as DatabricksRunNowOperator  # type: ignore

# Ensure src/ is importable
SRC_DIR = "/opt/archive/src" if os.path.exists("/opt/archive/src") else os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src"))
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from ingestion.extractor import TransactionExtractor, resolve_service_private_url

logger = logging.getLogger("airflow.dag_01_incremental_ingestion")

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
}


def task_check_api_readiness(**context: Any) -> str:
    """Verify that the FastAPI Transaction Plane is healthy and reachable."""
    import httpx

    api_url = os.environ.get("API_BASE_URL", "auto")
    if api_url == "auto":
        discovered = resolve_service_private_url()
        api_url = discovered or "http://13.38.219.168:8000"

    ready_endpoint = f"{api_url.rstrip('/')}/ready"
    logger.info("Probing API readiness at %s ...", ready_endpoint)

    with httpx.Client(timeout=10.0) as client:
        resp = client.get(ready_endpoint)
        if resp.status_code != 200:
            raise RuntimeError(f"API readiness probe failed with HTTP {resp.status_code}: {resp.text}")
        data = resp.json()
        logger.info("API is ready: %s", data)

    return api_url


def task_extract_transactions(**context: Any) -> Dict[str, Any]:
    """Execute one incremental ingestion cycle via TransactionExtractor."""
    ti = context["ti"]
    api_url = ti.xcom_pull(task_ids="check_api_readiness") or os.environ.get("API_BASE_URL", "auto")
    s3_bucket = os.environ.get("S3_BUCKET", "sic-pipeline-storage")
    batch_limit = int(os.environ.get("INGESTION_BATCH_LIMIT", "500"))

    logger.info("Initializing TransactionExtractor (API: %s, Bucket: %s, Limit: %d)", api_url, s3_bucket, batch_limit)
    extractor = TransactionExtractor(api_base_url=api_url, s3_bucket=s3_bucket)
    summary = extractor.run_ingestion_cycle(limit=batch_limit)

    logger.info("Ingestion cycle result: %s", json.dumps(summary, indent=2))
    if summary.get("status") in ("FAILED", "ERROR"):
        raise RuntimeError(f"Ingestion cycle ended in error: {summary}")

    return summary


DATABRICKS_JOB_ID = int(os.environ.get("DATABRICKS_JOB_ID", "815854574980923"))


def task_verify_s3_landing(**context: Any) -> bool:
    """Verify that the extracted Parquet object was successfully committed to S3.
    
    Acts as a ShortCircuit check: returns True to continue downstream Databricks
    processing if new records landed; returns False if zero records were extracted.
    """
    import boto3

    ti = context["ti"]
    summary = ti.xcom_pull(task_ids="extract_transactions")
    if not summary or summary.get("records_written", 0) == 0:
        logger.info("Zero records were written in this cycle (all caught up or quarantined). Skipping downstream Databricks Lakehouse run.")
        return False

    s3_path = summary.get("s3_path", "")
    if not s3_path.startswith("s3://"):
        logger.warning("No s3_path returned in summary, skipping downstream Databricks run.")
        return False

    parts = s3_path[5:].split("/", 1)
    bucket = parts[0]
    key = parts[1]

    s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "eu-west-3"))
    head = s3.head_object(Bucket=bucket, Key=key)
    size = head.get("ContentLength", 0)
    logger.info("Verified S3 landing: %s (Size: %d bytes, LastModified: %s)", s3_path, size, head.get("LastModified"))
    if size == 0:
        raise ValueError(f"S3 Parquet file at {s3_path} is empty (0 bytes)!")

    return True


with DAG(
    dag_id="dag_01_incremental_ingestion",
    default_args=DEFAULT_ARGS,
    description="Incremental Keyset API Extraction -> S3 Bronze Parquet -> Databricks Lakehouse",
    schedule="*/15 * * * *",  # Every 15 minutes
    start_date=datetime(2026, 10, 25),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "bronze", "transactions", "lakehouse", "databricks"],
) as dag:

    check_api = PythonOperator(
        task_id="check_api_readiness",
        python_callable=task_check_api_readiness,
    )

    extract = PythonOperator(
        task_id="extract_transactions",
        python_callable=task_extract_transactions,
    )

    verify_s3 = ShortCircuitOperator(
        task_id="verify_s3_landing",
        python_callable=task_verify_s3_landing,
    )

    trigger_databricks = DatabricksRunNowOperator(
        task_id="trigger_databricks_lakehouse",
        databricks_conn_id="databricks_default",
        job_id=DATABRICKS_JOB_ID,
        wait_for_termination=True,
    )

    check_api >> extract >> verify_s3 >> trigger_databricks
