"""DAG 2: Daily Reference Data Extraction to S3 Bronze Lakehouse.

Extracts full reference snapshots from RDS PostgreSQL (MCC and Cities)
and persists versioned, partitioned Parquet datasets directly to Amazon S3 Bronze.
Schedule: Daily at 00:00 UTC.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator

# Ensure src/ is importable
SRC_DIR = "/opt/archive/src" if os.path.exists("/opt/archive/src") else os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src"))
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from ingestion.batch_reference_extractor import (
    CITY_ARROW_SCHEMA,
    MCC_ARROW_SCHEMA,
    BatchReferenceExtractor,
    get_database_url,
)

logger = logging.getLogger("airflow.dag_02_batch_reference")

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=15),
}


def task_extract_mcc(**context: Any) -> Dict[str, Any]:
    """Extract MCC reference table from RDS PostgreSQL and upload Parquet to S3."""
    db_url = get_database_url()
    s3_bucket = os.environ.get("S3_BUCKET", "sic-pipeline-storage")
    region = os.environ.get("AWS_REGION", "eu-west-3")

    extractor = BatchReferenceExtractor(db_url=db_url, s3_bucket=s3_bucket, region=region)
    rows, stats = extractor.extract_mcc()
    s3_key = extractor.upload_dataset_to_s3("mcc", rows, MCC_ARROW_SCHEMA, stats)

    result = {
        "dataset": "mcc",
        "s3_key": s3_key,
        "s3_path": f"s3://{s3_bucket}/{s3_key}",
        "record_count": len(rows),
        "extracted_at": stats.get("extracted_at"),
    }
    logger.info("MCC extraction completed: %s", json.dumps(result, indent=2))
    return result


def task_extract_cities(**context: Any) -> Dict[str, Any]:
    """Extract City reference table from RDS PostgreSQL and upload Parquet to S3."""
    db_url = get_database_url()
    s3_bucket = os.environ.get("S3_BUCKET", "sic-pipeline-storage")
    region = os.environ.get("AWS_REGION", "eu-west-3")

    extractor = BatchReferenceExtractor(db_url=db_url, s3_bucket=s3_bucket, region=region)
    rows, stats = extractor.extract_cities()
    s3_key = extractor.upload_dataset_to_s3("cities", rows, CITY_ARROW_SCHEMA, stats)

    result = {
        "dataset": "cities",
        "s3_key": s3_key,
        "s3_path": f"s3://{s3_bucket}/{s3_key}",
        "record_count": len(rows),
        "extracted_at": stats.get("extracted_at"),
    }
    logger.info("Cities extraction completed: %s", json.dumps(result, indent=2))
    return result


def task_validate_landing(**context: Any) -> None:
    """Validate that both MCC and City reference Parquet snapshots are in S3 with non-zero size."""
    import boto3

    ti = context["ti"]
    mcc_info = ti.xcom_pull(task_ids="extract_mcc_reference")
    cities_info = ti.xcom_pull(task_ids="extract_cities_reference")

    s3_bucket = os.environ.get("S3_BUCKET", "sic-pipeline-storage")
    region = os.environ.get("AWS_REGION", "eu-west-3")
    s3 = boto3.client("s3", region_name=region)

    for item in [mcc_info, cities_info]:
        if not item or "s3_key" not in item:
            raise ValueError(f"Missing extraction metadata: {item}")
        key = item["s3_key"]
        head = s3.head_object(Bucket=s3_bucket, Key=key)
        size = head.get("ContentLength", 0)
        logger.info(
            "Verified S3 Reference Snapshot: s3://%s/%s (Size: %d bytes, Records: %s)",
            s3_bucket,
            key,
            size,
            item.get("record_count"),
        )
        if size == 0:
            raise ValueError(f"Reference Parquet snapshot s3://{s3_bucket}/{key} is 0 bytes!")


with DAG(
    dag_id="dag_02_batch_reference",
    default_args=DEFAULT_ARGS,
    description="Daily Snapshot Extraction of MCC & City Reference -> S3 Bronze Parquet",
    schedule="0 0 * * *",  # Daily at Midnight UTC
    start_date=datetime(2026, 10, 25),
    catchup=False,
    max_active_runs=1,
    tags=["reference", "bronze", "mcc", "cities", "lakehouse"],
) as dag:

    mcc_task = PythonOperator(
        task_id="extract_mcc_reference",
        python_callable=task_extract_mcc,
    )

    cities_task = PythonOperator(
        task_id="extract_cities_reference",
        python_callable=task_extract_cities,
    )

    validate_task = PythonOperator(
        task_id="validate_reference_landing",
        python_callable=task_validate_landing,
    )

    [mcc_task, cities_task] >> validate_task
