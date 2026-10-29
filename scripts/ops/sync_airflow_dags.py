#!/usr/bin/env python3
"""Sync local Airflow DAGs and configuration to AWS S3 (sic-pipeline-storage/airflow/).

Allows seamless continuous deployment of DAGs to AWS MWAA or AWS ECS Fargate Airflow tasks.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

import boto3

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("sync_airflow_dags")

DEFAULT_BUCKET = "sic-pipeline-storage"
DEFAULT_REGION = "eu-west-3"
DEFAULT_PREFIX = "airflow/dags"


def sync_dags(
    local_dir: Path,
    bucket: str = DEFAULT_BUCKET,
    prefix: str = DEFAULT_PREFIX,
    region: str = DEFAULT_REGION,
) -> int:
    """Upload all Python DAG files from local_dir to s3://bucket/prefix/."""
    s3 = boto3.client("s3", region_name=region)
    dag_files = list(local_dir.glob("*.py"))
    if not dag_files:
        logger.warning("No .py files found in %s", local_dir)
        return 0

    uploaded = 0
    for file_path in dag_files:
        s3_key = f"{prefix.rstrip('/')}/{file_path.name}"
        logger.info("Uploading %s -> s3://%s/%s ...", file_path.name, bucket, s3_key)
        with open(file_path, "rb") as f:
            s3.put_object(
                Bucket=bucket,
                Key=s3_key,
                Body=f.read(),
                ContentType="text/x-python",
            )
        uploaded += 1

    logger.info("Successfully synced %d DAG files to s3://%s/%s/", uploaded, bucket, prefix)
    return uploaded


def main():
    parser = argparse.ArgumentParser(description="Sync Airflow DAGs to S3")
    parser.add_argument("--local-dir", default="airflow/dags", help="Local directory containing DAG files")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET, help="Target S3 Bucket")
    parser.add_argument("--prefix", default=DEFAULT_PREFIX, help="Target S3 Key Prefix")
    parser.add_argument("--region", default=DEFAULT_REGION, help="AWS Region")
    args = parser.parse_args()

    local_path = Path(args.local_dir)
    if not local_path.exists():
        logger.error("Directory not found: %s", local_path)
        sys.exit(1)

    count = sync_dags(local_path, bucket=args.bucket, prefix=args.prefix, region=args.region)
    print(f"Sync complete. Uploaded {count} files.")


if __name__ == "__main__":
    main()
