#!/usr/bin/env python3
"""Reference Data Ingestion Pipeline: Lands baseline reference CSV datasets into S3 Bronze storage in Parquet format.
"""

from __future__ import annotations

import io
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import boto3
import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("reference_csv_ingest")

S3_BUCKET = os.environ.get("S3_BUCKET", "sic-pipeline-storage")
AWS_REGION = os.environ.get("AWS_REGION", "eu-west-3")

REFERENCE_SETS: Dict[str, str] = {
    "users": "sd254_users.csv",
    "cards": "sd254_cards.csv",
    "merchants": "merchant_category_codes.csv",
    "cities": "city_reference.csv",
}


def ingest_reference_dataset(name: str, filename: str, data_dir: Path, s3_client: Any) -> None:
    csv_path = data_dir / filename
    if not csv_path.exists():
        logger.error(f"Reference file {csv_path} does not exist!")
        return

    logger.info(f"Ingesting reference dataset '{name}' from {csv_path}...")
    
    # Read CSV with pyarrow (BOM safe)
    read_options = pcsv.ReadOptions(autogenerate_column_names=False)
    parse_options = pcsv.ParseOptions(delimiter=",", quote_char='"')
    
    # Handle BOM in headers if present
    table = pcsv.read_csv(csv_path, read_options=read_options, parse_options=parse_options)
    
    # Clean header names (strip BOM \ufeff)
    clean_names = [col.replace("\ufeff", "").strip() for col in table.column_names]
    table = table.rename_columns(clean_names)
    
    # Add ingestion audit metadata
    now_utc = datetime.now(timezone.utc).isoformat()
    ingested_col = pa.array([now_utc] * len(table), pa.string())
    table = table.append_column("ingested_at", ingested_col)

    sink = io.BytesIO()
    pq.write_table(table, sink, compression="snappy")
    parquet_bytes = sink.getvalue()

    s3_key = f"bronze/reference/{name}/{name}_snapshot_{datetime.now(timezone.utc).strftime('%Y%m%d')}.parquet"
    logger.info(f"Uploading {len(table)} rows ({len(parquet_bytes)/1024:.2f} KB) to s3://{S3_BUCKET}/{s3_key}")
    
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=parquet_bytes,
        ContentType="application/vnd.apache.parquet",
    )
    logger.info(f"Successfully landed reference dataset '{name}' in S3 Bronze.")


def main():
    workspace_root = Path(__file__).resolve().parent.parent.parent
    s3 = boto3.client("s3", region_name=AWS_REGION)

    for name, filename in REFERENCE_SETS.items():
        ingest_reference_dataset(name, filename, workspace_root, s3)


if __name__ == "__main__":
    main()
