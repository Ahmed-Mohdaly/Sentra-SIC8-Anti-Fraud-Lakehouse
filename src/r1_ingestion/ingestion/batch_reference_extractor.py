#!/usr/bin/env python3
"""Scheduled Batch Extractor for PostgreSQL Reference Tables (MCC and Cities).

Extracts full reference snapshots from PostgreSQL RDS, validates record counts
and schema consistency, and persists versioned, partitioned Parquet datasets
directly to Amazon S3 Bronze lakehouse layer with lineage metadata.
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text

# ------------------------------------------------------------------------------
# Structured Logging
# ------------------------------------------------------------------------------
class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if hasattr(record, "structured_data") and isinstance(record.structured_data, dict):
            payload.update(record.structured_data)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload)

logger = logging.getLogger("batch_reference_extractor")
logger.setLevel(logging.INFO)
if not logger.handlers:
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(JSONFormatter())
    logger.addHandler(ch)

# ------------------------------------------------------------------------------
# Schemas for Arrow Parquet Output
# ------------------------------------------------------------------------------
MCC_ARROW_SCHEMA = pa.schema([
    ("mcc", pa.string()),
    ("description", pa.string()),
    ("category", pa.string()),
    ("valid_payment_brands", pa.string()),
    ("required_abbreviation", pa.string()),
    ("extraction_timestamp", pa.string()),
    ("source_system", pa.string()),
])

CITY_ARROW_SCHEMA = pa.schema([
    ("city_id", pa.int64()),
    ("city_name", pa.string()),
    ("state_code", pa.string()),
    ("country_code", pa.string()),
    ("latitude", pa.float64()),
    ("longitude", pa.float64()),
    ("coordinate_source", pa.string()),
    ("coordinate_precision", pa.string()),
    ("extraction_timestamp", pa.string()),
    ("source_system", pa.string()),
])


def get_database_url() -> str:
    """Resolve database URL from environment or AWS Secrets Manager."""
    url = os.environ.get("DATABASE_URL")
    if url:
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql://", 1)
        return url

    # Fallback to Secrets Manager
    secret_id = os.environ.get("SECRET_ID", "synthetic-data-provider/database")
    region = os.environ.get("AWS_REGION", "eu-west-3")
    try:
        sm = boto3.client("secretsmanager", region_name=region)
        val = sm.get_secret_value(SecretId=secret_id)["SecretString"]
        secret_dict = json.loads(val)
        db_url = secret_dict.get("DATABASE_URL", "")
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
        return db_url
    except Exception as e:
        logger.error(f"Failed to fetch database credentials from Secrets Manager: {e}")
        raise RuntimeError(f"Database connection string unavailable: {e}")


class BatchReferenceExtractor:
    """Extracts, validates, and uploads reference snapshots to S3 Bronze."""

    def __init__(
        self,
        db_url: str,
        s3_bucket: str,
        region: str = "eu-west-3",
        s3_client: Optional[Any] = None,
    ):
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
        self.db_url = db_url
        self.s3_bucket = s3_bucket
        self.region = region
        self.s3 = s3_client or boto3.client("s3", region_name=region)
        self.engine = create_engine(db_url, pool_pre_ping=True)

    def extract_mcc(self) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """Extract and validate Merchant Category Codes reference dataset."""
        query = text("SELECT mcc, description, category, valid_payment_brands, required_abbreviation FROM merchant_category_codes ORDER BY mcc ASC;")
        now_iso = datetime.now(timezone.utc).isoformat()
        rows: List[Dict[str, Any]] = []

        with self.engine.connect() as conn:
            result = conn.execute(query)
            for r in result:
                rows.append({
                    "mcc": str(r[0]),
                    "description": str(r[1] or ""),
                    "category": str(r[2] or ""),
                    "valid_payment_brands": str(r[3] or ""),
                    "required_abbreviation": str(r[4] or ""),
                    "extraction_timestamp": now_iso,
                    "source_system": "RDS_POSTGRESQL",
                })

        if not rows:
            raise ValueError("Extraction validation failed: merchant_category_codes table is empty.")

        stats = {
            "table": "merchant_category_codes",
            "record_count": len(rows),
            "sample_mcc": rows[0]["mcc"],
            "extracted_at": now_iso,
        }
        logger.info(
            f"Extracted {len(rows)} records from merchant_category_codes",
            extra={"structured_data": stats},
        )
        return rows, stats

    def extract_cities(self) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """Extract and validate City Reference dataset."""
        query = text("SELECT city_id, city_name, state_code, country_code, latitude, longitude, coordinate_source, coordinate_precision FROM city_reference ORDER BY city_id ASC;")
        now_iso = datetime.now(timezone.utc).isoformat()
        rows: List[Dict[str, Any]] = []

        with self.engine.connect() as conn:
            result = conn.execute(query)
            for r in result:
                rows.append({
                    "city_id": int(r[0]),
                    "city_name": str(r[1] or ""),
                    "state_code": str(r[2] or ""),
                    "country_code": str(r[3] or "USA"),
                    "latitude": float(r[4]) if r[4] is not None else 0.0,
                    "longitude": float(r[5]) if r[5] is not None else 0.0,
                    "coordinate_source": str(r[6] or "Gazetteer"),
                    "coordinate_precision": str(r[7] or "approximate_city_centroid"),
                    "extraction_timestamp": now_iso,
                    "source_system": "RDS_POSTGRESQL",
                })

        if not rows:
            raise ValueError("Extraction validation failed: city_reference table is empty.")

        stats = {
            "table": "city_reference",
            "record_count": len(rows),
            "min_city_id": min(r["city_id"] for r in rows),
            "max_city_id": max(r["city_id"] for r in rows),
            "extracted_at": now_iso,
        }
        logger.info(
            f"Extracted {len(rows)} records from city_reference",
            extra={"structured_data": stats},
        )
        return rows, stats

    def upload_dataset_to_s3(
        self,
        dataset_name: str,
        rows: List[Dict[str, Any]],
        schema: pa.Schema,
        stats: Dict[str, Any],
    ) -> str:
        """Serialize rows to Parquet and upload to S3 Bronze prefix."""
        now = datetime.now(timezone.utc)
        date_str = now.strftime("%Y-%m-%d")
        ts_str = now.strftime("%Y%m%d_%H%M%S")

        # Convert to Arrow Table
        table = pa.Table.from_pylist(rows, schema=schema)
        parquet_buf = io.BytesIO()
        pq.write_table(table, parquet_buf, compression="snappy")
        parquet_bytes = parquet_buf.getvalue()

        s3_key = f"bronze/{dataset_name}/snapshot_date={date_str}/{dataset_name}_snapshot_{ts_str}.parquet"
        self.s3.put_object(
            Bucket=self.s3_bucket,
            Key=s3_key,
            Body=parquet_bytes,
            ContentType="application/octet-stream",
            Metadata={
                "record_count": str(len(rows)),
                "extraction_timestamp": stats.get("extracted_at", ""),
                "source_system": "RDS_POSTGRESQL",
            },
        )
        logger.info(
            f"Persisted {len(rows)} records to s3://{self.s3_bucket}/{s3_key}",
            extra={"structured_data": {"s3_key": s3_key, "size_bytes": len(parquet_bytes), "record_count": len(rows)}},
        )
        return s3_key

    def run_all(self) -> Dict[str, Any]:
        """Execute extraction and upload for both reference tables."""
        results: Dict[str, Any] = {}

        # 1. MCC
        mcc_rows, mcc_stats = self.extract_mcc()
        mcc_key = self.upload_dataset_to_s3("mcc", mcc_rows, MCC_ARROW_SCHEMA, mcc_stats)
        results["mcc"] = {"s3_key": mcc_key, "stats": mcc_stats}

        # 2. Cities
        city_rows, city_stats = self.extract_cities()
        city_key = self.upload_dataset_to_s3("cities", city_rows, CITY_ARROW_SCHEMA, city_stats)
        results["cities"] = {"s3_key": city_key, "stats": city_stats}

        return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Batch Extractor for PostgreSQL Reference Data")
    parser.add_argument("--table", choices=["mcc", "cities", "all"], default="all", help="Target reference table")
    parser.add_argument("--bucket", default=os.environ.get("S3_BUCKET", "sic-pipeline-storage"), help="Target S3 Bucket")
    parser.add_argument("--region", default=os.environ.get("AWS_REGION", "eu-west-3"), help="AWS Region")
    args = parser.parse_args()

    db_url = get_database_url()
    extractor = BatchReferenceExtractor(db_url=db_url, s3_bucket=args.bucket, region=args.region)

    try:
        if args.table == "all":
            results = extractor.run_all()
            print(json.dumps({"status": "SUCCESS", "results": results}, indent=2))
        elif args.table == "mcc":
            rows, stats = extractor.extract_mcc()
            key = extractor.upload_dataset_to_s3("mcc", rows, MCC_ARROW_SCHEMA, stats)
            print(json.dumps({"status": "SUCCESS", "dataset": "mcc", "s3_key": key, "stats": stats}, indent=2))
        elif args.table == "cities":
            rows, stats = extractor.extract_cities()
            key = extractor.upload_dataset_to_s3("cities", rows, CITY_ARROW_SCHEMA, stats)
            print(json.dumps({"status": "SUCCESS", "dataset": "cities", "s3_key": key, "stats": stats}, indent=2))
    except Exception as e:
        logger.error(f"Batch reference extraction failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
