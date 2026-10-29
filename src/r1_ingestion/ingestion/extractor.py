#!/usr/bin/env python3
"""Production-Grade Incremental Ingestion Extractor
Architecture: Watermark -> API Keyset Extract -> Validation Gate -> S3 Parquet Bronze -> DynamoDB Checkpoint
Guarantees: At-Least-Once delivery with idempotent deterministic S3 file keys and quarantine isolation.
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import os
import sys
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import boto3
from botocore.exceptions import ClientError
import httpx
import pyarrow as pa
import pyarrow.parquet as pq

# ------------------------------------------------------------------------------
# Structured Logging Configuration
# ------------------------------------------------------------------------------

class JSONFormatter(logging.Formatter):
    """Format log records as structured JSON without exposing secrets."""
    def format(self, record: logging.LogRecord) -> str:
        log_payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if hasattr(record, "structured_data") and isinstance(record.structured_data, dict):
            log_payload.update(record.structured_data)
        if record.exc_info:
            log_payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_payload)

logger = logging.getLogger("ingestion_extractor")
logger.setLevel(logging.INFO)
if not logger.handlers:
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(JSONFormatter())
    logger.addHandler(ch)

# ------------------------------------------------------------------------------
# Default Environment Configuration
# ------------------------------------------------------------------------------
DEFAULT_API_URL = os.environ.get("API_BASE_URL", "auto")
DEFAULT_S3_BUCKET = os.environ.get("S3_BUCKET", "sic-pipeline-storage")
DEFAULT_DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "ingestion_watermarks")
DEFAULT_AWS_REGION = os.environ.get("AWS_REGION", "eu-west-3")
PIPELINE_ID = "transactions_incremental_extractor"


def resolve_service_private_url(
    cluster_name: str = "synthetic-data-cluster",
    service_name: str = "synthetic-data-service",
    region_name: str = DEFAULT_AWS_REGION,
    port: int = 8000,
) -> Optional[str]:
    """Dynamically discover the active private IP of the API ECS Fargate task."""
    try:
        ecs = boto3.client("ecs", region_name=region_name)
        tasks_resp = ecs.list_tasks(cluster=cluster_name, serviceName=service_name, desiredStatus="RUNNING")
        task_arns = tasks_resp.get("taskArns", [])
        if not task_arns:
            logger.warning("No running tasks found for service %s in cluster %s", service_name, cluster_name)
            return None
        desc_resp = ecs.describe_tasks(cluster=cluster_name, tasks=task_arns)
        for task in desc_resp.get("tasks", []):
            if task.get("lastStatus") == "RUNNING":
                for container in task.get("containers", []):
                    for ni in container.get("networkInterfaces", []):
                        ip = ni.get("privateIpv4Address")
                        if ip:
                            logger.info(
                                "Discovered active ECS service private IP",
                                extra={"structured_data": {"ip": ip, "port": port}}
                            )
                            return f"http://{ip}:{port}"
    except Exception as e:
        logger.warning(f"Failed to auto-discover ECS service private IP: {e}")
    return None


# ------------------------------------------------------------------------------
# Strict Apache Arrow Schema Definition for Lakehouse Bronze Parquet
# ------------------------------------------------------------------------------
PARQUET_SCHEMA = pa.schema([
    ("id", pa.int64()),
    ("transaction_id", pa.string()),
    ("event_id", pa.string()),
    ("event_type", pa.string()),
    ("user_id", pa.int64()),
    ("card_index", pa.int32()),
    ("amount", pa.float64()),
    ("amount_raw", pa.string()),
    ("use_chip", pa.string()),
    ("transaction_entry_method", pa.string()),
    ("merchant_name", pa.string()),
    ("merchant_city", pa.string()),
    ("merchant_state", pa.string()),
    ("merchant_latitude", pa.float64()),
    ("merchant_longitude", pa.float64()),
    ("zip", pa.string()),
    ("mcc", pa.int32()),
    ("errors", pa.string()),
    ("is_fraud", pa.string()),
    ("scenario", pa.string()),
    ("source_system", pa.string()),
    ("run_id", pa.string()),
    ("created_at", pa.string()),
    ("ingested_at", pa.string()),
])

# ------------------------------------------------------------------------------
# Data Validation & Quarantine Engine
# ------------------------------------------------------------------------------
@dataclass
class ValidationResult:
    valid_records: List[Dict[str, Any]]
    quarantined_records: List[Dict[str, Any]]
    total_received: int
    duplicates_dropped: int


class DataQualityGate:
    """Validates raw records, filters in-batch duplicates, and routes corrupted rows to quarantine."""

    REQUIRED_FIELDS = {"id", "transaction_id", "user_id", "card_index", "amount", "created_at"}

    @classmethod
    def validate_batch(cls, raw_items: List[Dict[str, Any]]) -> ValidationResult:
        valid: List[Dict[str, Any]] = []
        quarantine: List[Dict[str, Any]] = []
        seen_ids: Set[int] = set()
        duplicates_count = 0

        for item in raw_items:
            # 1. Check mandatory fields
            missing = [f for f in cls.REQUIRED_FIELDS if item.get(f) is None]
            if missing:
                quarantine.append({
                    "raw_record": item,
                    "rejection_reason": f"Missing mandatory field(s): {', '.join(missing)}",
                    "quarantined_at": datetime.now(timezone.utc).isoformat(),
                })
                continue

            record_id = item["id"]

            # 2. Duplicate detection within the same incoming batch
            if record_id in seen_ids:
                duplicates_count += 1
                quarantine.append({
                    "raw_record": item,
                    "rejection_reason": f"Duplicate sequence ID {record_id} observed in batch",
                    "quarantined_at": datetime.now(timezone.utc).isoformat(),
                })
                continue
            seen_ids.add(record_id)

            # 3. Numeric amount validation
            try:
                amt = float(item["amount"])
                # Financial invariant: non-refund transactions must not have negative amounts
                scenario_str = str(item.get("scenario", "")).lower()
                if amt < 0 and not scenario_str.startswith("refund"):
                    quarantine.append({
                        "raw_record": item,
                        "rejection_reason": f"Negative amount {amt} on non-refund scenario '{scenario_str}'",
                        "quarantined_at": datetime.now(timezone.utc).isoformat(),
                    })
                    continue
            except (ValueError, TypeError):
                quarantine.append({
                    "raw_record": item,
                    "rejection_reason": f"Invalid numeric amount representation: {item.get('amount')}",
                    "quarantined_at": datetime.now(timezone.utc).isoformat(),
                })
                continue

            # 4. User and Card Index non-negativity
            if int(item["user_id"]) < 0 or int(item["card_index"]) < 0:
                quarantine.append({
                    "raw_record": item,
                    "rejection_reason": f"Invalid negative user_id ({item['user_id']}) or card_index ({item['card_index']})",
                    "quarantined_at": datetime.now(timezone.utc).isoformat(),
                })
                continue

            valid.append(item)

        return ValidationResult(
            valid_records=valid,
            quarantined_records=quarantine,
            total_received=len(raw_items),
            duplicates_dropped=duplicates_count,
        )


# ------------------------------------------------------------------------------
# Checkpoint & Watermark State Store (DynamoDB)
# ------------------------------------------------------------------------------
class IngestionStateStore:
    """Atomic, condition-guarded state store backed by Amazon DynamoDB."""

    def __init__(self, table_name: str, region_name: str, dynamodb_client: Optional[Any] = None):
        self.table_name = table_name
        self.region_name = region_name
        self.client = dynamodb_client or boto3.client("dynamodb", region_name=region_name)

    def get_watermark(self, pipeline_id: str = PIPELINE_ID) -> int:
        """Fetch the current committed monotonic since_id sequence."""
        try:
            resp = self.client.get_item(
                TableName=self.table_name,
                Key={"pipeline_id": {"S": pipeline_id}},
                ConsistentRead=True,
            )
            item = resp.get("Item")
            if item and "last_since_id" in item:
                return int(item["last_since_id"]["N"])
        except ClientError as e:
            logger.error(f"Failed to read watermark from DynamoDB: {e}", extra={"structured_data": {"error": str(e)}})
            raise
        return 0

    def commit_watermark(
        self,
        pipeline_id: str,
        new_since_id: int,
        records_count: int,
        s3_path: str,
        expected_prev_since_id: Optional[int] = None,
    ) -> None:
        """Atomically commit the high-water mark with conditional check against regressive race conditions."""
        now_utc = datetime.now(timezone.utc).isoformat()
        
        # Guard expression: strictly prevent regressive updates where an older task overwrites a newer watermark
        update_expr = (
            "SET last_since_id = :sid, last_ingested_at = :ts, "
            "last_batch_records = :cnt, last_s3_path = :s3, "
            "total_records = if_not_exists(total_records, :zero) + :cnt"
        )
        expr_values = {
            ":sid": {"N": str(new_since_id)},
            ":ts": {"S": now_utc},
            ":cnt": {"N": str(records_count)},
            ":s3": {"S": s3_path},
            ":zero": {"N": "0"},
        }

        condition_expr = "attribute_not_exists(last_since_id) OR last_since_id <= :sid"

        self.client.update_item(
            TableName=self.table_name,
            Key={"pipeline_id": {"S": pipeline_id}},
            UpdateExpression=update_expr,
            ConditionExpression=condition_expr,
            ExpressionAttributeValues=expr_values,
        )


# ------------------------------------------------------------------------------
# Core Extractor Service
# ------------------------------------------------------------------------------
class TransactionExtractor:
    """Pulls incremental transactions from Source A, writes Parquet Bronze, and commits watermarks."""

    def __init__(
        self,
        api_base_url: str = DEFAULT_API_URL,
        s3_bucket: str = DEFAULT_S3_BUCKET,
        table_name: str = DEFAULT_DYNAMODB_TABLE,
        region_name: str = DEFAULT_AWS_REGION,
        s3_client: Optional[Any] = None,
        dynamodb_client: Optional[Any] = None,
        http_client: Optional[httpx.Client] = None,
    ):
        self.region_name = region_name
        self.s3_bucket = s3_bucket
        self.s3 = s3_client or boto3.client("s3", region_name=region_name)
        self.state_store = IngestionStateStore(
            table_name=table_name,
            region_name=region_name,
            dynamodb_client=dynamodb_client,
        )
        self._http_client = http_client

        # Handle automatic service discovery
        if not api_base_url or api_base_url.lower() == "auto":
            discovered = resolve_service_private_url(region_name=region_name)
            if discovered:
                self.api_base_url = discovered
            else:
                fallback = os.environ.get("API_FALLBACK_URL", "http://172.31.9.107:8000")
                logger.info(f"Using fallback API URL: {fallback}")
                self.api_base_url = fallback
        else:
            self.api_base_url = api_base_url.rstrip("/")

    def fetch_page(self, since_id: int, limit: int = 1000) -> Dict[str, Any]:
        params = {"since_id": since_id, "limit": limit, "order": "asc"}
        client = self._http_client or httpx.Client(timeout=30.0)
        try:
            url = f"{self.api_base_url}/api/v1/transactions"
            try:
                resp = client.get(url, params=params)
                resp.raise_for_status()
                return resp.json()
            except (httpx.ConnectError, httpx.TimeoutException) as conn_err:
                logger.warning(f"Connection to {url} failed: {conn_err}. Attempting dynamic ECS discovery...")
                new_url = resolve_service_private_url(region_name=self.region_name)
                if new_url and new_url != self.api_base_url:
                    self.api_base_url = new_url
                    retry_url = f"{self.api_base_url}/api/v1/transactions"
                    logger.info(f"Retrying extraction request with re-discovered URL: {retry_url}")
                    resp = client.get(retry_url, params=params)
                    resp.raise_for_status()
                    return resp.json()
                raise
        finally:
            if self._http_client is None:
                client.close()

    def serialize_to_parquet(self, items: List[Dict[str, Any]]) -> bytes:
        """Convert validated items into strongly-typed Apache Parquet bytes with Snappy compression."""
        records = []
        ingested_at = datetime.now(timezone.utc).isoformat()

        for it in items:
            m_lat = float(it["merchant_latitude"]) if it.get("merchant_latitude") is not None else None
            m_lon = float(it["merchant_longitude"]) if it.get("merchant_longitude") is not None else None
            mcc_val = int(it["mcc"]) if it.get("mcc") is not None else None

            use_chip_val = str(it.get("use_chip", "Chip Transaction"))
            entry_meth = it.get("transaction_entry_method")
            if not entry_meth or entry_meth == "unknown":
                from synthetic_streaming.classification_and_geo import sample_entry_method
                entry_meth = sample_entry_method(
                    use_chip=use_chip_val,
                    source_system=str(it.get("source_system", "API")),
                    mcc=mcc_val or 5411,
                    scenario=str(it.get("scenario", "normal_spend")),
                )

            rec = {
                "id": int(it["id"]),
                "transaction_id": str(it["transaction_id"]),
                "event_id": str(it.get("event_id", "")),
                "event_type": str(it.get("event_type", "transaction.created")),
                "user_id": int(it["user_id"]),
                "card_index": int(it["card_index"]),
                "amount": float(it["amount"]),
                "amount_raw": str(it.get("amount_raw", f"${it['amount']}")),
                "use_chip": use_chip_val,
                "transaction_entry_method": str(entry_meth),
                "merchant_name": str(it.get("merchant_name", "")),
                "merchant_city": str(it.get("merchant_city", "")),
                "merchant_state": str(it.get("merchant_state", "")),
                "merchant_latitude": m_lat,
                "merchant_longitude": m_lon,
                "zip": str(it.get("zip", "")) if it.get("zip") is not None else None,
                "mcc": mcc_val,
                "errors": str(it.get("errors", "")) if it.get("errors") is not None else None,
                "is_fraud": str(it.get("is_fraud", "No")),
                "scenario": str(it.get("scenario", "normal_spend")),
                "source_system": str(it.get("source_system", "API")),
                "run_id": str(it.get("run_id", "")),
                "created_at": str(it["created_at"]),
                "ingested_at": ingested_at,
            }
            records.append(rec)

        table = pa.Table.from_pylist(records, schema=PARQUET_SCHEMA)
        sink = io.BytesIO()
        pq.write_table(table, sink, compression="snappy")
        return sink.getvalue()

    def run_ingestion_cycle(self, limit: int = 1000, run_id: Optional[str] = None) -> Dict[str, Any]:
        """Execute one complete atomic, idempotent extraction and landing cycle."""
        start_time = time.time()
        run_id = run_id or f"extract-{int(start_time)}-{uuid.uuid4().hex[:6]}"
        
        # 1. Watermark Read
        watermark_before = self.state_store.get_watermark(PIPELINE_ID)
        
        logger.info(
            "Starting incremental extraction cycle",
            extra={"structured_data": {
                "run_id": run_id,
                "watermark_before": watermark_before,
                "batch_limit": limit,
                "status": "STARTED"
            }}
        )

        # 2. API Keyset Extraction
        page = self.fetch_page(since_id=watermark_before, limit=limit)
        raw_items = page.get("items", [])
        records_fetched = len(raw_items)

        if records_fetched == 0:
            duration = round(time.time() - start_time, 3)
            logger.info(
                "No new transactions found. Pipeline idle.",
                extra={"structured_data": {
                    "run_id": run_id,
                    "watermark_before": watermark_before,
                    "watermark_after": watermark_before,
                    "records_fetched": 0,
                    "records_written": 0,
                    "duration_seconds": duration,
                    "status": "IDLE"
                }}
            )
            return {
                "run_id": run_id,
                "status": "IDLE",
                "watermark_before": watermark_before,
                "watermark_after": watermark_before,
                "records_fetched": 0,
                "records_written": 0,
                "records_quarantined": 0,
                "duration_seconds": duration,
            }

        # 3. Data Quality Gate & Quarantine Filtering
        val_result = DataQualityGate.validate_batch(raw_items)
        valid_items = val_result.valid_records
        quarantine_items = val_result.quarantined_records
        records_validated = len(valid_items)
        records_quarantined = len(quarantine_items)

        now = datetime.now(timezone.utc)
        date_partition = f"year={now.year}/month={now.month:02d}/day={now.day:02d}"

        # 4. Handle Quarantine Upload (if any corrupted records observed)
        if records_quarantined > 0:
            quarantine_key = f"bronze/quarantine/transactions/{date_partition}/quarantine_{run_id}.jsonl"
            quarantine_bytes = "\n".join(json.dumps(q) for q in quarantine_items).encode("utf-8")
            self.s3.put_object(
                Bucket=self.s3_bucket,
                Key=quarantine_key,
                Body=quarantine_bytes,
                ContentType="application/x-ndjson",
            )
            logger.warning(
                f"Quarantined {records_quarantined} invalid records",
                extra={"structured_data": {
                    "run_id": run_id,
                    "quarantined_count": records_quarantined,
                    "quarantine_s3_key": quarantine_key
                }}
            )

        if records_validated == 0:
            duration = round(time.time() - start_time, 3)
            return {
                "run_id": run_id,
                "status": "ALL_QUARANTINED",
                "watermark_before": watermark_before,
                "watermark_after": watermark_before,
                "records_fetched": records_fetched,
                "records_written": 0,
                "records_quarantined": records_quarantined,
                "duration_seconds": duration,
            }

        # 5. Deterministic S3 Key Generation (Idempotent Retry Safety)
        first_id = valid_items[0]["id"]
        last_id = valid_items[-1]["id"]
        # Deterministic filename based purely on sequence boundaries, NOT timestamps
        deterministic_filename = f"transactions_batch_{first_id}_{last_id}.parquet"
        s3_key = f"bronze/transactions/{date_partition}/{deterministic_filename}"

        # 6. Parquet Serialization & Atomic Upload
        parquet_bytes = self.serialize_to_parquet(valid_items)
        file_size_kb = len(parquet_bytes) / 1024.0

        self.s3.put_object(
            Bucket=self.s3_bucket,
            Key=s3_key,
            Body=parquet_bytes,
            ContentType="application/vnd.apache.parquet",
        )

        # 7. Watermark Checkpoint Commit (Only after S3 write is confirmed)
        new_watermark = last_id
        self.state_store.commit_watermark(
            pipeline_id=PIPELINE_ID,
            new_since_id=new_watermark,
            records_count=records_validated,
            s3_path=f"s3://{self.s3_bucket}/{s3_key}",
        )

        duration = round(time.time() - start_time, 3)
        summary = {
            "run_id": run_id,
            "status": "SUCCESS",
            "watermark_before": watermark_before,
            "watermark_after": new_watermark,
            "records_fetched": records_fetched,
            "records_validated": records_validated,
            "records_quarantined": records_quarantined,
            "records_written": records_validated,
            "s3_path": f"s3://{self.s3_bucket}/{s3_key}",
            "file_size_kb": round(file_size_kb, 2),
            "duration_seconds": duration,
            "has_more": page.get("has_more", False),
        }

        logger.info("Ingestion cycle completed successfully", extra={"structured_data": summary})
        return summary


def main():
    parser = argparse.ArgumentParser(description="Reliable Incremental Transaction Extractor to S3 Bronze")
    parser.add_argument("--api-url", default=DEFAULT_API_URL, help="FastAPI Source Endpoint")
    parser.add_argument("--s3-bucket", default=DEFAULT_S3_BUCKET, help="Target S3 Bucket")
    parser.add_argument("--limit", type=int, default=500, help="Batch extraction limit")
    parser.add_argument("--loop", action="store_true", help="Run continuously with poll interval")
    parser.add_argument("--interval", type=int, default=15, help="Poll interval in seconds")
    args = parser.parse_args()

    extractor = TransactionExtractor(api_base_url=args.api_url, s3_bucket=args.s3_bucket)

    if args.loop:
        logger.info(f"Extractor running in continuous polling mode every {args.interval}s...")
        while True:
            try:
                extractor.run_ingestion_cycle(limit=args.limit)
            except Exception as e:
                logger.error(f"Ingestion loop failure: {e}", exc_info=True)
            time.sleep(args.interval)
    else:
        try:
            result = extractor.run_ingestion_cycle(limit=args.limit)
            print("\n" + json.dumps(result, indent=2))
            if result.get("status") in ("FAILED", "ERROR"):
                sys.exit(1)
        except Exception as e:
            logger.error(f"Ingestion cycle execution failed: {e}", exc_info=True)
            sys.exit(1)


if __name__ == "__main__":
    main()
