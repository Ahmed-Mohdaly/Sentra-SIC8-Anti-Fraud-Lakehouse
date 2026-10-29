#!/usr/bin/env python3
"""Cloud-Native S3 Baseline Sync: Upload and synchronize baseline data directly to S3 Lakehouse.

Enables the entire platform and ML training pipelines to operate 100% cloud-native
without depending on local files on the host machine.
"""

from __future__ import annotations

import argparse
import io
import logging
import os
from pathlib import Path
import sys
from typing import Dict, List, Optional

import boto3
import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

# Add src to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("s3_baseline_sync")

DEFAULT_BUCKET = os.environ.get("S3_BUCKET", "sic-pipeline-storage")
DEFAULT_REGION = os.environ.get("AWS_REGION", "eu-west-3")

BASELINE_FILES = [
    "sd254_users.csv",
    "sd254_cards.csv",
    "merchant_category_codes.csv",
    "city_reference.csv",
    "User0_credit_card_transactions.csv",
]


def sync_baseline_files_to_s3(
    data_dir: Path,
    bucket_name: str = DEFAULT_BUCKET,
    prefix: str = "raw/baseline",
    s3_client: Optional[Any] = None,
) -> Dict[str, Any]:
    """Upload core baseline CSV datasets to S3 raw/baseline prefix."""
    s3 = s3_client or boto3.client("s3", region_name=DEFAULT_REGION)
    uploaded = []
    skipped = []

    for fname in BASELINE_FILES:
        local_path = data_dir / fname
        if not local_path.exists():
            if (data_dir / "data" / fname).exists():
                local_path = data_dir / "data" / fname
            else:
                logger.warning(f"File {fname} not found in {data_dir}. Skipping.")
                skipped.append(fname)
                continue

        s3_key = f"{prefix.rstrip('/')}/{fname}"
        file_size_kb = local_path.stat().st_size / 1024.0
        logger.info(f"Uploading {fname} ({file_size_kb:.1f} KB) -> s3://{bucket_name}/{s3_key}...")
        s3.upload_file(str(local_path), bucket_name, s3_key)
        uploaded.append({"file": fname, "s3_key": s3_key, "size_kb": round(file_size_kb, 1)})

    return {
        "bucket": bucket_name,
        "prefix": prefix,
        "uploaded": uploaded,
        "skipped": skipped,
    }


def main():
    parser = argparse.ArgumentParser(description="Upload baseline datasets to Amazon S3.")
    parser.add_argument("--data-dir", type=str, default="data", help="Local directory containing baseline CSVs")
    parser.add_argument("--bucket", type=str, default=DEFAULT_BUCKET, help="Target S3 Bucket")
    parser.add_argument("--prefix", type=str, default="raw/baseline", help="S3 object prefix")
    args = parser.parse_args()

    data_dir = Path(args.data_dir).resolve()
    logger.info(f"Syncing baseline datasets from {data_dir} to s3://{args.bucket}/{args.prefix}")

    res = sync_baseline_files_to_s3(data_dir=data_dir, bucket_name=args.bucket, prefix=args.prefix)

    print("\n" + "=" * 70)
    print("S3 BASELINE SYNC REPORT")
    print("=" * 70)
    print(f"Target Bucket:   s3://{res['bucket']}/{res['prefix']}/")
    print(f"Files Uploaded:  {len(res['uploaded'])}")
    for item in res["uploaded"]:
        print(f"  ✓ {item['file']} ({item['size_kb']} KB) -> {item['s3_key']}")
    if res["skipped"]:
        print(f"Files Skipped:   {len(res['skipped'])}")
        for f in res["skipped"]:
            print(f"  ✗ {f}")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
