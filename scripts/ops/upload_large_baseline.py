#!/usr/bin/env python3
"""Multipart upload script for credit_card_transactions-ibm_v2.csv (2.35 GB) to S3."""

from __future__ import annotations

import os
from pathlib import Path
import sys
import time

import boto3
from boto3.s3.transfer import TransferConfig

BUCKET = os.environ.get("S3_BUCKET", "sic-pipeline-storage")
REGION = os.environ.get("AWS_REGION", "eu-west-3")
FILE_PATH = Path("credit_card_transactions-ibm_v2.csv")
S3_KEY = "raw/baseline/credit_card_transactions-ibm_v2.csv"


class ProgressPercentage:
    def __init__(self, filename: str, total_bytes: int):
        self._filename = filename
        self._total_bytes = total_bytes
        self._seen_so_far = 0
        self._start_time = time.time()
        self._last_print = 0

    def __call__(self, bytes_amount: int):
        self._seen_so_far += bytes_amount
        percentage = (self._seen_so_far / self._total_bytes) * 100
        now = time.time()
        elapsed = now - self._start_time
        # Print every 2 seconds or when done
        if now - self._last_print > 2.0 or self._seen_so_far >= self._total_bytes:
            speed_mb = (self._seen_so_far / (1024 * 1024)) / elapsed if elapsed > 0 else 0
            mb_uploaded = self._seen_so_far / (1024 * 1024)
            total_mb = self._total_bytes / (1024 * 1024)
            sys.stdout.write(
                f"\rUploading {self._filename}: {mb_uploaded:.1f}/{total_mb:.1f} MB "
                f"({percentage:.1f}%) at {speed_mb:.2f} MB/s... "
            )
            sys.stdout.flush()
            self._last_print = now


def main():
    if not FILE_PATH.exists():
        print(f"Error: {FILE_PATH} not found!")
        sys.exit(1)

    total_size = FILE_PATH.stat().st_size
    print(f"Starting multipart upload: {FILE_PATH.name} ({total_size / (1024*1024*1024):.2f} GB)")
    print(f"Target: s3://{BUCKET}/{S3_KEY}")

    s3 = boto3.client("s3", region_name=REGION)

    # 16 MB chunksize with 10 threads
    config = TransferConfig(
        multipart_threshold=16 * 1024 * 1024,
        max_concurrency=10,
        multipart_chunksize=16 * 1024 * 1024,
        use_threads=True,
    )

    progress = ProgressPercentage(FILE_PATH.name, total_size)
    start = time.time()

    s3.upload_file(
        str(FILE_PATH),
        BUCKET,
        S3_KEY,
        Config=config,
        Callback=progress,
    )

    duration = time.time() - start
    print(f"\nSuccessfully uploaded {FILE_PATH.name} in {duration:.1f}s ({duration/60:.2f} min)!")


if __name__ == "__main__":
    main()
