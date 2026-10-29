"""End-to-End Pipeline Verification and Data Quality Test Suite.
Validates AWS components: API health, CSV integrity, S3 Bronze storage (.json.gz), and DynamoDB watermarks.
Clearly separates local integration assertions from live AWS infrastructure probes.
"""

from __future__ import annotations

import gzip
import io
import json
import os
from pathlib import Path
from typing import Any, Dict, List
import boto3
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
import httpx
import pyarrow.csv as pcsv
import pyarrow.parquet as pq
import pytest

AWS_REGION = os.environ.get("AWS_REGION", "eu-west-3")
S3_BUCKET = os.environ.get("S3_BUCKET", "sic-pipeline-storage")
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "ingestion_watermarks")


def get_api_base_url() -> str:
    """Resolve API base URL from env, ECS discovery, or default fallback."""
    if os.environ.get("API_BASE_URL"):
        return os.environ["API_BASE_URL"].rstrip("/")
    try:
        ecs = boto3.client("ecs", region_name=AWS_REGION)
        ec2 = boto3.client("ec2", region_name=AWS_REGION)
        tasks = ecs.list_tasks(cluster="synthetic-data-cluster", serviceName="synthetic-data-service")["taskArns"]
        if tasks:
            desc = ecs.describe_tasks(cluster="synthetic-data-cluster", tasks=tasks)
            for a in desc["tasks"][0].get("attachments", []):
                if a.get("type") == "ElasticNetworkInterface":
                    for d in a.get("details", []):
                        if d.get("name") == "networkInterfaceId":
                            eni = ec2.describe_network_interfaces(NetworkInterfaceIds=[d["value"]])["NetworkInterfaces"][0]
                            pub_ip = eni.get("Association", {}).get("PublicIp")
                            if pub_ip:
                                return f"http://{pub_ip}:8000"
    except Exception:
        pass
    return "http://15.188.246.109:8000"


API_BASE_URL = get_api_base_url()


def check_api_reachable(base_url: str, timeout: float = 2.0) -> bool:
    """Quickly probe API reachability without blocking test suites when network ingress is restricted."""
    try:
        with httpx.Client(timeout=timeout) as client:
            resp = client.get(f"{base_url}/health")
            return resp.status_code == 200
    except Exception:
        return False


@pytest.fixture(scope="module")
def s3_client():
    """Provide authenticated S3 client or skip if AWS credentials / network are unavailable."""
    try:
        client = boto3.client("s3", region_name=AWS_REGION)
        client.head_bucket(Bucket=S3_BUCKET)
        return client
    except (NoCredentialsError, ClientError, BotoCoreError) as e:
        pytest.skip(f"AWS S3 bucket '{S3_BUCKET}' is not accessible in this environment: {e}")


@pytest.fixture(scope="module")
def dynamo_client():
    """Provide authenticated DynamoDB client or skip if AWS credentials / table are unavailable."""
    try:
        client = boto3.client("dynamodb", region_name=AWS_REGION)
        client.describe_table(TableName=DYNAMODB_TABLE)
        return client
    except (NoCredentialsError, ClientError, BotoCoreError) as e:
        pytest.skip(f"AWS DynamoDB table '{DYNAMODB_TABLE}' is not accessible in this environment: {e}")


@pytest.mark.integration
class TestPipelineEndToEnd:
    """End-to-end integration tests verifying local contracts and live AWS pipeline health."""

    # --------------------------------------------------------------------------
    # Live AWS API Probes (Requires VPC network access or local server)
    # --------------------------------------------------------------------------

    @pytest.mark.live_aws
    def test_01_api_health_probe(self):
        """Verify Source A FastAPI server and database connection are healthy.

        Skips gracefully if running from outside the private AWS VPC where port 8000 ingress is restricted.
        """
        if not check_api_reachable(API_BASE_URL, timeout=2.0):
            pytest.skip(
                f"API endpoint '{API_BASE_URL}' is unreachable from this network "
                "(ECS service is isolated in private VPC subnets with port 8000 ingress restricted). "
                "Set API_BASE_URL or run inside VPC to execute live health probe."
            )

        url = f"{API_BASE_URL}/health"
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(url)
            assert resp.status_code == 200, f"Health check failed: {resp.text}"
            data = resp.json()
            assert data["status"] == "ok"
            assert data["database"]["status"] == "healthy"
            assert data["database"]["database"] == "postgresql"

    @pytest.mark.live_aws
    def test_02_generator_output_contracts(self):
        """Verify API serves properly contracted, watermarked transactions.

        Skips gracefully if running from outside the private AWS VPC.
        """
        if not check_api_reachable(API_BASE_URL, timeout=2.0):
            pytest.skip(
                f"API endpoint '{API_BASE_URL}' is unreachable from this network "
                "(ECS service is isolated in private VPC subnets with port 8000 ingress restricted). "
                "Set API_BASE_URL or run inside VPC to execute live generator probe."
            )

        url = f"{API_BASE_URL}/api/v1/transactions?limit=10&order=asc"
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(url)
            assert resp.status_code == 200
            data = resp.json()
            items = data.get("items", [])
            assert len(items) > 0, "No transactions returned from API"

            sample = items[0]
            mandatory = ["id", "transaction_id", "user_id", "card_index", "amount", "created_at"]
            for field in mandatory:
                assert field in sample, f"Missing field {field} in transaction output"

            ids = [it["id"] for it in items]
            assert ids == sorted(ids), "Returned transactions are not monotonically ordered"

    # --------------------------------------------------------------------------
    # Local Reference Data Quality Tests (Runnable offline anywhere)
    # --------------------------------------------------------------------------

    def test_03_reference_csv_schemas(self):
        """Verify local reference CSVs match expected schema and row counts."""
        root = Path(__file__).resolve().parent.parent.parent
        data_dir = root / "data"

        users_table = pcsv.read_csv(data_dir / "sd254_users.csv")
        assert len(users_table) == 2000, f"Expected 2000 users, found {len(users_table)}"
        assert "Person" in users_table.column_names
        assert "Current Age" in users_table.column_names

        cards_table = pcsv.read_csv(data_dir / "sd254_cards.csv")
        assert len(cards_table) == 6146, f"Expected 6146 cards, found {len(cards_table)}"
        assert "CARD INDEX" in cards_table.column_names

        mcc_table = pcsv.read_csv(data_dir / "merchant_category_codes.csv")
        assert len(mcc_table) == 914, f"Expected 914 MCCs, found {len(mcc_table)}"

        city_table = pcsv.read_csv(data_dir / "city_reference.csv")
        assert len(city_table) == 1433, f"Expected 1433 cities, found {len(city_table)}"

    # --------------------------------------------------------------------------
    # Live AWS Storage & State Probes (S3 Bronze Lakehouse & DynamoDB)
    # --------------------------------------------------------------------------

    @pytest.mark.live_aws
    def test_04_s3_bronze_storage(self, s3_client):
        """Verify transaction records exist in S3 Bronze storage and conform to sink schema.

        Aiven S3 Sink writes streaming transaction partitions as compressed JSON-lines (.json.gz).
        Batch extractors write partitioned Parquet. Both valid formats are verified accordingly.
        """
        resp = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix="bronze/transactions/",
        )
        contents = resp.get("Contents", [])
        assert len(contents) > 0, "No transaction files found in S3 Bronze storage"

        # Read back the most recent object
        latest_obj = sorted(contents, key=lambda x: x["LastModified"])[-1]
        obj_key = latest_obj["Key"]
        obj_resp = s3_client.get_object(Bucket=S3_BUCKET, Key=obj_key)
        raw_bytes = obj_resp["Body"].read()

        if obj_key.endswith(".json.gz") or obj_key.endswith(".gz"):
            # Decompress gzip and parse JSON-lines
            decompressed = gzip.decompress(raw_bytes).decode("utf-8")
            lines = [l for l in decompressed.strip().split("\n") if l]
            assert len(lines) > 0, f"Read empty JSON-lines file from S3 Bronze: {obj_key}"

            sample_record = json.loads(lines[0])
            # Verify Kafka Connect envelope fields
            assert "offset" in sample_record, f"Missing 'offset' in S3 Bronze record: {sample_record}"
            assert "value" in sample_record, f"Missing 'value' in S3 Bronze record: {sample_record}"

            val = sample_record["value"]
            assert "event_id" in val, f"Missing 'event_id' in event envelope: {val}"
            assert "event_type" in val, f"Missing 'event_type' in event envelope: {val}"

            # Verify transaction inner payload (supports both direct and generator envelopes)
            inner = val.get("payload") or val.get("data")
            assert inner is not None, f"Neither 'payload' nor 'data' found in event value: {val}"

            has_user = "User" in inner or "user_id" in inner
            has_amount = "Amount" in inner or "amount" in inner
            assert has_user, f"Missing user identifier in Bronze payload: {inner}"
            assert has_amount, f"Missing transaction amount in Bronze payload: {inner}"

        elif obj_key.endswith(".parquet"):
            # Parquet batch schema verification
            table = pq.read_table(io.BytesIO(raw_bytes))
            assert len(table) > 0, "Read empty Parquet table from S3"
            expected_cols = {"id", "transaction_id", "user_id", "card_index", "amount", "created_at"}
            actual_cols = set(table.column_names)
            assert expected_cols.issubset(actual_cols), (
                f"Missing expected columns in Parquet: {expected_cols - actual_cols}"
            )
        else:
            pytest.fail(f"Unexpected S3 Bronze storage format for {obj_key}: expected .json.gz")

    @pytest.mark.live_aws
    def test_05_dynamodb_watermark_consistency(self, dynamo_client):
        """Verify DynamoDB state store tracks high-water mark accurately."""
        resp = dynamo_client.get_item(
            TableName=DYNAMODB_TABLE,
            Key={"pipeline_id": {"S": "transactions_incremental_extractor"}},
            ConsistentRead=True,
        )
        item = resp.get("Item")
        assert item is not None, "No watermark item found in DynamoDB"
        assert "last_since_id" in item

        last_since_id = int(item["last_since_id"]["N"])
        total_records = int(item.get("total_records", {}).get("N", 0))
        assert last_since_id >= 200, f"Watermark did not advance as expected: {last_since_id}"
        assert total_records >= 200, f"Total records counter did not advance: {total_records}"

    @pytest.mark.live_aws
    def test_06_reference_datasets_landed_in_s3(self, s3_client):
        """Verify reference snapshots exist in S3 Bronze storage."""
        for ref_dataset in ["users", "cards", "merchants", "cities"]:
            resp = s3_client.list_objects_v2(
                Bucket=S3_BUCKET,
                Prefix=f"bronze/reference/{ref_dataset}/",
            )
            contents = resp.get("Contents", [])
            assert len(contents) > 0, f"Reference dataset {ref_dataset} not found in S3 Bronze"

