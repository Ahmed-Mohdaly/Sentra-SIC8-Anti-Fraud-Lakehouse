"""Comprehensive Unit Tests for Ingestion Extractor & DataQualityGate."""

from __future__ import annotations

import io
from typing import Any, Dict, List
from unittest.mock import MagicMock, call, patch
import pytest
import pyarrow.parquet as pq

from src.ingestion.extractor import (
    DataQualityGate,
    IngestionStateStore,
    TransactionExtractor,
    PARQUET_SCHEMA,
)


@pytest.fixture
def sample_raw_transactions() -> List[Dict[str, Any]]:
    return [
        {
            "id": 1,
            "transaction_id": "tx-1",
            "event_id": "evt-1",
            "event_type": "transaction.created",
            "user_id": 100,
            "card_index": 0,
            "amount": "45.50",
            "amount_raw": "$45.50",
            "use_chip": "Chip Transaction",
            "transaction_entry_method": "contact_chip",
            "merchant_name": "Store A",
            "merchant_city": "New York",
            "merchant_state": "NY",
            "merchant_latitude": 40.7128,
            "merchant_longitude": -74.006,
            "zip": "10001",
            "mcc": 5411,
            "errors": None,
            "is_fraud": "No",
            "scenario": "normal_spend",
            "source_system": "POS",
            "run_id": "test-run",
            "created_at": "2026-10-25T12:00:00Z",
        },
        {
            "id": 2,
            "transaction_id": "tx-2",
            "event_id": "evt-2",
            "event_type": "transaction.created",
            "user_id": 101,
            "card_index": 1,
            "amount": "120.00",
            "amount_raw": "$120.00",
            "use_chip": "Online Transaction",
            "transaction_entry_method": "ecommerce",
            "merchant_name": "Store B",
            "merchant_city": "Chicago",
            "merchant_state": "IL",
            "merchant_latitude": 41.8781,
            "merchant_longitude": -87.6298,
            "zip": "60601",
            "mcc": 5311,
            "errors": None,
            "is_fraud": "No",
            "scenario": "normal_spend",
            "source_system": "WEB",
            "run_id": "test-run",
            "created_at": "2026-10-25T12:01:00Z",
        },
    ]


class TestDataQualityGate:
    def test_valid_records_pass(self, sample_raw_transactions):
        result = DataQualityGate.validate_batch(sample_raw_transactions)
        assert len(result.valid_records) == 2
        assert len(result.quarantined_records) == 0
        assert result.duplicates_dropped == 0

    def test_missing_mandatory_fields_quarantined(self, sample_raw_transactions):
        # Corrupt one record
        bad_record = {"id": 99, "amount": "50.0"}  # missing user_id, transaction_id, created_at, card_index
        items = sample_raw_transactions + [bad_record]

        result = DataQualityGate.validate_batch(items)
        assert len(result.valid_records) == 2
        assert len(result.quarantined_records) == 1
        assert "Missing mandatory field" in result.quarantined_records[0]["rejection_reason"]

    def test_duplicate_ids_in_same_batch_quarantined(self, sample_raw_transactions):
        duplicate_record = dict(sample_raw_transactions[0])
        duplicate_record["transaction_id"] = "tx-dup"
        items = sample_raw_transactions + [duplicate_record]

        result = DataQualityGate.validate_batch(items)
        assert len(result.valid_records) == 2
        assert len(result.quarantined_records) == 1
        assert result.duplicates_dropped == 1
        assert "Duplicate sequence ID 1" in result.quarantined_records[0]["rejection_reason"]

    def test_negative_amount_without_refund_scenario_quarantined(self, sample_raw_transactions):
        bad_negative = dict(sample_raw_transactions[0])
        bad_negative["id"] = 3
        bad_negative["amount"] = "-25.00"
        bad_negative["scenario"] = "normal_spend"

        legit_refund = dict(sample_raw_transactions[1])
        legit_refund["id"] = 4
        legit_refund["amount"] = "-40.00"
        legit_refund["scenario"] = "refund"

        items = [bad_negative, legit_refund]
        result = DataQualityGate.validate_batch(items)
        assert len(result.valid_records) == 1
        assert result.valid_records[0]["id"] == 4
        assert len(result.quarantined_records) == 1
        assert "Negative amount -25.0 on non-refund" in result.quarantined_records[0]["rejection_reason"]

    def test_refund_return_scenario_negative_amount_passes(self, sample_raw_transactions):
        # Specifically tests the generator's exact refund_return scenario string
        refund_return_rec = dict(sample_raw_transactions[0])
        refund_return_rec["id"] = 5
        refund_return_rec["amount"] = "-175.14"
        refund_return_rec["scenario"] = "refund_return"

        result = DataQualityGate.validate_batch([refund_return_rec])
        assert len(result.valid_records) == 1
        assert len(result.quarantined_records) == 0
        assert result.valid_records[0]["amount"] == "-175.14"

    def test_negative_user_or_card_index_quarantined(self, sample_raw_transactions):
        bad_user = dict(sample_raw_transactions[0])
        bad_user["id"] = 6
        bad_user["user_id"] = -1

        bad_card = dict(sample_raw_transactions[1])
        bad_card["id"] = 7
        bad_card["card_index"] = -5

        result = DataQualityGate.validate_batch([bad_user, bad_card])
        assert len(result.valid_records) == 0
        assert len(result.quarantined_records) == 2
        assert "Invalid negative user_id" in result.quarantined_records[0]["rejection_reason"]
        assert "Invalid negative user_id" in result.quarantined_records[1]["rejection_reason"]

    def test_non_numeric_amount_quarantined(self, sample_raw_transactions):
        bad_amt = dict(sample_raw_transactions[0])
        bad_amt["id"] = 8
        bad_amt["amount"] = "INVALID_AMT"

        result = DataQualityGate.validate_batch([bad_amt])
        assert len(result.valid_records) == 0
        assert len(result.quarantined_records) == 1
        assert "Invalid numeric amount representation" in result.quarantined_records[0]["rejection_reason"]


class TestTransactionExtractor:
    def test_successful_cycle(self, sample_raw_transactions):
        mock_s3 = MagicMock()
        mock_dynamo = MagicMock()
        # Initial watermark is 0
        mock_dynamo.get_item.return_value = {"Item": {"last_since_id": {"N": "0"}}}

        extractor = TransactionExtractor(
            api_base_url="http://mock-api:8000",
            s3_bucket="test-bucket",
            table_name="test-table",
            region_name="eu-west-3",
            s3_client=mock_s3,
            dynamodb_client=mock_dynamo,
        )

        with patch.object(extractor, "fetch_page", return_value={"items": sample_raw_transactions, "has_more": False}):
            summary = extractor.run_ingestion_cycle(limit=100)

        assert summary["status"] == "SUCCESS"
        assert summary["records_written"] == 2
        assert summary["watermark_before"] == 0
        assert summary["watermark_after"] == 2

        # Verify S3 call was made with deterministic filename
        assert mock_s3.put_object.called
        s3_call_args = mock_s3.put_object.call_args[1]
        assert s3_call_args["Bucket"] == "test-bucket"
        assert "transactions_batch_1_2.parquet" in s3_call_args["Key"]

        # Verify parquet content is valid
        parquet_bytes = s3_call_args["Body"]
        table = pq.read_table(io.BytesIO(parquet_bytes))
        assert len(table) == 2
        assert table.column("id").to_pylist() == [1, 2]

        # Verify DynamoDB commit occurred AFTER S3 write with expected watermark
        assert mock_dynamo.update_item.called
        dynamo_call_args = mock_dynamo.update_item.call_args[1]
        assert dynamo_call_args["ExpressionAttributeValues"][":sid"]["N"] == "2"

    def test_empty_batch_handling(self):
        mock_s3 = MagicMock()
        mock_dynamo = MagicMock()
        mock_dynamo.get_item.return_value = {"Item": {"last_since_id": {"N": "50"}}}

        extractor = TransactionExtractor(
            s3_bucket="test-bucket",
            table_name="test-table",
            s3_client=mock_s3,
            dynamodb_client=mock_dynamo,
        )

        with patch.object(extractor, "fetch_page", return_value={"items": [], "has_more": False}):
            summary = extractor.run_ingestion_cycle(limit=100)

        assert summary["status"] == "IDLE"
        assert summary["records_written"] == 0
        assert summary["watermark_before"] == 50
        assert summary["watermark_after"] == 50
        # No S3 writes and no DynamoDB updates should occur
        assert not mock_s3.put_object.called
        assert not mock_dynamo.update_item.called

    def test_s3_failure_preserves_watermark(self, sample_raw_transactions):
        mock_s3 = MagicMock()
        mock_dynamo = MagicMock()
        mock_dynamo.get_item.return_value = {"Item": {"last_since_id": {"N": "10"}}}
        mock_s3.put_object.side_effect = RuntimeError("S3 PutObject 503 SlowDown")

        extractor = TransactionExtractor(
            s3_client=mock_s3,
            dynamodb_client=mock_dynamo,
        )

        with patch.object(extractor, "fetch_page", return_value={"items": sample_raw_transactions, "has_more": False}):
            with pytest.raises(RuntimeError, match="S3 PutObject 503"):
                extractor.run_ingestion_cycle(limit=100)

        # DynamoDB update must NOT be called if S3 fails
        assert not mock_dynamo.update_item.called

    def test_deterministic_filename_across_retries(self, sample_raw_transactions):
        mock_s3 = MagicMock()
        mock_dynamo = MagicMock()
        mock_dynamo.get_item.return_value = {"Item": {"last_since_id": {"N": "0"}}}

        extractor = TransactionExtractor(s3_client=mock_s3, dynamodb_client=mock_dynamo)

        with patch.object(extractor, "fetch_page", return_value={"items": sample_raw_transactions, "has_more": False}):
            summary1 = extractor.run_ingestion_cycle(limit=100)
            summary2 = extractor.run_ingestion_cycle(limit=100)

        # Both runs of the same batch boundary (1 -> 2) produce the exact same S3 key
        assert summary1["s3_path"] == summary2["s3_path"]
        assert "transactions_batch_1_2.parquet" in summary1["s3_path"]
