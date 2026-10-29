import json
from decimal import Decimal
import pytest
from synthetic_streaming.event_schema import (
    CardPayload,
    EventEnvelope,
    TransactionPayload,
    UserPayload,
)


def test_transaction_payload_valid():
    payload_data = {
        "User": 100,
        "Card": 2,
        "Year": 2026,
        "Month": 10,
        "Day": 24,
        "Time": "14:30",
        "Amount": "$125.50",
        "Use Chip": "Chip Transaction",
        "Merchant Name": "3527213246127876953",
        "Merchant City": "La Verne",
        "Merchant State": "CA",
        "Zip": 91750.0,
        "MCC": 5411,
        "Errors?": None,
        "Is Fraud?": "No",
    }
    tx = TransactionPayload(**payload_data)
    assert tx.User == 100
    assert tx.Card == 2
    assert tx.Amount == "$125.50"
    assert tx.Use_Chip == "Chip Transaction"


def test_transaction_payload_invalid_chip():
    payload_data = {
        "User": 100,
        "Card": 2,
        "Year": 2026,
        "Month": 10,
        "Day": 24,
        "Time": "14:30",
        "Amount": "$125.50",
        "Use Chip": "Invalid Chip Method",
        "Merchant Name": "3527213246127876953",
        "Merchant City": "La Verne",
        "Merchant State": "CA",
        "Zip": 91750.0,
        "MCC": 5411,
        "Errors?": None,
        "Is Fraud?": "No",
    }
    with pytest.raises(ValueError, match="Invalid Use Chip value"):
        TransactionPayload(**payload_data)


def test_event_envelope_serialization():
    envelope = EventEnvelope(
        event_type="transaction.created",
        source_system="POS_TERMINAL",
        run_id="test-run",
        scenario="normal_spend",
        payload={"User": 50, "Card": 0, "Amount": "$10.00"},
    )
    raw_json = envelope.to_json()
    parsed = json.loads(raw_json)
    assert parsed["event_type"] == "transaction.created"
    assert parsed["source_system"] == "POS_TERMINAL"
    assert parsed["schema_version"] == 1
    assert "event_id" in parsed
    assert "event_timestamp" in parsed
    assert parsed["payload"]["Amount"] == "$10.00"
