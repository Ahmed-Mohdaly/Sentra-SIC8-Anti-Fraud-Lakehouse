"""Unit tests for Fraud Detection Feature Extraction, StateStore, and Real-Time Consumer."""

from __future__ import annotations

import json
import time
from unittest.mock import MagicMock
import pytest
from ml.feature_engineering import (
    FEATURE_NAMES,
    FeatureExtractor,
    clean_currency,
    haversine_distance,
)
from ml.state_store import StateStore
from ml.fraud_consumer import FraudStreamingConsumer


def test_haversine_distance():
    # Los Angeles to New York (~3935 km)
    la_lat, la_lon = 34.0522, -118.2437
    ny_lat, ny_lon = 40.7128, -74.0060

    dist = haversine_distance(la_lat, la_lon, ny_lat, ny_lon)
    assert 3900.0 < dist < 4000.0

    # Same location should be 0.0
    assert haversine_distance(la_lat, la_lon, la_lat, la_lon) == 0.0

    # None coordinate returns 0.0
    assert haversine_distance(None, la_lon, ny_lat, ny_lon) == 0.0


def test_clean_currency():
    assert clean_currency("$1,234.56") == 1234.56
    assert clean_currency("$-50.00") == -50.00
    assert clean_currency(99.9) == 99.9
    assert clean_currency(None, default=10.0) == 10.0


def test_feature_extractor_online():
    extractor = FeatureExtractor(
        mcc_risk_map={5411: 0.001, 5732: 0.045},
        global_stats={"user_avg_spend": 50.0, "user_std_spend": 25.0, "credit_limit": 5000.0, "global_fraud_rate": 0.0015}
    )

    payload = {
        "User": 10,
        "Card": 0,
        "Amount": "$250.00",
        "Use Chip": "Swipe Transaction",
        "Transaction Entry Method": "magnetic_stripe_swipe",
        "Time": "02:30",  # Night transaction
        "Merchant Latitude": 40.7128,
        "Merchant Longitude": -74.0060,
        "MCC": 5732,
        "Is Fraud?": "Yes",
    }
    user_p = {
        "user_id": 10,
        "Current Age": 45,
        "FICO Score": 750,
        "Yearly Income - Person": "$80,000",
        "Total Debt": "$15,000",
        "Num Credit Cards": 4,
        "Latitude": 34.0522,
        "Longitude": -118.2437,
    }
    card_p = {
        "Card Brand": "Visa",
        "Card Type": "Credit",
        "Has Chip": "YES",
        "Credit Limit": "$10,000",
        "Card on Dark Web": "Yes",
    }
    state = {
        "user_avg_spend": 50.0,
        "user_std_spend": 25.0,
        "time_since_last_tx_sec": 600.0,  # 10 minutes ago
        "prev_merchant_lat": 34.0522,      # Los Angeles
        "prev_merchant_lon": -118.2437,
        "tx_count_last_1h": 3.0,
        "tx_count_last_24h": 5.0,
        "spend_sum_last_1h": 400.0,
    }

    fdict = extractor.extract_online_features(
        payload=payload,
        user_profile=user_p,
        card_profile=card_p,
        state=state,
    )

    # Verifications
    assert fdict["amount"] == 250.0
    assert fdict["spend_deviation_ratio"] == 5.0  # 250 / 50
    assert fdict["credit_limit_utilization"] == 0.025  # 250 / 10000
    assert fdict["amount_zscore"] == 8.0  # (250 - 50) / 25
    assert fdict["is_night_tx"] == 1  # 02:30 is night
    assert fdict["card_on_dark_web"] == 1
    assert fdict["mcc_risk_score"] == 0.045
    # Impossible travel speed between LA and NY in 10 minutes: ~3935 km / (10/60)h ~= 23,610 km/h
    assert fdict["travel_speed_kmh"] > 10000.0
    assert fdict["is_online_tx"] == 0
    assert fdict["log_amount"] > 0
    assert fdict["mcc_high_risk_flag"] == 1
    assert fdict["is_refund"] == 0

    # Ensure strictly aligned feature vector
    vector = extractor.to_feature_vector(fdict)
    assert len(vector) == len(FEATURE_NAMES)


def test_state_store_sliding_window():
    store = StateStore(enable_in_memory_fallback=True)
    # Force in-memory fallback for local unit test
    store._using_fallback = True

    now = time.time()
    # Transaction 1
    st1 = store.get_and_update_state(
        user_id=1,
        card_index=0,
        amount=50.0,
        epoch_sec=now - 7200,  # 2 hours ago
        merchant_lat=34.0,
        merchant_lon=-118.0
    )
    assert st1["tx_count_last_24h"] == 1.0

    # Transaction 2 (30 minutes later)
    st2 = store.get_and_update_state(
        user_id=1,
        card_index=0,
        amount=100.0,
        epoch_sec=now - 5400,  # 1.5 hours ago
        merchant_lat=34.1,
        merchant_lon=-118.1
    )
    assert st2["tx_count_last_24h"] == 2.0
    assert st2["time_since_last_tx_sec"] == 1800.0

    # Transaction 3 (Now)
    st3 = store.get_and_update_state(
        user_id=1,
        card_index=0,
        amount=30.0,
        epoch_sec=now,
        merchant_lat=34.2,
        merchant_lon=-118.2
    )
    # In last 1h (now - 3600 to now), only Transaction 3 exists
    assert st3["tx_count_last_1h"] == 1.0
    assert st3["spend_sum_last_1h"] == 30.0
    # In last 24h, all 3 exist
    assert st3["tx_count_last_24h"] == 3.0


def test_consumer_process_transaction():
    consumer = FraudStreamingConsumer()
    consumer.state_store._using_fallback = True
    consumer.preload_reference_data()

    envelope = {
        "event_id": "test-tx-001",
        "event_type": "transaction.created",
        "event_timestamp": "2026-10-28T17:30:00Z",
        "payload": {
            "User": 0,
            "Card": 0,
            "Time": "14:15",
            "Amount": "$85.00",
            "Use Chip": "Chip Transaction",
            "Transaction Entry Method": "contact_chip",
            "Merchant Latitude": 34.15,
            "Merchant Longitude": -117.76,
            "MCC": 5411,
            "Is Fraud?": "No",
        }
    }

    alert = consumer.process_transaction(envelope)

    assert "alert_id" in alert
    assert alert["user_id"] == 0
    assert alert["amount"] == 85.0
    assert alert["ground_truth_fraud"] == "No"
    assert isinstance(alert["is_fraud_predicted"], bool)
    assert isinstance(alert["fraud_probability"], float)
    assert alert["latency_ms"] <= 50.0  # Extremely fast


def test_duplicate_event_handling_idempotency():
    """Verify that retried/duplicate messages do not corrupt rolling spend sums or counts."""
    store = StateStore(enable_in_memory_fallback=True)
    store._using_fallback = True

    now = time.time()
    evt_id = "test-dup-uuid-999"

    # First event delivery
    st1 = store.get_and_update_state(
        user_id=42,
        card_index=1,
        amount=100.0,
        epoch_sec=now,
        merchant_lat=34.0,
        merchant_lon=-118.0,
        event_id=evt_id,
    )
    assert st1["spend_sum_last_1h"] == 100.0
    assert st1["tx_count_last_1h"] == 1.0

    # Duplicate delivery (same event_id retried by Kafka consumer)
    st2 = store.get_and_update_state(
        user_id=42,
        card_index=1,
        amount=100.0,
        epoch_sec=now,
        merchant_lat=34.0,
        merchant_lon=-118.0,
        event_id=evt_id,
    )
    # The rolling history must have suppressed the duplicate addition:
    assert len(store._local_user_tx_history[f"{42}:{1}"]) == 1
    # Count should not increment to 2:
    assert store._local_user_spend_stats[42]["count"] == 1.0
    assert store._local_user_spend_stats[42]["sum"] == 100.0


def test_malformed_event_handling():
    """Verify graceful error handling on corrupt or non-dictionary event envelopes."""
    consumer = FraudStreamingConsumer()
    consumer.state_store._using_fallback = True

    # 1. Non-dict input
    res_bad = consumer.process_transaction("corrupt string message")  # type: ignore
    assert res_bad["status"] == "MALFORMED_INPUT"
    assert res_bad["is_fraud_predicted"] is False

    # 2. Envelope with non-dict payload
    res_no_payload = consumer.process_transaction({"event_id": "bad-1", "payload": None})
    assert res_no_payload["status"] == "MALFORMED_PAYLOAD"
    assert res_no_payload["is_fraud_predicted"] is False


def test_consumer_pass_and_alert_decision():
    """Verify distinct PASS vs ALERT decision output semantics."""
    consumer = FraudStreamingConsumer()
    consumer.state_store._using_fallback = True
    consumer.preload_reference_data()

    # Normal spend transaction -> PASS
    pass_envelope = {
        "event_id": "evt-pass-001",
        "event_type": "transaction.created",
        "payload": {
            "User": 10,
            "Card": 0,
            "Amount": "$15.00",
            "Use Chip": "Chip Transaction",
            "Transaction Entry Method": "contact_chip",
            "Merchant Latitude": 34.05,
            "Merchant Longitude": -118.24,
            "MCC": 5411,
            "Is Fraud?": "No",
        }
    }
    pass_res = consumer.process_transaction(pass_envelope)
    assert pass_res["decision"] == "PASS"
    assert pass_res["is_fraud_predicted"] is False

    # Severe anomaly spend spike -> ALERT
    alert_envelope = {
        "event_id": "evt-alert-002",
        "event_type": "transaction.created",
        "payload": {
            "User": 10,
            "Card": 0,
            "Amount": "$7500.00",
            "Use Chip": "Swipe Transaction",
            "Transaction Entry Method": "magnetic_stripe_swipe",
            "Merchant Latitude": 34.05,
            "Merchant Longitude": -118.24,
            "MCC": 5300,
            "Is Fraud?": "Yes",
        }
    }
    alert_res = consumer.process_transaction(alert_envelope)
    assert alert_res["decision"] == "ALERT"
    assert alert_res["is_fraud_predicted"] is True
    assert "spend_spike" in str(alert_res["risk_factors"]) or "high_risk_mcc" in str(alert_res["risk_factors"]) or alert_res["fraud_probability"] >= consumer.threshold


def test_manual_offset_commit_on_pass():
    """Verify offset commit on successful PASS without publishing alert."""
    consumer = FraudStreamingConsumer()
    consumer.state_store._using_fallback = True
    consumer.preload_reference_data()

    mock_kafka_consumer = MagicMock()
    mock_kafka_producer = MagicMock()

    pass_payload = {
        "event_id": "tx-pass-101",
        "payload": {
            "User": 10,
            "Card": 0,
            "Amount": "$12.50",
            "Use Chip": "Chip Transaction",
            "Merchant Latitude": 34.05,
            "Merchant Longitude": -118.24,
            "MCC": 5411,
            "Is Fraud?": "No",
        }
    }
    mock_msg = MagicMock()
    mock_msg.value.return_value = json.dumps(pass_payload).encode("utf-8")
    mock_msg.offset.return_value = 101

    res = consumer.consume_and_process_single_message(
        msg=mock_msg,
        consumer=mock_kafka_consumer,
        producer=mock_kafka_producer,
    )

    assert res["decision"] == "PASS"
    assert res["is_fraud_predicted"] is False
    # Producer should NOT be called for PASS
    assert mock_kafka_producer.produce.call_count == 0
    # Consumer offset MUST be committed synchronously
    mock_kafka_consumer.commit.assert_called_once_with(message=mock_msg, asynchronous=False)


def test_manual_offset_commit_on_alert():
    """Verify alert publishing and offset commit on ALERT."""
    consumer = FraudStreamingConsumer()
    consumer.state_store._using_fallback = True
    consumer.preload_reference_data()
    consumer.threshold = 0.01  # Force ALERT decision

    mock_kafka_consumer = MagicMock()
    mock_kafka_producer = MagicMock()
    mock_kafka_producer.flush.return_value = 0  # 0 remaining messages

    alert_payload = {
        "event_id": "tx-alert-202",
        "event_type": "transaction.created",
        "payload": {
            "User": 10,
            "Card": 0,
            "Amount": "$7500.00",
            "Use Chip": "Swipe Transaction",
            "Transaction Entry Method": "magnetic_stripe_swipe",
            "Merchant Latitude": 34.05,
            "Merchant Longitude": -118.24,
            "MCC": 5300,
            "Is Fraud?": "Yes",
        }
    }
    mock_msg = MagicMock()
    mock_msg.value.return_value = json.dumps(alert_payload).encode("utf-8")
    mock_msg.offset.return_value = 202

    res = consumer.consume_and_process_single_message(
        msg=mock_msg,
        consumer=mock_kafka_consumer,
        producer=mock_kafka_producer,
    )

    assert res["decision"] == "ALERT"
    assert res["is_fraud_predicted"] is True
    # Producer MUST produce alert and flush
    assert mock_kafka_producer.produce.call_count == 1
    mock_kafka_producer.flush.assert_called_once_with(timeout=5.0)
    # Consumer offset MUST be committed synchronously only after delivery
    mock_kafka_consumer.commit.assert_called_once_with(message=mock_msg, asynchronous=False)


def test_producer_delivery_failure_prevents_offset_commit():
    """Verify producer flush timeout failure blocks offset commit."""
    consumer = FraudStreamingConsumer()
    consumer.state_store._using_fallback = True
    consumer.preload_reference_data()
    consumer.threshold = 0.01  # Force ALERT decision

    mock_kafka_consumer = MagicMock()
    mock_kafka_producer = MagicMock()
    # Simulate flush timeout leaving unflushed messages
    mock_kafka_producer.flush.return_value = 1

    alert_payload = {
        "event_id": "tx-alert-303",
        "event_type": "transaction.created",
        "payload": {
            "User": 10,
            "Card": 0,
            "Amount": "$7500.00",
            "Use Chip": "Swipe Transaction",
            "Transaction Entry Method": "magnetic_stripe_swipe",
            "Merchant Latitude": 34.05,
            "Merchant Longitude": -118.24,
            "MCC": 5300,
            "Is Fraud?": "Yes",
        }
    }
    mock_msg = MagicMock()
    mock_msg.value.return_value = json.dumps(alert_payload).encode("utf-8")
    mock_msg.offset.return_value = 303

    with pytest.raises(RuntimeError, match="Producer delivery failed"):
        consumer.consume_and_process_single_message(
            msg=mock_msg,
            consumer=mock_kafka_consumer,
            producer=mock_kafka_producer,
        )

    # Offset MUST NOT be committed if producer alert delivery failed
    mock_kafka_consumer.commit.assert_not_called()


def test_producer_callback_error_prevents_offset_commit():
    """Verify delivery callback error blocks offset commit."""
    consumer = FraudStreamingConsumer()
    consumer.state_store._using_fallback = True
    consumer.preload_reference_data()
    consumer.threshold = 0.01  # Force ALERT decision

    mock_kafka_consumer = MagicMock()
    mock_kafka_producer = MagicMock()

    def side_effect_produce(topic, key, value, on_delivery):
        if on_delivery:
            on_delivery(RuntimeError("MSK Broker write rejected"), None)

    mock_kafka_producer.produce.side_effect = side_effect_produce
    mock_kafka_producer.flush.return_value = 0

    alert_payload = {
        "event_id": "tx-alert-304",
        "event_type": "transaction.created",
        "payload": {
            "User": 10,
            "Card": 0,
            "Amount": "$7500.00",
            "Use Chip": "Swipe Transaction",
            "Transaction Entry Method": "magnetic_stripe_swipe",
            "Merchant Latitude": 34.05,
            "Merchant Longitude": -118.24,
            "MCC": 5300,
            "Is Fraud?": "Yes",
        }
    }
    mock_msg = MagicMock()
    mock_msg.value.return_value = json.dumps(alert_payload).encode("utf-8")
    mock_msg.offset.return_value = 304

    with pytest.raises(RuntimeError, match="MSK Broker write rejected"):
        consumer.consume_and_process_single_message(
            msg=mock_msg,
            consumer=mock_kafka_consumer,
            producer=mock_kafka_producer,
        )

    # Offset MUST NOT be committed
    mock_kafka_consumer.commit.assert_not_called()



def test_processing_failure_prevents_offset_commit():
    """Verify deserialization or processing failure blocks offset commit."""
    consumer = FraudStreamingConsumer()
    consumer.state_store._using_fallback = True

    mock_kafka_consumer = MagicMock()
    mock_kafka_producer = MagicMock()

    mock_msg = MagicMock()
    mock_msg.value.return_value = b"{corrupt_json: invalid"
    mock_msg.offset.return_value = 404

    with pytest.raises(Exception):
        consumer.consume_and_process_single_message(
            msg=mock_msg,
            consumer=mock_kafka_consumer,
            producer=mock_kafka_producer,
        )

    # Offset MUST NOT be committed on processing / deserialization errors
    mock_kafka_consumer.commit.assert_not_called()

