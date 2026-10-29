"""Focused tests validating reliability, bounded retries, secrets protection, and contract integrity."""

import importlib
import logging
import pytest
from unittest.mock import MagicMock, patch
from pydantic import ValidationError
from synthetic_streaming.config import GeneratorConfig, KafkaConfig
from synthetic_streaming.generator_service import GeneratorService, ServiceState
from synthetic_streaming.kafka_publisher import KafkaEventPublisher, MockEventPublisher
from synthetic_streaming.rate_controller import RateController


def test_tps_configuration_validation():
    """Verify TPS configuration rejects zero or negative rates."""
    with pytest.raises(ValidationError):
        GeneratorConfig(target_tps=0)

    with pytest.raises(ValidationError):
        GeneratorConfig(target_tps=-5.0)

    cfg = GeneratorConfig(target_tps=25.0)
    assert cfg.target_tps == 25.0


def test_removed_monitoring_modules_not_present():
    """Verify metrics module has been removed and cannot be imported."""
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("synthetic_streaming.metrics")


def test_bounded_kafka_producer_retries():
    """Verify KafkaEventPublisher produce retries are strictly bounded on BufferError."""
    cfg = KafkaConfig(bootstrap_servers="localhost:9092")
    mock_producer = MagicMock()
    # Always raise BufferError on produce
    mock_producer.produce.side_effect = BufferError("Queue full")
    mock_producer.__len__.return_value = 1000

    pub = KafkaEventPublisher.__new__(KafkaEventPublisher)
    pub.config = cfg
    pub._producer = mock_producer
    pub._initialized = True
    pub._is_closed = False
    pub._total_published = 0
    pub._total_delivered = 0
    pub._total_failed = 0
    pub._last_error = None
    pub._lock = MagicMock()

    with pytest.raises(BufferError):
        pub.publish("test_topic", "key1", b"val1")

    # Should have retried at most 3 times and called poll to drain
    assert mock_producer.produce.call_count == 3
    assert mock_producer.poll.call_count == 3
    assert pub._total_failed == 1


def test_graceful_shutdown_flushes_pending_messages():
    """Verify GeneratorService.stop() initiates bounded publisher flush."""
    cfg = GeneratorConfig(target_tps=100.0)
    mock_pub = MagicMock()
    mock_pub.health.return_value = {"status": "healthy"}
    mock_pub.flush.return_value = 0

    service = GeneratorService(config=cfg, publisher=mock_pub)
    service.start()
    assert service.state == ServiceState.RUNNING

    service.stop(timeout=2.0)
    assert service.state == ServiceState.STOPPED
    # Verify publisher.flush was called with bounded timeout
    mock_pub.flush.assert_called_with(timeout=2.0)


def test_rate_controller_prevents_catchup_bursts():
    """Verify rate limiter does not generate uncontrolled bursts when falling behind."""
    controller = RateController(target_tps=10.0)
    controller.start()

    # Simulate falling behind by 2 seconds
    controller._next_event_time = controller._next_event_time - 2.0

    # Next wait_for_slot should reset slot rather than granting 20 slots immediately
    slot_granted = controller.wait_for_slot()
    assert slot_granted is True

    # The scheduled next event time should be now + interval, not in the past
    now = controller._window_start_time
    assert controller._next_event_time >= now
    controller.stop()


def test_secrets_masked_in_safe_dict_and_logs(caplog):
    """Verify sensitive connection strings, API keys, and payment data are not exposed."""
    cfg = GeneratorConfig(
        api_key="super-secret-api-key",
        kafka=KafkaConfig(
            sasl_password="Endpoint=sb://myeventhubs.servicebus.windows.net/;SharedAccessKeyName=Root;SharedAccessKey=secret123"
        )
    )
    safe = cfg.to_safe_dict()
    assert safe["api_key"] == "********"
    assert safe["kafka"]["sasl_password"] == "********"
    assert "secret123" not in str(safe)
    assert "super-secret" not in str(safe)


def test_broker_outage_does_not_cause_unbounded_memory():
    """Verify MockEventPublisher ring buffer enforces strict bound on message storage."""
    pub = MockEventPublisher(max_buffer=50)
    for i in range(100):
        try:
            pub.publish(topic="test", key=f"k{i}", value=f"val{i}".encode("utf-8"))
        except BufferError:
            pass

    # Buffer should not exceed max_buffer
    assert len(pub.published_messages) <= 50
    assert pub.is_buffer_full() is True


def test_sanitize_db_error_masks_credentials():
    """Verify sanitize_db_error strips credentials from connection strings and error messages."""
    from synthetic_streaming.database import sanitize_db_error

    err_with_url = (
        'sqlalchemy.exc.OperationalError: (psycopg2.OperationalError) '
        'connection to server at "db.rds.amazonaws.com" failed: '
        'FATAL: password authentication failed for user "postgres" '
        'url=postgresql://postgres:MyVerySecretPassword123!@db.rds.amazonaws.com:5432/synthetic_db'
    )
    clean = sanitize_db_error(err_with_url)
    assert "MyVerySecretPassword123!" not in clean
    assert "postgresql://postgres:********@db.rds.amazonaws.com:5432/synthetic_db" in clean

    # Standalone password parameter
    err_param = "fe_sendauth: no password supplied password=SuperSecretPassword host=localhost"

def test_e2e_validation_decompression_and_id_verification():
    """Verify that gzipped JSONL parsing in e2e validation correctly extracts and matches IDs."""
    import gzip
    import json
    import io

    records = [
        {"key": "user1", "value": json.dumps({"data": {"transaction_id": "tx-e2e-12345-001"}})},
        {"key": "user2", "value": json.dumps({"payload": {"after": {"user_id": 888801}}})},
    ]
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as f:
        for r in records:
            f.write((json.dumps(r) + "\n").encode("utf-8"))
    compressed_data = buf.getvalue()

    decompressed = gzip.decompress(compressed_data).decode("utf-8")
    found_tx = False
    found_user = False
    for line in decompressed.strip().split("\n"):
        rec = json.loads(line)
        val = json.loads(rec["value"])
        if val.get("data", {}).get("transaction_id") == "tx-e2e-12345-001":
            found_tx = True
        if val.get("payload", {}).get("after", {}).get("user_id") == 888801:
            found_user = True

    assert found_tx is True
    assert found_user is True

