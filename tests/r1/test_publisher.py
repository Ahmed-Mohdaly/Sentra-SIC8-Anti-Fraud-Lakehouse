import pytest
from unittest.mock import MagicMock
from synthetic_streaming.kafka_publisher import MockEventPublisher, KafkaEventPublisher
from synthetic_streaming.config import KafkaConfig


def test_mock_publisher_successful_delivery():
    pub = MockEventPublisher()

    delivered_msgs = []
    def on_deliv(err, msg):
        delivered_msgs.append((err, msg))

    pub.publish(topic="test_topic", key="k1", value=b"test-val", on_delivery=on_deliv)

    assert len(pub.published_messages) == 1
    assert len(delivered_msgs) == 1
    assert delivered_msgs[0][0] is None
    health = pub.health()
    assert health["status"] == "healthy"
    assert health["total_published"] == 1
    assert health["total_delivered"] == 1
    assert health["total_failed"] == 0


def test_mock_publisher_simulated_failure():
    pub = MockEventPublisher()
    pub.simulate_failure = True

    delivered_errors = []
    def on_deliv(err, msg):
        delivered_errors.append(err)

    pub.publish(topic="test_topic", key="k1", value=b"test-val", on_delivery=on_deliv)

    assert len(delivered_errors) == 1
    assert delivered_errors[0] is not None
    health = pub.health()
    assert health["status"] == "unhealthy"
    assert health["total_failed"] == 1


def test_mock_publisher_buffer_exhaustion():
    pub = MockEventPublisher(max_buffer=2)
    pub.publish(topic="test_topic", key="k1", value=b"val1")
    pub.publish(topic="test_topic", key="k2", value=b"val2")

    with pytest.raises(BufferError, match="Simulated buffer full"):
        pub.publish(topic="test_topic", key="k3", value=b"val3")


def test_kafka_publisher_health_non_blocking():
    cfg = KafkaConfig(bootstrap_servers="localhost:9092")
    # Mock confluent_kafka.Producer to avoid network connection
    mock_producer = MagicMock()
    mock_producer.__len__.return_value = 5

    pub = KafkaEventPublisher.__new__(KafkaEventPublisher)
    pub.config = cfg
    pub._producer = mock_producer
    pub._initialized = True
    pub._is_closed = False
    pub._total_published = 10
    pub._total_delivered = 8
    pub._total_failed = 0
    pub._last_error = None

    health = pub.health()
    assert health["status"] == "healthy"
    assert health["buffered_messages"] == 5
    assert health["total_published"] == 10
    assert health["total_delivered"] == 8
    # Producer list_topics should NOT have been called (non-blocking)
    assert not mock_producer.list_topics.called


def test_publisher_module_imports_without_confluent_kafka():
    """Verify that kafka_publisher and mock publisher work when confluent_kafka is missing."""
    import importlib
    import sys
    from unittest.mock import patch

    with patch.dict(sys.modules, {"confluent_kafka": None}):
        import synthetic_streaming.kafka_publisher as kp
        importlib.reload(kp)
        assert kp.HAS_CONFLUENT_KAFKA is False

        # MockEventPublisher must continue to work seamlessly
        mock_pub = kp.MockEventPublisher()
        mock_pub.publish("topic", "key", b"value")
        assert len(mock_pub.published_messages) == 1

        # Attempting to instantiate KafkaEventPublisher must raise informative RuntimeError
        with pytest.raises(RuntimeError, match="confluent-kafka is not installed"):
            kp.KafkaEventPublisher(KafkaConfig())

    # Reload again to restore normal state for subsequent tests
    importlib.reload(kp)


def test_kafka_producer_buffer_error_bounded_retry():
    """Verify that KafkaEventPublisher retries on BufferError and propagates failure if exhausted."""
    cfg = KafkaConfig(bootstrap_servers="localhost:9092", queue_buffering_max_messages=100)
    mock_producer = MagicMock()
    # Always raise BufferError to simulate full queue
    mock_producer.produce.side_effect = BufferError("Queue full")
    mock_producer.__len__.return_value = 100

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
    pub._last_error_log_time = 0.0

    errors_caught = []
    def callback(err, msg):
        errors_caught.append(err)

    with pytest.raises(BufferError):
        pub.publish("test_topic", "key1", b"val1", on_delivery=callback)

    # Must have attempted bounded retries (3 attempts total)
    assert mock_producer.produce.call_count == 3
    # Bounded polling must have been called during retries
    assert mock_producer.poll.call_count >= 2
    # Callback must have received the BufferError
    assert len(errors_caught) == 1
    assert isinstance(errors_caught[0], BufferError)


def test_generator_service_batch_delivery_failure_raises():
    """Verify that GeneratorService.generate_batch propagates delivery failures to the caller."""
    from synthetic_streaming.generator_service import GeneratorService
    from synthetic_streaming.config import GeneratorConfig
    from sqlalchemy import create_engine

    cfg = GeneratorConfig(worker_id=1, baseline_data_dir="data", auto_seed=False)
    db_engine = create_engine("sqlite:///:memory:")

    mock_pub = MockEventPublisher()
    mock_pub.simulate_failure = True  # Will fail delivery callback

    service = GeneratorService(config=cfg, publisher=mock_pub, db_engine=db_engine)

    with pytest.raises(RuntimeError, match="Batch transaction delivery failed"):
        service.generate_batch(count=5)

    status = service.get_status()
    assert status["total_transactions_failed"] > 0


def test_s3_sink_prefix_template_formatting():
    """Verify that S3 sink template strictly produces bronze/ prefixed keys."""
    template = "bronze/{{topic}}/partition={{partition}}/{{start_offset:padding=true}}.json.gz"
    topic = "transactions"
    partition = "0"
    offset = "00000000000000000100"

    rendered = template.replace("{{topic}}", topic).replace("{{partition}}", partition).replace("{{start_offset:padding=true}}", offset)
    assert rendered.startswith("bronze/transactions/partition=0/")
    assert rendered.endswith(".json.gz")


def test_generator_service_batch_flush_timeout_raises():
    """Verify that GeneratorService.generate_batch raises on flush timeout with unacknowledged messages."""
    from synthetic_streaming.generator_service import GeneratorService
    from synthetic_streaming.config import GeneratorConfig
    from sqlalchemy import create_engine
    from unittest.mock import MagicMock

    cfg = GeneratorConfig(worker_id=1, baseline_data_dir="data", auto_seed=False)
    db_engine = create_engine("sqlite:///:memory:")

    mock_pub = MockEventPublisher()
    mock_pub.flush = MagicMock(return_value=3)  # 3 unacknowledged messages remaining after timeout

    service = GeneratorService(config=cfg, publisher=mock_pub, db_engine=db_engine)

    with pytest.raises(RuntimeError, match="Batch transaction delivery failed: 3 unacknowledged messages"):
        service.generate_batch(count=5)

    status = service.get_status()
    assert status["total_transactions_failed"] == 3


