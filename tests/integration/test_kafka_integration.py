import json
import time
import pytest

# Skip this entire integration module cleanly if confluent_kafka is not installed (e.g. in minimal CI)
confluent_kafka = pytest.importorskip("confluent_kafka")

from synthetic_streaming.config import GeneratorConfig
from synthetic_streaming.generator_service import GeneratorService
from synthetic_streaming.kafka_publisher import KafkaEventPublisher


@pytest.mark.integration
def test_kafka_live_publish_and_consume():
    config = GeneratorConfig()
    config.kafka.bootstrap_servers = "localhost:9092"
    config.target_tps = 10.0
    config.max_events = 5

    # Check broker connectivity first
    try:
        publisher = KafkaEventPublisher(config=config.kafka)
        if not publisher.check_connectivity(timeout=1.0):
            pytest.skip("Kafka broker at localhost:9092 is not reachable.")
    except Exception as e:
        pytest.skip(f"Kafka broker connection failed: {e}")

    # Set up consumer
    consumer = confluent_kafka.Consumer(
        {
            "bootstrap.servers": config.kafka.bootstrap_servers,
            "group.id": "test-integration-group",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([config.kafka.topic_transactions])

    service = GeneratorService(config=config)
    service.start()

    time.sleep(1.0)
    service.stop()

    msg = consumer.poll(timeout=2.0)
    consumer.close()

    if msg is None:
        pytest.fail("Consumer timed out waiting for published event.")
    if msg.error():
        pytest.fail(f"Consumer encountered error: {msg.error()}")

    payload = json.loads(msg.value().decode("utf-8"))
    assert "event_id" in payload
    assert payload["event_type"] == "transaction.created"
    assert "payload" in payload
