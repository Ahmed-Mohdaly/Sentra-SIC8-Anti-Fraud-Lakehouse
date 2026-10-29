import json
import time
import pytest
import confluent_kafka
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
    conf = {
        "bootstrap.servers": "localhost:9092",
        "group.id": f"test-group-{int(time.time())}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer = confluent_kafka.Consumer(conf)
    consumer.subscribe(["transactions"])

    # Run generator service to produce 5 events to live Kafka
    service = GeneratorService(config=config, publisher=publisher)
    service.start()

    # Wait for completion
    timeout = 10.0
    start = time.time()
    while service.state.value == "RUNNING" and (time.time() - start) < timeout:
        time.sleep(0.5)

    service.stop(timeout=5.0)

    # Consume at least 1 message from transactions topic
    consumed_msg = None
    consume_deadline = time.time() + 10.0
    while time.time() < consume_deadline:
        msg = consumer.poll(1.0)
        if msg is not None and not msg.error():
            consumed_msg = msg
            break

    consumer.close()

    assert consumed_msg is not None, "Failed to consume message from live Kafka transactions topic"
    val = json.loads(consumed_msg.value().decode("utf-8"))
    assert val["event_type"] == "transaction.created"
    assert "payload" in val
    assert "User" in val["payload"]
    assert "Card" in val["payload"]
    assert "Amount" in val["payload"]
