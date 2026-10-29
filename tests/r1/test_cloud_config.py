import os
from unittest.mock import patch
from synthetic_streaming.config import GeneratorConfig, KafkaConfig
from synthetic_streaming.kafka_publisher import KafkaEventPublisher


def test_sasl_tls_config_loading():
    env_vars = {
        "KAFKA_BOOTSTRAP_SERVERS": "my-eventhub.servicebus.windows.net:9093",
        "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
        "KAFKA_SASL_MECHANISM": "PLAIN",
        "KAFKA_SASL_USERNAME": "$ConnectionString",
        "KAFKA_SASL_PASSWORD": "Endpoint=sb://test/;SharedAccessKey=secret",
        "WORKER_CONTROL_URL": "http://worker-internal:8001",
        "ROLE": "generator-api",
        "WORKER_ID": "3",
        "TOTAL_WORKERS": "5",
    }
    with patch.dict(os.environ, env_vars, clear=False):
        config = GeneratorConfig.load()
        assert config.kafka.bootstrap_servers == "my-eventhub.servicebus.windows.net:9093"
        assert config.kafka.security_protocol == "SASL_SSL"
        assert config.kafka.sasl_mechanism == "PLAIN"
        assert config.kafka.sasl_username == "$ConnectionString"
        assert config.kafka.sasl_password.get_secret_value() == "Endpoint=sb://test/;SharedAccessKey=secret"
        assert config.worker_control_url == "http://worker-internal:8001"
        assert config.role == "generator-api"
        assert config.worker_id == 3
        assert config.total_workers == 5


def test_kafka_publisher_sasl_config_mapping():
    kafka_cfg = KafkaConfig(
        bootstrap_servers="broker.cloud:9093",
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_username="alice",
        sasl_password="secretpassword",
    )
    with patch("confluent_kafka.Producer") as mock_producer:
        publisher = KafkaEventPublisher(config=kafka_cfg)
        args, kwargs = mock_producer.call_args
        config_dict = args[0]
        assert config_dict["security.protocol"] == "SASL_SSL"
        assert config_dict["sasl.mechanism"] == "PLAIN"
        assert config_dict["sasl.username"] == "alice"
        assert config_dict["sasl.password"] == "secretpassword"
