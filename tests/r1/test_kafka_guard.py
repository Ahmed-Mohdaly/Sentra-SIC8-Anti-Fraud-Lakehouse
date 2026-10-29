from src.r1_ingestion.kafka_reference import publish_local_kafka
import pytest

def test_kafka_not_called_by_default():
    with pytest.raises(PermissionError):publish_local_kafka()
    with pytest.raises(PermissionError):publish_local_kafka(bootstrap='real-msk.example:9092',allow=True)
