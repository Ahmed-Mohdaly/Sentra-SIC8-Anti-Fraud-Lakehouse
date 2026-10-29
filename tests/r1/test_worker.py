import pytest
from fastapi.testclient import TestClient
from synthetic_streaming.config import GeneratorConfig
from synthetic_streaming.generator_service import GeneratorService
from synthetic_streaming.kafka_publisher import MockEventPublisher
from synthetic_streaming.worker import create_worker_internal_app


@pytest.fixture
def worker_app():
    config = GeneratorConfig(target_tps=10.0, worker_id=2, total_workers=4)
    publisher = MockEventPublisher()
    service = GeneratorService(config=config, publisher=publisher)
    app = create_worker_internal_app(service)
    return TestClient(app), service


def test_worker_health_and_ready(worker_app):
    client, _ = worker_app
    health_resp = client.get("/health")
    assert health_resp.status_code == 200
    assert health_resp.json()["status"] == "ok"
    assert health_resp.json()["worker_id"] == 2

    ready_resp = client.get("/ready")
    assert ready_resp.status_code == 200
    assert ready_resp.json()["status"] == "ready"


def test_worker_status(worker_app):
    client, _ = worker_app
    resp = client.get("/status")
    assert resp.status_code == 200
    data = resp.json()
    assert data["worker_id"] == 2
    assert data["total_workers"] == 4
    assert data["state"] == "STOPPED"
    assert data["target_tps"] == 10.0


def test_worker_control_start_stop(worker_app):
    client, service = worker_app
    # Start worker
    resp = client.post("/control/start")
    assert resp.status_code == 200
    assert resp.json()["status"]["state"] == "RUNNING"
    assert service.is_running

    # Stop worker
    resp = client.post("/control/stop")
    assert resp.status_code == 200
    assert resp.json()["status"]["state"] == "STOPPED"
    assert not service.is_running


def test_worker_control_config_update(worker_app):
    client, service = worker_app
    patch_resp = client.post("/control/config", json={"target_tps": 40.0, "new_user_transaction_ratio": 0.25})
    assert patch_resp.status_code == 200
    assert service.config.target_tps == 40.0
    assert service.config.new_user_transaction_ratio == 0.25
