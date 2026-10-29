import pytest
from fastapi.testclient import TestClient
from synthetic_streaming.api import create_app
from synthetic_streaming.config import GeneratorConfig
from synthetic_streaming.generator_service import GeneratorService
from synthetic_streaming.kafka_publisher import MockEventPublisher


@pytest.fixture
def test_app():
    config = GeneratorConfig(target_tps=5.0)
    publisher = MockEventPublisher()
    service = GeneratorService(config=config, publisher=publisher)
    app = create_app(service)
    return TestClient(app), service


def test_health_endpoint(test_app):
    client, _ = test_app
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_ready_endpoint(test_app):
    client, _ = test_app
    resp = client.get("/ready")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ready"


def test_status_endpoint(test_app):
    client, _ = test_app
    resp = client.get("/api/v1/generator/status")
    assert resp.status_code == 200
    data = resp.json()
    assert "state" in data
    assert "target_tps" in data


def test_config_endpoints(test_app):
    client, service = test_app
    # GET config
    resp = client.get("/api/v1/generator/config")
    assert resp.status_code == 200
    assert resp.json()["target_tps"] == 5.0

    # PATCH config
    patch_resp = client.patch("/api/v1/generator/config", json={"target_tps": 25.0})
    assert patch_resp.status_code == 200
    assert service.config.target_tps == 25.0


def test_start_stop_endpoints(test_app):
    client, service = test_app
    # Start generator
    start_resp = client.post("/api/v1/generator/start")
    assert start_resp.status_code == 200
    assert service.state.value == "RUNNING"

    # Start already running generator should return 409 Conflict
    conflict_resp = client.post("/api/v1/generator/start")
    assert conflict_resp.status_code == 409

    # Stop generator
    stop_resp = client.post("/api/v1/generator/stop")
    assert stop_resp.status_code == 200
    assert service.state.value == "STOPPED"

    # Restart generator to verify repeatability
    restart_resp = client.post("/api/v1/generator/start")
    assert restart_resp.status_code == 200
    assert service.state.value == "RUNNING"
    client.post("/api/v1/generator/stop")


def test_api_auth_enforcement_with_configured_key(tmp_path):
    """Verify endpoints requiring auth reject unauthenticated requests and accept valid credentials."""
    db_file = tmp_path / "auth_test.db"
    config = GeneratorConfig(
        database_url=f"sqlite:///{db_file}",
        api_key="super-secret-key-123",
        target_tps=5.0,
    )
    publisher = MockEventPublisher()
    service = GeneratorService(config=config, publisher=publisher)
    app = create_app(service=service, config=config)
    client = TestClient(app)

    # 1. Missing auth token returns 401
    resp_no_auth = client.post("/api/v1/generator/start")
    assert resp_no_auth.status_code == 401
    assert "Authentication token required" in resp_no_auth.json()["detail"]

    # 2. Invalid auth token returns 403
    resp_bad_auth = client.post("/api/v1/generator/start", headers={"X-API-Key": "wrong-key"})
    assert resp_bad_auth.status_code == 403
    assert "Invalid authentication credentials" in resp_bad_auth.json()["detail"]

    # 3. Valid X-API-Key header returns 200
    resp_good_header = client.post("/api/v1/generator/start", headers={"X-API-Key": "super-secret-key-123"})
    assert resp_good_header.status_code == 200
    assert service.is_running is True

    # 4. Valid Bearer Authorization header returns 200
    resp_bearer = client.post("/api/v1/generator/stop", headers={"Authorization": "Bearer super-secret-key-123"})
    assert resp_bearer.status_code == 200
    assert service.is_running is False


def test_create_app_auto_binds_embedded_generator_service(tmp_path):
    """Verify create_app() auto-binds an embedded GeneratorService when none is passed."""
    db_file = tmp_path / "auto_embed.db"
    config = GeneratorConfig(
        database_url=f"sqlite:///{db_file}",
        target_tps=10.0,
        auto_seed=False,
    )
    # Instantiate without passing service
    app = create_app(service=None, config=config)
    client = TestClient(app)

    # /health should return standalone mode with operational generator state
    health_resp = client.get("/health")
    assert health_resp.status_code == 200
    h_data = health_resp.json()
    assert h_data["mode"] == "standalone"
    assert h_data["generator_state"] == "STOPPED"

    # /api/v1/generator/status should return active status directly
    status_resp = client.get("/api/v1/generator/status")
    assert status_resp.status_code == 200
    assert status_resp.json()["state"] == "STOPPED"

