"""Unit and integration tests for the Transaction REST API and persistence layer."""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import sessionmaker

from synthetic_streaming.api import create_app
from synthetic_streaming.config import GeneratorConfig
from synthetic_streaming.database import (
    CardModel,
    MerchantCategoryCodeModel,
    TransactionModel,
    UserModel,
    create_db_engine,
    init_db,
    persist_prerequisite_entities,
    persist_transaction,
    seed_reference_data,
)
from synthetic_streaming.generator_service import GeneratorService
from synthetic_streaming.kafka_publisher import MockEventPublisher


def seed_sample_transactions(session, count: int = 30) -> None:
    """Helper to seed sample historical transactions in DB for testing keyset API."""
    now = datetime.now(timezone.utc)
    for i in range(1, count + 1):
        tx = TransactionModel(
            id=i,
            transaction_id=f"tx-hist-{i:04d}",
            event_id=f"00000000-0000-0000-0000-{i:012d}",
            event_type="transaction.created",
            user_id=i % 5,
            card_index=0,
            amount=Decimal(f"{10.0 + i:.2f}"),
            amount_raw=f"${10.0 + i:.2f}",
            use_chip="Chip Transaction",
            transaction_entry_method="chip",
            merchant_name=f"Merchant_{i}",
            merchant_city="Anytown",
            merchant_state="CA",
            merchant_latitude=34.0,
            merchant_longitude=-118.0,
            zip="90001",
            mcc=5411,
            is_fraud="Yes" if i % 7 == 0 else "No",
            scenario="normal_spend",
            source_system="POS_TERMINAL",
            run_id="historical-run",
            created_at=now + timedelta(seconds=i),
            raw_payload={"User": i % 5, "Amount": f"${10.0 + i:.2f}"},
        )
        session.add(tx)
    session.commit()


@pytest.fixture
def transaction_test_env(tmp_path: Path):
    db_file = tmp_path / "test_app.db"
    db_url = f"sqlite:///{db_file}"
    config = GeneratorConfig(
        database_url=db_url,
        target_tps=5.0,
        baseline_data_dir=Path("."),
        auto_seed=True,
    )
    publisher = MockEventPublisher()
    service = GeneratorService(config=config, publisher=publisher)
    app = create_app(service=service)
    client = TestClient(app)
    return client, service, db_url


def test_seed_and_reference_db(transaction_test_env):
    """Verify reference tables are properly seeded and accessible independently."""
    _, service, db_url = transaction_test_env
    engine = create_db_engine(db_url)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()

    users_count = session.query(UserModel).count()
    cards_count = session.query(CardModel).count()
    mcc_count = session.query(MerchantCategoryCodeModel).count()

    assert users_count == 2000
    assert cards_count == 6146
    assert mcc_count == 914

    # Verify relationships
    user0 = session.query(UserModel).filter_by(user_id=0).first()
    assert user0 is not None
    assert user0.person == "Hazel Robinson"
    assert len(user0.cards) == 5

    card0 = user0.cards[0]
    assert card0.card_brand == "Visa"
    assert card0.user_id == 0

    session.close()


def test_batch_generate_streams_to_publisher_and_excludes_db(transaction_test_env):
    """Per PRD: Verify batch generation publishes to Kafka and DOES NOT insert transactions into PostgreSQL."""
    client, service, db_url = transaction_test_env
    engine = create_db_engine(db_url)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()

    # 1. Trigger batch generation of 25 transactions via REST API
    gen_resp = client.post("/api/v1/generator/generate", json={"count": 25, "run_id": "test-run-1"})
    assert gen_resp.status_code == 200
    gen_data = gen_resp.json()
    assert gen_data["generated_count"] == 25
    assert gen_data["run_id"] == "test-run-1"

    # 2. Acceptance Criterion 2: Transactions MUST NOT be inserted into PostgreSQL / relational DB
    db_tx_count = session.query(TransactionModel).count()
    assert db_tx_count == 0, "Transactions must NOT be inserted into relational database"

    # 3. Acceptance Criterion 1: Transactions MUST be published to streaming publisher (Kafka / MSK)
    assert len(service.publisher.published_messages) == 25

    # Verify event structure published to streaming topic
    first_evt = service.publisher.published_messages[0]
    assert first_evt["topic"] == "transactions"
    import json
    val = json.loads(first_evt["value"].decode("utf-8"))
    assert "event_id" in val
    assert val["event_type"] == "transaction.created"
    assert "User" in val["payload"]
    assert "Card" in val["payload"]

    session.close()


def test_pagination_and_incremental_watermarking(transaction_test_env):
    """Verify keyset cursor pagination and monotonic since_id watermarking on historical transactions."""
    client, _, db_url = transaction_test_env
    engine = create_db_engine(db_url)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    seed_sample_transactions(session, count=30)
    session.close()

    # Fetch first page of 10 items
    p1 = client.get("/api/v1/transactions?limit=10")
    assert p1.status_code == 200
    p1_data = p1.json()
    assert p1_data["count"] == 10
    assert p1_data["has_more"] is True
    assert p1_data["next_cursor"] == "10"
    assert p1_data["max_id"] == 10

    # Fetch second page using next_cursor
    p2 = client.get(f"/api/v1/transactions?limit=10&cursor={p1_data['next_cursor']}")
    assert p2.status_code == 200
    p2_data = p2.json()
    assert p2_data["count"] == 10
    assert p2_data["has_more"] is True
    assert p2_data["next_cursor"] == "20"
    assert p2_data["items"][0]["id"] == 11
    assert p2_data["items"][-1]["id"] == 20

    # Verify no overlaps between pages
    p1_ids = {item["id"] for item in p1_data["items"]}
    p2_ids = {item["id"] for item in p2_data["items"]}
    assert p1_ids.isdisjoint(p2_ids)

    # Incremental extraction using since_id (watermark > 20)
    inc_resp = client.get("/api/v1/transactions?since_id=20&limit=50")
    assert inc_resp.status_code == 200
    inc_data = inc_resp.json()
    assert inc_data["count"] == 10
    assert inc_data["items"][0]["id"] == 21
    assert inc_data["items"][-1]["id"] == 30
    assert inc_data["has_more"] is False


def test_transaction_detail_and_filtering(transaction_test_env):
    """Test retrieving transaction by ID and filtering by attributes on historical records."""
    client, _, db_url = transaction_test_env
    engine = create_db_engine(db_url)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    seed_sample_transactions(session, count=20)
    sample_tx = session.query(TransactionModel).filter_by(id=6).first()
    session.close()

    # Get by transaction_id
    detail_resp = client.get(f"/api/v1/transactions/{sample_tx.transaction_id}")
    assert detail_resp.status_code == 200
    d_data = detail_resp.json()
    assert d_data["transaction_id"] == sample_tx.transaction_id
    assert d_data["id"] == sample_tx.id
    assert d_data["user_id"] == sample_tx.user_id

    # Get by event_id
    event_resp = client.get(f"/api/v1/transactions/{sample_tx.event_id}")
    assert event_resp.status_code == 200
    assert event_resp.json()["transaction_id"] == sample_tx.transaction_id

    # Not found
    nf_resp = client.get("/api/v1/transactions/non-existent-id-99999")
    assert nf_resp.status_code == 404

    # Filter by user_id
    user_resp = client.get(f"/api/v1/transactions?user_id={sample_tx.user_id}")
    assert user_resp.status_code == 200
    for it in user_resp.json()["items"]:
        assert it["user_id"] == sample_tx.user_id

    # Filter by scenario
    sc_resp = client.get(f"/api/v1/transactions?scenario={sample_tx.scenario}")
    assert sc_resp.status_code == 200
    for it in sc_resp.json()["items"]:
        assert it["scenario"] == sample_tx.scenario

    # Invalid cursor returns 400
    bad_cursor = client.get("/api/v1/transactions?cursor=not-an-int")
    assert bad_cursor.status_code == 400

    # Invalid is_fraud returns 400
    bad_fraud = client.get("/api/v1/transactions?is_fraud=maybe")
    assert bad_fraud.status_code == 400


def test_historical_transactions_across_service_restart(transaction_test_env):
    """Verify historical transactions remain intact across container/service restarts."""
    client, service, db_url = transaction_test_env
    engine = create_db_engine(db_url)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    seed_sample_transactions(session, count=15)
    session.close()

    resp1 = client.get("/api/v1/transactions?limit=100")
    assert resp1.json()["count"] == 15

    # Simulate container restart: stop service, create a new service & app connected to same DB
    service.stop()

    new_config = GeneratorConfig(
        database_url=db_url,
        baseline_data_dir=Path("."),
        auto_seed=False,
    )
    new_publisher = MockEventPublisher()
    new_service = GeneratorService(config=new_config, publisher=new_publisher)
    new_app = create_app(service=new_service)
    new_client = TestClient(new_app)

    # Verify transactions are preserved
    resp2 = new_client.get("/api/v1/transactions?limit=100")
    assert resp2.status_code == 200
    data2 = resp2.json()
    assert data2["count"] == 15
    assert data2["items"][0]["id"] == 1
    assert data2["items"][-1]["id"] == 15


def test_batch_persistence_atomicity_on_failure(transaction_test_env):
    """Verify that a mid-batch failure rolls back all partial database writes and counters."""
    client, service, db_url = transaction_test_env
    engine = create_db_engine(db_url)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()

    # Pre-condition: 0 transactions in DB
    assert session.query(TransactionModel).count() == 0
    assert service.total_tx_generated == 0

    original_persist = persist_transaction
    call_count = 0

    def mock_persist_tx(sess, envelope, prereq_envelopes=None, commit=True):
        nonlocal call_count
        call_count += 1
        if call_count == 10:
            raise RuntimeError("Simulated DB connection drop on 10th record")
        return original_persist(sess, envelope, prereq_envelopes=prereq_envelopes, commit=commit)

    with patch("synthetic_streaming.generator_service.persist_transaction", side_effect=mock_persist_tx):
        resp = client.post("/api/v1/generator/generate", json={"count": 20, "run_id": "failed-run"})
        assert resp.status_code == 500
        data = resp.json()
        assert "Batch generation failed" in data["detail"]
        assert "Simulated DB connection drop" in data["detail"]

    # Counters should be untouched
    assert service.total_tx_generated == 0
    assert service.total_prereq_events_generated == 0

    # Verify system can still cleanly generate transactions after a failure
    resp_recovery = client.post("/api/v1/generator/generate", json={"count": 10, "run_id": "recovery-run"})
    assert resp_recovery.status_code == 200
    assert resp_recovery.json()["generated_count"] == 10
    assert service.total_tx_generated == 10
    assert len(service.publisher.published_messages) == 10

    session.close()


def test_batch_generate_sanitizes_db_credentials_on_error(transaction_test_env):
    """Verify that if a database exception containing credentials occurs, API response masks them."""
    client, service, _ = transaction_test_env

    with patch.object(
        service,
        "generate_batch",
        side_effect=RuntimeError(
            "Connection failed to postgresql://postgres:MySecretPassword123@rds.amazonaws.com:5432/synthetic_db"
        ),
    ):
        resp = client.post("/api/v1/generator/generate", json={"count": 5})
        assert resp.status_code == 500
        detail = resp.json()["detail"]
        assert "MySecretPassword123" not in detail
        assert "postgresql://postgres:********@rds.amazonaws.com:5432/synthetic_db" in detail


def test_keyset_pagination_order_asc_enforcement(transaction_test_env):
    """Verify keyset pagination and since_id watermark enforce order=asc and reject order=desc."""
    client, _, db_url = transaction_test_env
    engine = create_db_engine(db_url)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    seed_sample_transactions(session, count=20)
    session.close()

    # 1. since_id with order=desc must be rejected with 400
    resp_since_desc = client.get("/api/v1/transactions?since_id=5&order=desc")
    assert resp_since_desc.status_code == 400
    assert "requires order='asc'" in resp_since_desc.json()["detail"]

    # 2. cursor with order=desc must be rejected with 400
    resp_cursor_desc = client.get("/api/v1/transactions?cursor=5&order=desc")
    assert resp_cursor_desc.status_code == 400
    assert "requires order='asc'" in resp_cursor_desc.json()["detail"]

    # 3. since_id without explicit order defaults to asc and returns monotonic IDs > since_id
    resp_since_asc = client.get("/api/v1/transactions?since_id=5&limit=5")
    assert resp_since_asc.status_code == 200
    data_since = resp_since_asc.json()
    ids_since = [it["id"] for it in data_since["items"]]
    assert ids_since == [6, 7, 8, 9, 10]
    assert data_since["has_more"] is True
    assert data_since["next_cursor"] == "10"

    # 4. Standard listing with order=desc (no since_id / cursor) is allowed (dashboard view)
    resp_desc = client.get("/api/v1/transactions?order=desc&limit=5")
    assert resp_desc.status_code == 200
    data_desc = resp_desc.json()
    ids_desc = [it["id"] for it in data_desc["items"]]
    assert ids_desc == [20, 19, 18, 17, 16]
    # Keyset cursor pagination is disabled when sorting descending
    assert data_desc["next_cursor"] is None


def test_fastapi_lifespan_lifecycle_graceful_shutdown():
    """Verify FastAPI lifespan triggers graceful service stop and engine disposal on shutdown."""
    mock_service = MagicMock()
    mock_engine = MagicMock()

    app = create_app(service=mock_service, db_engine=mock_engine)

    with TestClient(app) as test_client:
        resp = test_client.get("/health")
        assert resp.status_code in (200, 503)

    mock_service.stop.assert_called_once_with(timeout=5.0)
    mock_engine.dispose.assert_called_once()
