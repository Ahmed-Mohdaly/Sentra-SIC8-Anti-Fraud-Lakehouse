"""Unit and integration tests for transaction classification taxonomy and geographic coordinates."""

import csv
from decimal import Decimal
from pathlib import Path
import pytest
from pydantic import ValidationError
from sqlalchemy.orm import sessionmaker

from synthetic_streaming.classification_and_geo import (
    ALL_VALID_ENTRY_METHODS,
    ENTRY_METHODS_BY_USE_CHIP,
    CityResolver,
    is_valid_entry_method,
    normalize_city_state,
    sample_entry_method,
    validate_coordinates,
)
from synthetic_streaming.database import (
    Base,
    CityReferenceModel,
    TransactionModel,
    create_db_engine,
    persist_transaction,
    seed_reference_data,
)
from synthetic_streaming.event_schema import EventEnvelope, TransactionPayload
from synthetic_streaming.api import create_app
from fastapi.testclient import TestClient


# ==============================================================================
# 1. Geographic Coordinate Validation Tests
# ==============================================================================

def test_validate_coordinates_valid():
    assert validate_coordinates(34.0522, -118.2437) is True
    assert validate_coordinates(-33.8688, 151.2093) is True
    assert validate_coordinates(0.0, 0.0) is True
    assert validate_coordinates(90.0, 180.0) is True
    assert validate_coordinates(-90.0, -180.0) is True
    assert validate_coordinates(None, None) is True


def test_validate_coordinates_out_of_bounds():
    assert validate_coordinates(90.1, 0.0) is False
    assert validate_coordinates(-90.1, 0.0) is False
    assert validate_coordinates(0.0, 180.1) is False
    assert validate_coordinates(0.0, -180.1) is False


def test_validate_coordinates_co_occurrence():
    # Latitude and longitude must be either both present or both absent
    assert validate_coordinates(34.0, None) is False
    assert validate_coordinates(None, -118.0) is False


# ==============================================================================
# 2. Transaction Entry Method Taxonomy & Compatibility Tests
# ==============================================================================

def test_taxonomy_definitions():
    assert "magnetic_stripe_swipe" in ENTRY_METHODS_BY_USE_CHIP["Swipe Transaction"]
    assert "contact_chip" in ENTRY_METHODS_BY_USE_CHIP["Chip Transaction"]
    assert "contactless_chip" in ENTRY_METHODS_BY_USE_CHIP["Chip Transaction"]
    assert "ecommerce" in ENTRY_METHODS_BY_USE_CHIP["Online Transaction"]
    assert "food_delivery" in ENTRY_METHODS_BY_USE_CHIP["Online Transaction"]
    assert "digital_subscription" in ENTRY_METHODS_BY_USE_CHIP["Online Transaction"]
    assert "digital_goods" in ENTRY_METHODS_BY_USE_CHIP["Online Transaction"]
    assert "online_bill_payment" in ENTRY_METHODS_BY_USE_CHIP["Online Transaction"]
    assert "in_app_purchase" in ENTRY_METHODS_BY_USE_CHIP["Online Transaction"]
    assert "online_service_payment" in ENTRY_METHODS_BY_USE_CHIP["Online Transaction"]
    assert "unknown" in ALL_VALID_ENTRY_METHODS


def test_is_valid_entry_method_compatibility():
    # Valid pairings
    assert is_valid_entry_method("Swipe Transaction", "magnetic_stripe_swipe") is True
    assert is_valid_entry_method("Swipe Transaction", "unknown") is True
    assert is_valid_entry_method("Chip Transaction", "contact_chip") is True
    assert is_valid_entry_method("Chip Transaction", "contactless_chip") is True
    assert is_valid_entry_method("Online Transaction", "ecommerce") is True
    assert is_valid_entry_method("Online Transaction", "food_delivery") is True

    # Incompatible pairings
    assert is_valid_entry_method("Swipe Transaction", "contact_chip") is False
    assert is_valid_entry_method("Swipe Transaction", "ecommerce") is False
    assert is_valid_entry_method("Chip Transaction", "magnetic_stripe_swipe") is False
    assert is_valid_entry_method("Chip Transaction", "ecommerce") is False
    assert is_valid_entry_method("Online Transaction", "magnetic_stripe_swipe") is False
    assert is_valid_entry_method("Online Transaction", "contact_chip") is False

    # Unknown or None is always safe
    assert is_valid_entry_method("Swipe Transaction", None) is True
    assert is_valid_entry_method("Chip Transaction", "unknown") is True


def test_sample_entry_method_rules():
    # Physical swipe
    assert sample_entry_method("Swipe Transaction", "POS_TERMINAL", 5411) == "magnetic_stripe_swipe"

    # Digital wallet tap-to-pay
    assert sample_entry_method("Chip Transaction", "DIGITAL_WALLET", 5411) == "contactless_chip"

    # ATM requires physical chip insert
    assert sample_entry_method("Chip Transaction", "ATM_NETWORK", 6011) == "contact_chip"

    # Recurring billing for utilities
    assert sample_entry_method("Online Transaction", "RECURRING_BILLING", 4900) == "online_bill_payment"

    # Food delivery MCC
    food_res = sample_entry_method("Online Transaction", "MOBILE_APP", 5814)
    assert food_res in {"food_delivery", "ecommerce"}


# ==============================================================================
# 3. City Reference & Resolver Tests
# ==============================================================================

def test_city_resolver_matching_and_duplicates():
    resolver = CityResolver()
    resolver.populate([
        {"city_name": "Springfield", "state_code": "IL", "latitude": 39.7817, "longitude": -89.6501},
        {"city_name": "Springfield", "state_code": "MO", "latitude": 37.2089, "longitude": -93.2923},
        {"city_name": "Monterey Park", "state_code": "CA", "latitude": 34.0625, "longitude": -118.1228},
    ])

    # Case-insensitive resolution
    lat_il, lon_il = resolver.resolve("springfield", "il")
    assert lat_il == pytest.approx(39.7817)
    assert lon_il == pytest.approx(-89.6501)

    lat_mo, lon_mo = resolver.resolve("SPRINGFIELD", "MO")
    assert lat_mo == pytest.approx(37.2089)
    assert lon_mo == pytest.approx(-93.2923)

    # Online returns (None, None)
    assert resolver.resolve("ONLINE", "CA") == (None, None)
    assert resolver.resolve("online", None) == (None, None)

    # Missing / unmatched returns (None, None) without fabricating (0, 0)
    assert resolver.resolve("NonExistentCity", "ZZ") == (None, None)
    assert resolver.resolve(None, "CA") == (None, None)


# ==============================================================================
# 4. Pydantic TransactionPayload Validation Tests
# ==============================================================================

def test_transaction_payload_valid():
    payload = TransactionPayload(
        User=0,
        Card=0,
        Year=2023,
        Month=5,
        Day=12,
        Time="14:30",
        Amount="$25.50",
        Use_Chip="Chip Transaction",
        Transaction_Entry_Method="contact_chip",
        Merchant_Name="Grocery Store",
        Merchant_City="Monterey Park",
        Merchant_State="CA",
        Merchant_Latitude=34.0625,
        Merchant_Longitude=-118.1228,
        MCC=5411,
    )
    assert payload.Transaction_Entry_Method == "contact_chip"
    assert payload.Merchant_Latitude == 34.0625
    assert payload.Merchant_Longitude == -118.1228


def test_transaction_payload_incompatible_method():
    with pytest.raises(ValidationError) as exc_info:
        TransactionPayload(
            User=0,
            Card=0,
            Year=2023,
            Month=5,
            Day=12,
            Time="14:30",
            Amount="$25.50",
            Use_Chip="Swipe Transaction",
            Transaction_Entry_Method="contact_chip",  # Incompatible with Swipe
            Merchant_Name="Store",
            Merchant_City="City",
            MCC=5411,
        )
    assert "Invalid Transaction Entry Method" in str(exc_info.value)


def test_transaction_payload_invalid_coordinates():
    # Latitude out of bounds
    with pytest.raises(ValidationError):
        TransactionPayload(
            User=0,
            Card=0,
            Year=2023,
            Month=5,
            Day=12,
            Time="14:30",
            Amount="$25.50",
            Use_Chip="Chip Transaction",
            Merchant_Name="Store",
            Merchant_City="City",
            Merchant_Latitude=95.0,  # Invalid
            Merchant_Longitude=-118.0,
            MCC=5411,
        )

    # Only one coordinate present
    with pytest.raises(ValidationError):
        TransactionPayload(
            User=0,
            Card=0,
            Year=2023,
            Month=5,
            Day=12,
            Time="14:30",
            Amount="$25.50",
            Use_Chip="Chip Transaction",
            Merchant_Name="Store",
            Merchant_City="City",
            Merchant_Latitude=34.0,
            Merchant_Longitude=None,  # Incomplete
            MCC=5411,
        )


# ==============================================================================
# 5. Database Persistence & Migration Tests
# ==============================================================================

def test_database_persistence_with_classification_and_geo():
    engine = create_db_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine, expire_on_commit=False)
    session = Session()

    # Pre-populate reference city
    ref_city = CityReferenceModel(
        city_name="Pasadena",
        state_code="CA",
        country_code="US",
        latitude=34.1478,
        longitude=-118.1445,
        coordinate_source="US Census Gazetteer",
        coordinate_precision="city_center",
    )
    session.add(ref_city)
    session.commit()

    envelope = EventEnvelope(
        event_id="evt-1001",
        event_type="transaction.created",
        source_system="POS_TERMINAL",
        scenario="normal_spend",
        payload={
            "User": 10,
            "Card": 0,
            "Amount": "$42.15",
            "Use Chip": "Chip Transaction",
            "Transaction Entry Method": "contactless_chip",
            "Merchant Name": "Cafe",
            "Merchant City": "Pasadena",
            "Merchant State": "CA",
            "Zip": "91101",
            "MCC": 5814,
        },
    )

    # Persist and verify automatic coordinate resolution from city_reference
    tx_rec = persist_transaction(session, envelope)
    assert tx_rec.transaction_entry_method == "contactless_chip"
    assert tx_rec.merchant_latitude == pytest.approx(34.1478)
    assert tx_rec.merchant_longitude == pytest.approx(-118.1445)

    # Test dictionary serialization
    d = tx_rec.to_dict()
    assert d["transaction_entry_method"] == "contactless_chip"
    assert d["merchant_latitude"] == pytest.approx(34.1478)
    assert d["merchant_longitude"] == pytest.approx(-118.1445)
    session.close()


# ==============================================================================
# 6. REST API Endpoint & Filtering Tests
# ==============================================================================

def test_api_transactions_with_classification_and_filter():
    engine = create_db_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine, expire_on_commit=False)
    session = Session()

    # Create test transactions with different entry methods
    tx1 = TransactionModel(
        transaction_id="tx-1",
        event_id="evt-1",
        event_type="transaction.created",
        user_id=1,
        card_index=0,
        amount=Decimal("15.00"),
        amount_raw="$15.00",
        use_chip="Swipe Transaction",
        transaction_entry_method="magnetic_stripe_swipe",
        merchant_name="Gas Station",
        merchant_city="La Verne",
        merchant_state="CA",
        merchant_latitude=34.1008,
        merchant_longitude=-117.7678,
        mcc=5541,
        is_fraud="No",
        scenario="normal_spend",
        source_system="POS_TERMINAL",
        run_id="run-1",
    )
    tx2 = TransactionModel(
        transaction_id="tx-2",
        event_id="evt-2",
        event_type="transaction.created",
        user_id=1,
        card_index=0,
        amount=Decimal("99.00"),
        amount_raw="$99.00",
        use_chip="Online Transaction",
        transaction_entry_method="ecommerce",
        merchant_name="Online Store",
        merchant_city="ONLINE",
        merchant_state=None,
        merchant_latitude=None,
        merchant_longitude=None,
        mcc=5311,
        is_fraud="No",
        scenario="normal_spend",
        source_system="ECOMMERCE_WEB",
        run_id="run-1",
    )
    session.add_all([tx1, tx2])
    session.commit()
    session.close()

    app = create_app(db_engine=engine)
    client = TestClient(app)

    # 1. Fetch all transactions
    resp = client.get("/api/v1/transactions")
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 2
    item1 = [it for it in data["items"] if it["transaction_id"] == "tx-1"][0]
    assert item1["transaction_entry_method"] == "magnetic_stripe_swipe"
    assert item1["merchant_latitude"] == pytest.approx(34.1008)

    item2 = [it for it in data["items"] if it["transaction_id"] == "tx-2"][0]
    assert item2["transaction_entry_method"] == "ecommerce"
    assert item2["merchant_latitude"] is None

    # 2. Filter by transaction_entry_method
    resp_filtered = client.get("/api/v1/transactions?transaction_entry_method=ecommerce")
    assert resp_filtered.status_code == 200
    data_filtered = resp_filtered.json()
    assert data_filtered["count"] == 1
    assert data_filtered["items"][0]["transaction_id"] == "tx-2"


def test_api_reference_cities_endpoint():
    engine = create_db_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine, expire_on_commit=False)
    session = Session()
    c1 = CityReferenceModel(
        city_name="Burbank",
        state_code="CA",
        country_code="US",
        latitude=34.1808,
        longitude=-118.3090,
        coordinate_source="US Census Gazetteer",
        coordinate_precision="city_center",
    )
    session.add(c1)
    session.commit()
    session.close()

    app = create_app(db_engine=engine)
    client = TestClient(app)

    # Fetch reference cities
    resp = client.get("/api/v1/reference/cities?city=Burbank")
    assert resp.status_code == 200
    cities = resp.json()
    assert len(cities) == 1
    assert cities[0]["city_name"] == "Burbank"
    assert cities[0]["state_code"] == "CA"
    assert cities[0]["latitude"] == pytest.approx(34.1808)


# ==============================================================================
# 7. Idempotent Seeding & Generator Integration Tests
# ==============================================================================

def test_idempotent_reference_seeding(tmp_path):
    engine = create_db_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    # Seed once
    counts1 = seed_reference_data(engine, data_dir=".", recreate=False)
    assert counts1["city_reference"] > 0

    # Seed second time without recreate - counts should match and not duplicate
    counts2 = seed_reference_data(engine, data_dir=".", recreate=False)
    assert counts2["city_reference"] == counts1["city_reference"]


def test_generator_service_populates_classification_and_geo():
    from synthetic_streaming.config import GeneratorConfig
    from synthetic_streaming.generator_service import GeneratorService

    cfg = GeneratorConfig(database_url="sqlite:///:memory:", target_tps=50.0)
    service = GeneratorService(config=cfg)

    # Generate batch of 20 transactions
    records = service.generate_batch(count=20, run_id="test-geo-run")
    assert len(records) == 20

    for r in records:
        # Verify original Use Chip is preserved
        assert r.use_chip in {"Swipe Transaction", "Chip Transaction", "Online Transaction"}
        # Verify new detailed classification is assigned and compatible
        assert r.transaction_entry_method in ALL_VALID_ENTRY_METHODS
        assert is_valid_entry_method(r.use_chip, r.transaction_entry_method) is True

        # Verify coordinates: either both present (and valid) or both None
        assert validate_coordinates(r.merchant_latitude, r.merchant_longitude) is True
        if r.merchant_city and r.merchant_city.upper() == "ONLINE":
            assert r.merchant_latitude is None
            assert r.merchant_longitude is None


def test_enrich_historical_dataset_workflow(tmp_path):
    from scripts.enrich_historical_data import enrich_historical_dataset

    # Create small mock historical CSV
    input_csv = tmp_path / "mock_hist.csv"
    with open(input_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["User", "Card", "Year", "Month", "Day", "Time", "Amount", "Use Chip", "Merchant Name", "Merchant City", "Merchant State", "Zip", "MCC", "Errors?", "Is Fraud?"])
        writer.writerow([0, 0, 2020, 1, 1, "12:00", "$10.00", "Swipe Transaction", "Store 1", "La Verne", "CA", "91750", "5411", "", "No"])
        writer.writerow([0, 0, 2020, 1, 2, "13:00", "$50.00", "Online Transaction", "Store 2", "ONLINE", "", "", "5311", "", "No"])
        writer.writerow([0, 0, 2020, 1, 3, "14:00", "$25.00", "Chip Transaction", "Store 3", "UnknownCityXYZ", "ZZ", "99999", "5812", "", "No"])

    out_csv = tmp_path / "mock_hist_enriched.csv"
    report = enrich_historical_dataset(input_csv, out_csv, Path("city_reference.csv"), randomize_entry_method=True, seed=42)

    assert report["total_rows_processed"] == 3
    assert report["matched_with_coordinates"] == 1  # La Verne, CA matches
    assert report["online_transactions_null_coords"] == 1  # ONLINE
    assert report["unmatched_cities"] == 1  # UnknownCityXYZ
    assert report["entry_method_unknown_rate_percentage"] == 0.0
    assert "magnetic_stripe_swipe" in report["entry_method_distribution"]

    # Verify input file was untouched
    assert input_csv.exists()
    assert out_csv.exists()

    # Read output and verify columns
    with open(out_csv, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        hdr = next(reader)
        assert "Transaction Entry Method" in hdr
        assert "Merchant Latitude" in hdr
        assert "Merchant Longitude" in hdr

        r1 = next(reader)
        assert r1[hdr.index("Transaction Entry Method")] == "magnetic_stripe_swipe"
        assert float(r1[hdr.index("Merchant Latitude")]) == pytest.approx(34.1008, rel=1e-2)

        r2 = next(reader)
        assert r2[hdr.index("Transaction Entry Method")] in ALL_VALID_ENTRY_METHODS
        assert r2[hdr.index("Transaction Entry Method")] != "unknown"
        assert r2[hdr.index("Merchant Latitude")] == ""  # None

        r3 = next(reader)
        assert r3[hdr.index("Transaction Entry Method")] in {"contact_chip", "contactless_chip"}
        assert r3[hdr.index("Merchant Latitude")] == ""  # Unmatched

    # Verify legacy mode preserving 'unknown'
    out_legacy_csv = tmp_path / "mock_hist_legacy.csv"
    report_legacy = enrich_historical_dataset(input_csv, out_legacy_csv, Path("city_reference.csv"), randomize_entry_method=False)
    assert report_legacy["entry_method_unknown_rate_percentage"] == 100.0
    with open(out_legacy_csv, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        hdr = next(reader)
        r1_legacy = next(reader)
        assert r1_legacy[hdr.index("Transaction Entry Method")] == "unknown"

