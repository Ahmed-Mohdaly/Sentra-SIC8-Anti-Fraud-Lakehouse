"""Unit tests for the scheduled batch reference extractor."""

from datetime import datetime, timezone
import io
import pytest
import pyarrow.parquet as pq
from unittest.mock import MagicMock
from sqlalchemy import create_engine, text

from ingestion.batch_reference_extractor import (
    BatchReferenceExtractor,
    MCC_ARROW_SCHEMA,
    CITY_ARROW_SCHEMA,
)


@pytest.fixture
def sqlite_ref_db(tmp_path):
    """Create a temporary SQLite database with reference tables matching PostgreSQL schema."""
    db_path = tmp_path / "ref_test.db"
    db_url = f"sqlite:///{db_path}"
    engine = create_engine(db_url)

    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE merchant_category_codes (
                mcc VARCHAR(16) PRIMARY KEY,
                description TEXT,
                category VARCHAR(255),
                valid_payment_brands VARCHAR(255),
                required_abbreviation VARCHAR(64)
            );
        """))
        conn.execute(text("""
            INSERT INTO merchant_category_codes (mcc, description, category, valid_payment_brands, required_abbreviation) VALUES
            ('5411', 'Groceries and retail food', 'Grocery Stores, Supermarkets', 'Visa,Mastercard', 'GROC'),
            ('5812', 'Food and beverage dining', 'Eating Places, Restaurants', 'Visa,Mastercard,Amex', 'REST');
        """))

        conn.execute(text("""
            CREATE TABLE city_reference (
                city_id INTEGER PRIMARY KEY,
                city_name VARCHAR(128),
                state_code VARCHAR(32),
                country_code VARCHAR(32),
                latitude FLOAT,
                longitude FLOAT,
                coordinate_source VARCHAR(64),
                coordinate_precision VARCHAR(64)
            );
        """))
        conn.execute(text("""
            INSERT INTO city_reference (city_id, city_name, state_code, country_code, latitude, longitude, coordinate_source, coordinate_precision) VALUES
            (1, 'New York', 'NY', 'US', 40.7128, -74.0060, 'USPS', 'EXACT'),
            (2, 'Los Angeles', 'CA', 'US', 34.0522, -118.2437, 'USPS', 'EXACT');
        """))
        conn.commit()

    return db_url


def test_batch_reference_extractor_flow(sqlite_ref_db):
    mock_s3 = MagicMock()
    mock_s3.put_object.return_value = {"ETag": "test-etag"}

    extractor = BatchReferenceExtractor(
        db_url=sqlite_ref_db,
        s3_bucket="test-bronze-bucket",
        region="eu-west-3",
        s3_client=mock_s3,
    )

    results = extractor.run_all()

    # Verify MCC
    assert "mcc" in results
    assert results["mcc"]["stats"]["record_count"] == 2
    assert results["mcc"]["stats"]["sample_mcc"] == "5411"
    assert results["mcc"]["s3_key"].startswith("bronze/mcc/snapshot_date=")

    # Verify Cities
    assert "cities" in results
    assert results["cities"]["stats"]["record_count"] == 2
    assert results["cities"]["stats"]["min_city_id"] == 1
    assert results["cities"]["stats"]["max_city_id"] == 2
    assert results["cities"]["s3_key"].startswith("bronze/cities/snapshot_date=")

    # Verify S3 upload calls
    assert mock_s3.put_object.call_count == 2
    call_args_list = mock_s3.put_object.call_args_list

    # Check Parquet content of first call (MCC)
    mcc_call_kwargs = call_args_list[0].kwargs
    assert mcc_call_kwargs["Bucket"] == "test-bronze-bucket"
    assert "mcc" in mcc_call_kwargs["Key"]
    mcc_buf = io.BytesIO(mcc_call_kwargs["Body"])
    mcc_table = pq.read_table(mcc_buf)
    assert mcc_table.num_rows == 2
    assert set(mcc_table.column_names) == {
        "mcc",
        "description",
        "category",
        "valid_payment_brands",
        "required_abbreviation",
        "extraction_timestamp",
        "source_system",
    }

    # Check Parquet content of second call (Cities)
    cities_call_kwargs = call_args_list[1].kwargs
    assert cities_call_kwargs["Bucket"] == "test-bronze-bucket"
    assert "cities" in cities_call_kwargs["Key"]
    city_buf = io.BytesIO(cities_call_kwargs["Body"])
    city_table = pq.read_table(city_buf)
    assert city_table.num_rows == 2
    assert set(city_table.column_names) == {
        "city_id",
        "city_name",
        "state_code",
        "country_code",
        "latitude",
        "longitude",
        "coordinate_source",
        "coordinate_precision",
        "extraction_timestamp",
        "source_system",
    }
