"""Database models, connection lifecycle, and reference dataset seeding."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from decimal import Decimal
import json
import logging
from pathlib import Path
import re
from typing import Any, Dict, Generator, List, Optional, Tuple, Union

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    create_engine,
    func,
    inspect,
    text,
)
from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base, relationship, sessionmaker, Session
from sqlalchemy.pool import StaticPool
from sqlalchemy.types import JSON

logger = logging.getLogger(__name__)

Base = declarative_base()


class UserModel(Base):
    """Reference table for baseline and dynamically generated user entities."""
    __tablename__ = "users"

    user_id = Column(BigInteger, primary_key=True, index=True)
    person = Column(String(255), nullable=False)
    current_age = Column(Integer, nullable=False)
    retirement_age = Column(Integer, nullable=False)
    birth_year = Column(Integer, nullable=False)
    birth_month = Column(Integer, nullable=False)
    gender = Column(String(32), nullable=False)
    address = Column(String(255), nullable=False)
    apartment = Column(String(64), nullable=True)
    city = Column(String(128), nullable=False)
    state = Column(String(32), nullable=False, index=True)
    zipcode = Column(Integer, nullable=False, index=True)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    per_capita_income = Column(String(64), nullable=False)
    yearly_income = Column(String(64), nullable=False)
    total_debt = Column(String(64), nullable=False)
    fico_score = Column(Integer, nullable=False)
    num_credit_cards = Column(Integer, nullable=False)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)

    cards = relationship("CardModel", back_populates="user", cascade="all, delete-orphan")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "user_id": self.user_id,
            "person": self.person,
            "current_age": self.current_age,
            "retirement_age": self.retirement_age,
            "birth_year": self.birth_year,
            "birth_month": self.birth_month,
            "gender": self.gender,
            "address": self.address,
            "apartment": self.apartment,
            "city": self.city,
            "state": self.state,
            "zipcode": self.zipcode,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "per_capita_income": self.per_capita_income,
            "yearly_income": self.yearly_income,
            "total_debt": self.total_debt,
            "fico_score": self.fico_score,
            "num_credit_cards": self.num_credit_cards,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class CardModel(Base):
    """Reference table for card accounts issued to users."""
    __tablename__ = "cards"

    user_id = Column(BigInteger, ForeignKey("users.user_id", ondelete="CASCADE"), primary_key=True)
    card_index = Column(Integer, primary_key=True)
    card_brand = Column(String(64), nullable=False)
    card_type = Column(String(64), nullable=False)
    card_number = Column(String(64), nullable=False)
    expires = Column(String(16), nullable=False)
    cvv = Column(String(8), nullable=False)
    has_chip = Column(String(8), nullable=False)
    cards_issued = Column(Integer, nullable=False)
    credit_limit = Column(String(32), nullable=False)
    acct_open_date = Column(String(16), nullable=False)
    year_pin_last_changed = Column(Integer, nullable=False)
    card_on_dark_web = Column(String(8), nullable=False)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)

    user = relationship("UserModel", back_populates="cards")

    __table_args__ = (
        Index("idx_cards_user_index", "user_id", "card_index"),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "user_id": self.user_id,
            "card_index": self.card_index,
            "card_brand": self.card_brand,
            "card_type": self.card_type,
            "card_number": self.card_number,
            "expires": self.expires,
            "cvv": self.cvv,
            "has_chip": self.has_chip,
            "cards_issued": self.cards_issued,
            "credit_limit": self.credit_limit,
            "acct_open_date": self.acct_open_date,
            "year_pin_last_changed": self.year_pin_last_changed,
            "card_on_dark_web": self.card_on_dark_web,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class MerchantCategoryCodeModel(Base):
    """Reference table for merchant category codes and industry groupings."""
    __tablename__ = "merchant_category_codes"

    mcc = Column(String(8), primary_key=True)
    description = Column(String(255), nullable=False)
    valid_payment_brands = Column(String(64), nullable=True)
    category = Column(String(128), nullable=False, index=True)
    required_abbreviation = Column(String(64), nullable=True)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "mcc": self.mcc,
            "description": self.description,
            "valid_payment_brands": self.valid_payment_brands,
            "category": self.category,
            "required_abbreviation": self.required_abbreviation,
        }


class CityReferenceModel(Base):
    """Reference table for city geographic coordinates and metadata."""
    __tablename__ = "city_reference"

    city_id = Column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True)
    city_name = Column(String(128), nullable=False)
    state_code = Column(String(32), nullable=False)
    country_code = Column(String(8), nullable=False, default="US")
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    coordinate_source = Column(String(128), nullable=False)
    coordinate_precision = Column(String(64), nullable=False, default="city_center")
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)

    __table_args__ = (
        Index("idx_city_ref_lookup", "city_name", "state_code", unique=True),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "city_id": self.city_id,
            "city_name": self.city_name,
            "state_code": self.state_code,
            "country_code": self.country_code,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "coordinate_source": self.coordinate_source,
            "coordinate_precision": self.coordinate_precision,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class TransactionModel(Base):
    """Persistent storage for generated financial transactions."""
    __tablename__ = "transactions"

    id = Column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True)
    transaction_id = Column(String(64), unique=True, index=True, nullable=False)
    event_id = Column(String(64), unique=True, index=True, nullable=False)
    event_type = Column(String(64), nullable=False, default="transaction.created")
    user_id = Column(BigInteger, nullable=False, index=True)
    card_index = Column(Integer, nullable=False)
    amount = Column(Numeric(12, 2), nullable=False)
    amount_raw = Column(String(32), nullable=False)
    use_chip = Column(String(64), nullable=False)
    transaction_entry_method = Column(String(64), nullable=True, index=True)
    merchant_name = Column(String(128), nullable=False)
    merchant_city = Column(String(128), nullable=True)
    merchant_state = Column(String(32), nullable=True)
    merchant_latitude = Column(Float, nullable=True)
    merchant_longitude = Column(Float, nullable=True)
    zip = Column(String(32), nullable=True)
    mcc = Column(Integer, nullable=False, index=True)
    errors = Column(String(128), nullable=True)
    is_fraud = Column(String(8), nullable=False, default="No")
    scenario = Column(String(64), nullable=False, index=True)
    source_system = Column(String(64), nullable=False)
    run_id = Column(String(64), nullable=False)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False, index=True)
    raw_payload = Column(JSON, nullable=True)

    __table_args__ = (
        Index("idx_tx_created_at_id", "created_at", "id"),
        Index("idx_tx_user_card", "user_id", "card_index"),
        Index("idx_tx_scenario_fraud", "scenario", "is_fraud"),
        Index("idx_tx_entry_method", "transaction_entry_method"),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "transaction_id": self.transaction_id,
            "event_id": self.event_id,
            "event_type": self.event_type,
            "user_id": self.user_id,
            "card_index": self.card_index,
            "amount": f"{self.amount:.2f}",
            "amount_raw": self.amount_raw,
            "use_chip": self.use_chip,
            "transaction_entry_method": self.transaction_entry_method,
            "merchant_name": self.merchant_name,
            "merchant_city": self.merchant_city,
            "merchant_state": self.merchant_state,
            "merchant_latitude": self.merchant_latitude,
            "merchant_longitude": self.merchant_longitude,
            "zip": self.zip,
            "mcc": self.mcc,
            "errors": self.errors,
            "is_fraud": self.is_fraud,
            "scenario": self.scenario,
            "source_system": self.source_system,
            "run_id": self.run_id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "payload": self.raw_payload,
        }


def sanitize_db_error(error: Union[Exception, str]) -> str:
    """Sanitize database error messages to prevent password or sensitive credential leakage."""
    msg = str(error)
    # Mask passwords in URLs: postgresql://user:pass@host:port/db -> postgresql://user:********@host:port/db
    sanitized = re.sub(r"://([^:@/\s]+):([^@/\s]+)@", r"://\1:********@", msg)
    # Mask standalone password=... parameter if present
    sanitized = re.sub(r"password=([^\s,]+)", r"password=********", sanitized)
    return sanitized


def create_db_engine(database_url: str) -> Engine:
    """Create SQLAlchemy engine configured for PostgreSQL or SQLite."""
    if database_url.startswith("sqlite"):
        if ":memory:" in database_url:
            return create_engine(
                database_url,
                connect_args={"check_same_thread": False},
                poolclass=StaticPool,
                hide_parameters=True,
            )
        return create_engine(
            database_url,
            connect_args={"check_same_thread": False},
            hide_parameters=True,
        )
    if database_url.startswith("postgresql://"):
        database_url = database_url.replace("postgresql://", "postgresql+psycopg2://", 1)

    return create_engine(
        database_url,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20,
        hide_parameters=True,
    )


def run_migrations(engine: Engine) -> None:
    """Apply safe, backward-compatible schema migrations to existing database tables."""
    inspector = inspect(engine)
    table_names = inspector.get_table_names()
    if "transactions" in table_names:
        existing_cols = {col["name"] for col in inspector.get_columns("transactions")}
        with engine.begin() as conn:
            if "transaction_entry_method" not in existing_cols:
                logger.info("Migrating transactions: adding transaction_entry_method column")
                conn.execute(text("ALTER TABLE transactions ADD COLUMN transaction_entry_method VARCHAR(64)"))
            if "merchant_latitude" not in existing_cols:
                logger.info("Migrating transactions: adding merchant_latitude column")
                conn.execute(text("ALTER TABLE transactions ADD COLUMN merchant_latitude FLOAT"))
            if "merchant_longitude" not in existing_cols:
                logger.info("Migrating transactions: adding merchant_longitude column")
                conn.execute(text("ALTER TABLE transactions ADD COLUMN merchant_longitude FLOAT"))

            try:
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_tx_entry_method ON transactions (transaction_entry_method)"))
            except Exception as e:
                logger.debug(f"Index creation note: {e}")


def init_db(engine: Engine) -> None:
    """Create all database tables if they do not already exist and apply schema migrations."""
    Base.metadata.create_all(engine)
    run_migrations(engine)
    logger.info("Database schema initialized and migrations verified successfully.")


def check_db_health(engine: Engine) -> Dict[str, Any]:
    """Execute simple connectivity check with sanitized error reporting."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "healthy", "database": engine.name}
    except Exception as e:
        clean_err = sanitize_db_error(e)
        logger.error(f"Database health check failed: {clean_err}")
        return {"status": "unhealthy", "database": engine.name, "error": clean_err}


def seed_reference_data(
    engine: Engine,
    data_dir: Union[str, Path] = ".",
    recreate: bool = False,
) -> Dict[str, int]:
    """Seed reference tables (users, cards, merchant_category_codes) from baseline CSVs."""
    p_data_dir = Path(data_dir)
    if not (p_data_dir / "sd254_users.csv").exists():
        if (p_data_dir / "data" / "sd254_users.csv").exists():
            p_data_dir = p_data_dir / "data"
        else:
            repo_data = Path(__file__).resolve().parent.parent.parent / "data"
            if (repo_data / "sd254_users.csv").exists():
                p_data_dir = repo_data
    init_db(engine)

    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    session = SessionLocal()

    seeded_counts = {"users": 0, "cards": 0, "merchant_category_codes": 0, "city_reference": 0}

    try:
        # Check if users already seeded
        users_count = session.query(UserModel).count()
        if recreate or users_count == 0:
            if recreate:
                session.query(CardModel).delete()
                session.query(UserModel).delete()
                session.commit()

            users_file = p_data_dir / "sd254_users.csv"
            if users_file.exists():
                logger.info(f"Seeding users from {users_file}...")
                user_objects: List[UserModel] = []
                with open(users_file, "r", encoding="utf-8-sig") as f:
                    reader = csv.reader(f)
                    next(reader)  # Header
                    for idx, row in enumerate(reader):
                        if not row or len(row) < 18:
                            continue
                        user_objects.append(
                            UserModel(
                                user_id=idx,
                                person=row[0].strip(),
                                current_age=int(row[1]),
                                retirement_age=int(row[2]),
                                birth_year=int(row[3]),
                                birth_month=int(row[4]),
                                gender=row[5].strip(),
                                address=row[6].strip(),
                                apartment=row[7].strip() if row[7].strip() else None,
                                city=row[8].strip(),
                                state=row[9].strip(),
                                zipcode=int(row[10]),
                                latitude=float(row[11]),
                                longitude=float(row[12]),
                                per_capita_income=row[13].strip(),
                                yearly_income=row[14].strip(),
                                total_debt=row[15].strip(),
                                fico_score=int(row[16]),
                                num_credit_cards=int(row[17]),
                            )
                        )
                        if len(user_objects) >= 1000:
                            session.bulk_save_objects(user_objects)
                            session.commit()
                            seeded_counts["users"] += len(user_objects)
                            user_objects = []
                    if user_objects:
                        session.bulk_save_objects(user_objects)
                        session.commit()
                        seeded_counts["users"] += len(user_objects)
            else:
                logger.warning(f"Users file not found at {users_file}")
        else:
            seeded_counts["users"] = users_count

        # Check if cards already seeded
        cards_count = session.query(CardModel).count()
        if recreate or cards_count == 0:
            cards_file = p_data_dir / "sd254_cards.csv"
            if cards_file.exists():
                logger.info(f"Seeding cards from {cards_file}...")
                card_objects: List[CardModel] = []
                with open(cards_file, "r", encoding="utf-8-sig") as f:
                    reader = csv.reader(f)
                    next(reader)  # Header
                    for row in reader:
                        if not row or len(row) < 13:
                            continue
                        card_objects.append(
                            CardModel(
                                user_id=int(row[0]),
                                card_index=int(row[1]),
                                card_brand=row[2].strip(),
                                card_type=row[3].strip(),
                                card_number=row[4].strip(),
                                expires=row[5].strip(),
                                cvv=row[6].strip(),
                                has_chip=row[7].strip(),
                                cards_issued=int(row[8]),
                                credit_limit=row[9].strip(),
                                acct_open_date=row[10].strip(),
                                year_pin_last_changed=int(row[11]),
                                card_on_dark_web=row[12].strip(),
                            )
                        )
                        if len(card_objects) >= 2000:
                            session.bulk_save_objects(card_objects)
                            session.commit()
                            seeded_counts["cards"] += len(card_objects)
                            card_objects = []
                    if card_objects:
                        session.bulk_save_objects(card_objects)
                        session.commit()
                        seeded_counts["cards"] += len(card_objects)
            else:
                logger.warning(f"Cards file not found at {cards_file}")
        else:
            seeded_counts["cards"] = cards_count

        # Check if merchant category codes already seeded
        mcc_count = session.query(MerchantCategoryCodeModel).count()
        if recreate or mcc_count == 0:
            if recreate:
                session.query(MerchantCategoryCodeModel).delete()
                session.commit()

            mcc_file = p_data_dir / "merchant_category_codes.csv"
            if mcc_file.exists():
                logger.info(f"Seeding merchant category codes from {mcc_file}...")
                mcc_objects: List[MerchantCategoryCodeModel] = []
                with open(mcc_file, "r", encoding="utf-8-sig") as f:
                    reader = csv.reader(f)
                    next(reader)  # Header
                    for row in reader:
                        if not row or len(row) < 4:
                            continue
                        mcc_code = row[0].strip()
                        if not mcc_code:
                            continue
                        mcc_objects.append(
                            MerchantCategoryCodeModel(
                                mcc=mcc_code,
                                description=row[1].strip(),
                                valid_payment_brands=row[2].strip() if row[2].strip() else None,
                                category=row[3].strip(),
                                required_abbreviation=row[4].strip() if len(row) > 4 and row[4].strip() else None,
                            )
                        )
                        if len(mcc_objects) >= 500:
                            session.bulk_save_objects(mcc_objects)
                            session.commit()
                            seeded_counts["merchant_category_codes"] += len(mcc_objects)
                            mcc_objects = []
                    if mcc_objects:
                        session.bulk_save_objects(mcc_objects)
                        session.commit()
                        seeded_counts["merchant_category_codes"] += len(mcc_objects)
            else:
                logger.warning(f"MCC file not found at {mcc_file}")
        else:
            seeded_counts["merchant_category_codes"] = mcc_count

        # Check if city_reference already seeded
        city_ref_count = session.query(CityReferenceModel).count()
        if recreate or city_ref_count == 0:
            if recreate:
                session.query(CityReferenceModel).delete()
                session.commit()

            city_file = p_data_dir / "city_reference.csv"
            if city_file.exists():
                logger.info(f"Seeding city_reference from {city_file}...")
                city_objects: List[CityReferenceModel] = []
                with open(city_file, "r", encoding="utf-8-sig") as f:
                    reader = csv.reader(f)
                    next(reader, None)  # Header
                    for row in reader:
                        if not row or len(row) < 5:
                            continue
                        c_name = row[0].strip()
                        s_code = row[1].strip()
                        c_code = row[2].strip() if len(row) > 2 and row[2].strip() else "US"
                        try:
                            lat = float(row[3])
                            lon = float(row[4])
                        except ValueError:
                            continue
                        c_source = row[5].strip() if len(row) > 5 and row[5].strip() else "US Census Gazetteer & GeoNames"
                        c_prec = row[6].strip() if len(row) > 6 and row[6].strip() else "city_center"
                        city_objects.append(
                            CityReferenceModel(
                                city_name=c_name,
                                state_code=s_code,
                                country_code=c_code,
                                latitude=lat,
                                longitude=lon,
                                coordinate_source=c_source,
                                coordinate_precision=c_prec,
                            )
                        )
                        if len(city_objects) >= 1000:
                            session.bulk_save_objects(city_objects)
                            session.commit()
                            seeded_counts["city_reference"] += len(city_objects)
                            city_objects = []
                    if city_objects:
                        session.bulk_save_objects(city_objects)
                        session.commit()
                        seeded_counts["city_reference"] += len(city_objects)
            else:
                logger.warning(f"City reference file not found at {city_file}")
        else:
            seeded_counts["city_reference"] = city_ref_count

        logger.info(f"Reference seeding finished: {seeded_counts}")
        return seeded_counts
    finally:
        session.close()


def _extract_field(obj: Any, field: str, default: Any = None) -> Any:
    """Safely extract field from Pydantic model, dataclass, or dict."""
    if hasattr(obj, field):
        val = getattr(obj, field)
        return val if val is not None else default
    if isinstance(obj, dict):
        return obj.get(field, default)
    return default


def persist_prerequisite_entities(
    session: Session,
    prereq_envelopes: Optional[List[Any]] = None,
    commit: bool = True,
) -> None:
    """Persist generated prerequisite entities (users and cards) to PostgreSQL.
    
    These records are captured by PostgreSQL logical replication and Debezium CDC.
    """
    if not prereq_envelopes:
        return

    try:
        for p_env in prereq_envelopes:
            payload = _extract_field(p_env, "payload", {})
            event_type = _extract_field(p_env, "event_type", "")

            if event_type == "user.created":
                user_id = int(_extract_field(payload, "User", _extract_field(payload, "user_id", 0)))
                existing_user = session.query(UserModel).filter_by(user_id=user_id).first()
                if not existing_user:
                    new_user = UserModel(
                        user_id=user_id,
                        person=_extract_field(payload, "Person", _extract_field(payload, "person_name", f"User {user_id}")),
                        current_age=int(_extract_field(payload, "Current Age", _extract_field(payload, "current_age", 35))),
                        retirement_age=int(_extract_field(payload, "Retirement Age", _extract_field(payload, "retirement_age", 67))),
                        birth_year=int(_extract_field(payload, "Birth Year", _extract_field(payload, "birth_year", 1990))),
                        birth_month=int(_extract_field(payload, "Birth Month", _extract_field(payload, "birth_month", 1))),
                        gender=_extract_field(payload, "Gender", _extract_field(payload, "gender", "Female")),
                        address=_extract_field(payload, "Address", _extract_field(payload, "address", "123 Main St")),
                        apartment=_extract_field(payload, "Apartment", _extract_field(payload, "apartment")),
                        city=_extract_field(payload, "City", _extract_field(payload, "city", "Anytown")),
                        state=_extract_field(payload, "State", _extract_field(payload, "state", "CA")),
                        zipcode=int(_extract_field(payload, "Zipcode", _extract_field(payload, "zipcode", 90001))),
                        latitude=float(_extract_field(payload, "Latitude", _extract_field(payload, "latitude", 34.0))),
                        longitude=float(_extract_field(payload, "Longitude", _extract_field(payload, "longitude", -118.0))),
                        per_capita_income=str(_extract_field(payload, "Per Capita Income - Zipcode", _extract_field(payload, "per_capita_income", "$30000"))),
                        yearly_income=str(_extract_field(payload, "Yearly Income - Person", _extract_field(payload, "yearly_income", "$60000"))),
                        total_debt=str(_extract_field(payload, "Total Debt", _extract_field(payload, "total_debt", "$15000"))),
                        fico_score=int(_extract_field(payload, "FICO Score", _extract_field(payload, "fico_score", 700))),
                        num_credit_cards=int(_extract_field(payload, "Num Credit Cards", _extract_field(payload, "num_credit_cards", 1))),
                    )
                    session.add(new_user)
                    session.flush()

            elif event_type == "card.created":
                user_id = int(_extract_field(payload, "User", _extract_field(payload, "user_id", 0)))
                card_index = int(_extract_field(payload, "CARD INDEX", _extract_field(payload, "card_index", 0)))
                existing_card = session.query(CardModel).filter_by(user_id=user_id, card_index=card_index).first()
                if not existing_card:
                    new_card = CardModel(
                        user_id=user_id,
                        card_index=card_index,
                        card_brand=_extract_field(payload, "Card Brand", _extract_field(payload, "card_brand", "Visa")),
                        card_type=_extract_field(payload, "Card Type", _extract_field(payload, "card_type", "Credit")),
                        card_number=str(_extract_field(payload, "Card Number", _extract_field(payload, "card_number_masked", "4000000000000000"))),
                        expires=str(_extract_field(payload, "Expires", _extract_field(payload, "expires", "12/2030"))),
                        cvv=str(_extract_field(payload, "CVV", "123")),
                        has_chip=str(_extract_field(payload, "Has Chip", "YES")),
                        cards_issued=int(_extract_field(payload, "Cards Issued", 1)),
                        credit_limit=str(_extract_field(payload, "Credit Limit", "$5000")),
                        acct_open_date=str(_extract_field(payload, "Acct Open Date", "01/2020")),
                        year_pin_last_changed=int(_extract_field(payload, "Year PIN last Changed", 2020)),
                        card_on_dark_web=str(_extract_field(payload, "Card on Dark Web", "No")),
                    )
                    session.add(new_card)
                    session.flush()

        if commit:
            session.commit()
    except Exception:
        if commit:
            session.rollback()
        raise


def persist_transaction(
    session: Session,
    envelope: Any,
    prereq_envelopes: Optional[List[Any]] = None,
    commit: bool = True,
) -> TransactionModel:
    """Persist prerequisite entities (users, cards) to relational database while keeping transactions purely streaming.
    
    Per PRD architectural requirement:
    - Transactions are strictly NOT inserted into PostgreSQL transactions table.
    - Prerequisite users and cards are persisted in PostgreSQL to enable Debezium CDC.
    - TransactionModel is instantiated in-memory for downstream validation and contracts.
    """
    try:
        # Persist prerequisite users and cards in PostgreSQL
        if prereq_envelopes:
            persist_prerequisite_entities(session, prereq_envelopes, commit=commit)

        tx_p = _extract_field(envelope, "payload", {})
        event_id = _extract_field(envelope, "event_id", "")
        event_type = _extract_field(envelope, "event_type", "transaction.created")
        scenario = _extract_field(envelope, "scenario", "normal_spend") or "normal_spend"
        source_system = _extract_field(envelope, "source_system", "POS_TERMINAL")
        run_id = _extract_field(envelope, "run_id", "default-run")

        amount_raw = str(_extract_field(tx_p, "Amount", "$0.00"))
        amt_clean = amount_raw.replace("$", "").replace(",", "").strip()
        try:
            amt_dec = Decimal(amt_clean)
        except Exception:
            amt_dec = Decimal("0.00")

        ts_str = _extract_field(envelope, "event_timestamp", None)
        if ts_str:
            try:
                created_at = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            except Exception:
                created_at = datetime.now(timezone.utc)
        else:
            created_at = datetime.now(timezone.utc)

        if hasattr(envelope, "model_dump"):
            raw_data = envelope.model_dump(by_alias=True)
        elif isinstance(envelope, dict):
            raw_data = envelope
        else:
            raw_data = tx_p

        entry_method = _extract_field(tx_p, "Transaction Entry Method", _extract_field(tx_p, "transaction_entry_method", None))
        if not entry_method or entry_method == "unknown":
            from synthetic_streaming.classification_and_geo import sample_entry_method
            use_chip_val = str(tx_p.get("Use Chip", "Chip Transaction"))
            try:
                mcc_val = int(tx_p.get("MCC", 5411)) if tx_p.get("MCC") else 5411
            except (ValueError, TypeError):
                mcc_val = 5411
            entry_method = sample_entry_method(
                use_chip=use_chip_val,
                source_system=source_system,
                mcc=mcc_val,
                scenario=scenario,
            )
        m_lat = _extract_field(tx_p, "Merchant Latitude", _extract_field(tx_p, "merchant_latitude", None))
        m_lon = _extract_field(tx_p, "Merchant Longitude", _extract_field(tx_p, "merchant_longitude", None))

        m_city = str(tx_p.get("Merchant City")) if tx_p.get("Merchant City") else None
        m_state = str(tx_p.get("Merchant State")) if tx_p.get("Merchant State") else None

        # Resolve coordinates from city_reference if not provided in payload
        if (m_lat is None or m_lon is None) and m_city and m_state:
            if m_city.strip().lower() != "online":
                match = session.query(CityReferenceModel).filter(
                    func.lower(CityReferenceModel.city_name) == m_city.strip().lower(),
                    func.lower(CityReferenceModel.state_code) == m_state.strip().lower(),
                ).first()
                if match:
                    m_lat = match.latitude
                    m_lon = match.longitude

        from synthetic_streaming.classification_and_geo import validate_coordinates
        try:
            f_lat = float(m_lat) if m_lat is not None else None
            f_lon = float(m_lon) if m_lon is not None else None
        except (ValueError, TypeError):
            f_lat, f_lon = None, None

        if not validate_coordinates(f_lat, f_lon):
            f_lat, f_lon = None, None

        # Construct TransactionModel in-memory WITHOUT adding to session
        # Transactions are delivered to Amazon MSK and S3 Bronze, NOT PostgreSQL.
        tx_record = TransactionModel(
            transaction_id=f"tx-{event_id}",
            event_id=event_id,
            event_type=event_type,
            user_id=int(tx_p.get("User", 0)),
            card_index=int(tx_p.get("Card", 0)),
            amount=amt_dec,
            amount_raw=amount_raw,
            use_chip=str(tx_p.get("Use Chip", "Chip Transaction")),
            transaction_entry_method=str(entry_method) if entry_method else "unknown",
            merchant_name=str(tx_p.get("Merchant Name", "")),
            merchant_city=m_city,
            merchant_state=m_state,
            merchant_latitude=f_lat,
            merchant_longitude=f_lon,
            zip=str(tx_p.get("Zip")) if tx_p.get("Zip") is not None else None,
            mcc=int(tx_p.get("MCC", 0)),
            errors=str(tx_p.get("Errors?")) if tx_p.get("Errors?") else None,
            is_fraud=str(tx_p.get("Is Fraud?", "No")),
            scenario=scenario,
            source_system=source_system,
            run_id=run_id,
            created_at=created_at,
            raw_payload=raw_data,
        )
        return tx_record
    except Exception:
        if commit:
            session.rollback()
        raise


