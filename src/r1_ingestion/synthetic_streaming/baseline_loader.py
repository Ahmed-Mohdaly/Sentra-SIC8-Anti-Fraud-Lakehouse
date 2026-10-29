"""Baseline data loader for IBM Synthetic Credit Card dataset with fallback bootstrap support."""

from __future__ import annotations

import csv
import logging
import random
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class BaselineUser:
    user_id: int
    person: str
    current_age: int
    retirement_age: int
    birth_year: int
    birth_month: int
    gender: str
    address: str
    apartment: Optional[str]
    city: str
    state: str
    zipcode: int
    latitude: float
    longitude: float
    per_capita_income: str
    yearly_income: str
    total_debt: str
    fico_score: int
    num_credit_cards: int


@dataclass
class BaselineCard:
    user_id: int
    card_index: int
    card_brand: str
    card_type: str
    card_number: str
    expires: str
    cvv: str
    has_chip: str
    cards_issued: int
    credit_limit: str
    acct_open_date: str
    year_pin_last_changed: int
    card_on_dark_web: str


class BaselineLoader:
    """Loads and indexes baseline entities and empirical transaction distributions."""

    def __init__(self, data_dir: str | Path):
        import os
        import tempfile
        self.s3_uri: Optional[str] = None
        if isinstance(data_dir, str) and data_dir.startswith("s3://"):
            self.s3_uri = data_dir
            self.data_dir = Path(tempfile.gettempdir()) / "baseline_s3_cache"
        else:
            self.data_dir = Path(data_dir)
            self.s3_uri = os.environ.get("BASELINE_S3_URI", "s3://sic-pipeline-storage/raw/baseline")

        self.users: Dict[int, BaselineUser] = {}
        self.cards_by_user: Dict[int, List[BaselineCard]] = {}
        self.cards_by_key: Dict[Tuple[int, int], BaselineCard] = {}
        self.user_ids: List[int] = []
        self.high_water_mark: int = 2000

        # Empirical distributions extracted from baseline
        self.merchant_samples: List[Dict[str, Any]] = []
        self.mcc_list: List[int] = [5411, 5812, 5300, 5541, 5912, 5814, 5999, 4814, 5311, 7011]
        self.cities_and_states: List[Tuple[str, str, str]] = []

    def _try_download_from_s3(self) -> bool:
        """Download baseline reference CSV files from Amazon S3 into local cache."""
        if not self.s3_uri or not self.s3_uri.startswith("s3://"):
            return False

        try:
            import boto3
            from botocore.exceptions import ClientError

            parts = self.s3_uri.replace("s3://", "").split("/", 1)
            bucket = parts[0]
            prefix = parts[1].rstrip("/") if len(parts) > 1 else ""

            s3 = boto3.client("s3")
            self.data_dir.mkdir(parents=True, exist_ok=True)

            files = [
                "sd254_users.csv",
                "sd254_cards.csv",
                "merchant_category_codes.csv",
                "city_reference.csv",
                "User0_credit_card_transactions.csv",
            ]
            for fname in files:
                dest = self.data_dir / fname
                if not dest.exists() or dest.stat().st_size == 0:
                    key = f"{prefix}/{fname}" if prefix else fname
                    try:
                        logger.info(f"Downloading baseline file from S3: s3://{bucket}/{key} -> {dest}")
                        s3.download_file(bucket, key, str(dest))
                    except ClientError as e:
                        logger.warning(f"Could not download {key} from S3: {e}")

            return (self.data_dir / "sd254_users.csv").exists() and (self.data_dir / "sd254_cards.csv").exists()
        except Exception as e:
            logger.warning(f"S3 baseline download failed: {e}")
        return False

    def load(self, sample_tx_rows: int = 5000) -> BaselineLoader:
        """Load baseline users, cards, and a representative sample of merchants/MCCs."""
        users_file = self.data_dir / "sd254_users.csv"
        cards_file = self.data_dir / "sd254_cards.csv"

        if not (users_file.exists() and cards_file.exists()):
            fallback_dir = self.data_dir / "data"
            if (fallback_dir / "sd254_users.csv").exists():
                self.data_dir = fallback_dir
                users_file = self.data_dir / "sd254_users.csv"
                cards_file = self.data_dir / "sd254_cards.csv"
            else:
                repo_fallback = Path(__file__).resolve().parent.parent.parent / "data"
                if (repo_fallback / "sd254_users.csv").exists():
                    self.data_dir = repo_fallback
                    users_file = self.data_dir / "sd254_users.csv"
                    cards_file = self.data_dir / "sd254_cards.csv"
                else:
                    self._try_download_from_s3()
                    users_file = self.data_dir / "sd254_users.csv"
                    cards_file = self.data_dir / "sd254_cards.csv"

        if users_file.exists() and cards_file.exists():
            self._load_users()
            self._load_cards()
            self._load_sample_merchants(sample_tx_rows)
        else:
            logger.info(
                f"Baseline CSV files not found in {self.data_dir}. "
                "Initializing synthetic bootstrap baseline (500 users, contiguous cards, high-water mark 2000)."
            )
            self._bootstrap_synthetic_baseline()

        return self

    def _load_users(self) -> None:
        users_file = self.data_dir / "sd254_users.csv"
        with open(users_file, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            for idx, row in enumerate(reader):
                u = BaselineUser(
                    user_id=idx,
                    person=row[0],
                    current_age=int(row[1]),
                    retirement_age=int(row[2]),
                    birth_year=int(row[3]),
                    birth_month=int(row[4]),
                    gender=row[5],
                    address=row[6],
                    apartment=row[7] if row[7] else None,
                    city=row[8],
                    state=row[9],
                    zipcode=int(row[10]) if row[10] else 0,
                    latitude=float(row[11]) if row[11] else 0.0,
                    longitude=float(row[12]) if row[12] else 0.0,
                    per_capita_income=row[13],
                    yearly_income=row[14],
                    total_debt=row[15],
                    fico_score=int(row[16]),
                    num_credit_cards=int(row[17]),
                )
                self.users[idx] = u
                self.user_ids.append(idx)

        self.high_water_mark = max(self.user_ids) + 1 if self.user_ids else 0

    def _load_cards(self) -> None:
        cards_file = self.data_dir / "sd254_cards.csv"
        with open(cards_file, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            for row in reader:
                u_id = int(row[0])
                c_idx = int(row[1])
                c = BaselineCard(
                    user_id=u_id,
                    card_index=c_idx,
                    card_brand=row[2],
                    card_type=row[3],
                    card_number=row[4],
                    expires=row[5],
                    cvv=row[6],
                    has_chip=row[7],
                    cards_issued=int(row[8]),
                    credit_limit=row[9],
                    acct_open_date=row[10],
                    year_pin_last_changed=int(row[11]),
                    card_on_dark_web=row[12],
                )
                if u_id not in self.cards_by_user:
                    self.cards_by_user[u_id] = []
                self.cards_by_user[u_id].append(c)
                self.cards_by_key[(u_id, c_idx)] = c

    def _load_sample_merchants(self, sample_limit: int) -> None:
        """Sample merchants and locations from the transaction baseline without loading the full 2.35GB file."""
        tx_file = self.data_dir / "User0_credit_card_transactions.csv"
        if not tx_file.exists():
            tx_file = self.data_dir / "credit_card_transactions-ibm_v2.csv"
        if not tx_file.exists():
            self._seed_default_merchants()
            return

        with open(tx_file, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            for count, row in enumerate(reader):
                if count >= sample_limit:
                    break
                merchant_name = row[8]
                city = row[9]
                state = row[10]
                zip_val = row[11]
                mcc = int(row[12]) if row[12].isdigit() else 5411

                self.merchant_samples.append({
                    "merchant_name": merchant_name,
                    "city": city,
                    "state": state,
                    "zip": zip_val,
                    "mcc": mcc,
                })
                if city and state:
                    self.cities_and_states.append((city, state, zip_val))

    def _bootstrap_synthetic_baseline(self) -> None:
        """Generate a lightweight in-memory baseline when running containerized without mounted CSVs."""
        self.high_water_mark = 2000
        rng = random.Random(1337)

        for u_id in range(500):
            num_cards = rng.randint(1, 4)
            u = BaselineUser(
                user_id=u_id,
                person=f"Bootstrap User {u_id}",
                current_age=rng.randint(25, 75),
                retirement_age=65,
                birth_year=1980,
                birth_month=rng.randint(1, 12),
                gender="Female" if u_id % 2 == 0 else "Male",
                address=f"{100 + u_id} Baseline Ave",
                apartment=None,
                city="San Jose",
                state="CA",
                zipcode=95101,
                latitude=37.33,
                longitude=-121.89,
                per_capita_income="$45000",
                yearly_income="$75000",
                total_debt="$15000",
                fico_score=720,
                num_credit_cards=num_cards,
            )
            self.users[u_id] = u
            self.user_ids.append(u_id)
            self.cards_by_user[u_id] = []

            for c_idx in range(num_cards):
                card = BaselineCard(
                    user_id=u_id,
                    card_index=c_idx,
                    card_brand="Visa" if c_idx % 2 == 0 else "Mastercard",
                    card_type="Debit" if c_idx == 0 else "Credit",
                    card_number=f"400000{u_id:04d}{c_idx:02d}0000"[:16],
                    expires="12/2028",
                    cvv="123",
                    has_chip="YES",
                    cards_issued=1,
                    credit_limit="$10000",
                    acct_open_date="01/2020",
                    year_pin_last_changed=2024,
                    card_on_dark_web="No",
                )
                self.cards_by_user[u_id].append(card)
                self.cards_by_key[(u_id, c_idx)] = card

        self._seed_default_merchants()

    def _seed_default_merchants(self) -> None:
        """Seed representative merchant profiles."""
        merchants = [
            {"merchant_name": "3527213246127876953", "city": "La Verne", "state": "CA", "zip": "91750.0", "mcc": 5411},
            {"merchant_name": "-727612092139916043", "city": "Monterey Park", "state": "CA", "zip": "91754.0", "mcc": 5411},
            {"merchant_name": "3414527459579106770", "city": "Monterey Park", "state": "CA", "zip": "91754.0", "mcc": 5651},
            {"merchant_name": "581200001234567890", "city": "Los Angeles", "state": "CA", "zip": "90001.0", "mcc": 5812},
            {"merchant_name": "554100009876543210", "city": "New York", "state": "NY", "zip": "10001.0", "mcc": 5541},
        ]
        self.merchant_samples.extend(merchants)
        for m in merchants:
            self.cities_and_states.append((m["city"], m["state"], m["zip"]))

    def get_user(self, user_id: int) -> Optional[BaselineUser]:
        return self.users.get(user_id)

    def get_cards_for_user(self, user_id: int) -> List[BaselineCard]:
        return self.cards_by_user.get(user_id, [])

    def get_card(self, user_id: int, card_index: int) -> Optional[BaselineCard]:
        return self.cards_by_key.get((user_id, card_index))

    def sample_user(self) -> BaselineUser:
        """Return a random baseline user."""
        u_id = random.choice(self.user_ids)
        return self.users[u_id]

    def sample_card_for_user(self, user_id: int) -> BaselineCard:
        """Return a random card owned by the specified user."""
        cards = self.cards_by_user.get(user_id)
        if not cards:
            raise ValueError(f"No cards found for user {user_id}")
        return random.choice(cards)

    def sample_merchant(self) -> Dict[str, Any]:
        """Return a sample merchant profile or fallback default."""
        if self.merchant_samples:
            return random.choice(self.merchant_samples)
        return {
            "merchant_name": "3527213246127876953",
            "city": "La Verne",
            "state": "CA",
            "zip": "91750.0",
            "mcc": 5411,
        }
