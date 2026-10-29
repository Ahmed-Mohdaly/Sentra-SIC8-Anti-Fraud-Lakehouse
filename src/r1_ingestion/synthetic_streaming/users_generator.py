"""Synthetic user generator with non-colliding IDs and privacy-preserving attributes."""

from __future__ import annotations

import random
import threading
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from synthetic_streaming.event_schema import EventEnvelope, UserPayload


FIRST_NAMES_MALE = [
    "James", "John", "Robert", "Michael", "William", "David", "Richard", "Joseph",
    "Thomas", "Charles", "Daniel", "Matthew", "Anthony", "Donald", "Mark", "Paul",
    "Steven", "Andrew", "Kenneth", "Joshua", "Kevin", "Brian", "George", "Edward"
]

FIRST_NAMES_FEMALE = [
    "Mary", "Patricia", "Jennifer", "Linda", "Elizabeth", "Barbara", "Susan", "Jessica",
    "Sarah", "Karen", "Lisa", "Nancy", "Betty", "Margaret", "Sandra", "Ashley",
    "Kimberly", "Emily", "Donna", "Michelle", "Carol", "Amanda", "Dorothy", "Melissa"
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White"
]

STREET_NAMES = [
    "Main St", "Highland Ave", "Maple St", "Oak Dr", "Washington Blvd", "Pine St",
    "Cedar Ave", "Elm St", "Park Way", "Sunset Blvd", "Broadway", "Lakeview Dr"
]

US_LOCATIONS = [
    ("New York", "NY", 10001, 40.75, -73.99),
    ("Los Angeles", "CA", 90001, 33.97, -118.24),
    ("Chicago", "IL", 60601, 41.88, -87.62),
    ("Houston", "TX", 77001, 29.76, -95.36),
    ("Phoenix", "AZ", 85001, 33.45, -112.07),
    ("Philadelphia", "PA", 19101, 39.95, -75.16),
    ("San Antonio", "TX", 78201, 29.42, -98.49),
    ("San Diego", "CA", 92101, 32.71, -117.16),
    ("Dallas", "TX", 75201, 32.77, -96.79),
    ("San Jose", "CA", 95101, 37.33, -121.89),
    ("Austin", "TX", 78701, 30.26, -97.74),
    ("Seattle", "WA", 98101, 47.60, -122.33),
]


class UsersGenerator:
    """Thread-safe generator for synthetic users ensuring unique IDs above baseline high-water mark."""

    def __init__(
        self,
        initial_high_water_mark: int = 2000,
        db_engine: Optional[Any] = None,
        worker_id: int = 0,
        rng: Optional[random.Random] = None,
    ):
        self._lock = threading.Lock()
        self._rng = rng or random.Random()
        self.generated_users: Dict[int, UserPayload] = {}

        hwm = initial_high_water_mark
        if db_engine is not None:
            try:
                from sqlalchemy import text
                with db_engine.connect() as conn:
                    result = conn.execute(text("SELECT COALESCE(MAX(user_id), 0) FROM users")).scalar()
                    if result is not None and int(result) > 0:
                        db_max = int(result)
                        if db_max >= hwm:
                            hwm = db_max + 1
            except Exception:
                pass

        self._next_user_id = hwm

    def get_next_user_id(self) -> int:
        with self._lock:
            uid = self._next_user_id
            self._next_user_id += 1
            return uid

    def generate_user(self, run_id: str = "run-001", source_system: str = "ISSUER_PROCESSOR") -> Tuple[UserPayload, EventEnvelope]:
        """Generate a new unique synthetic user and its creation event envelope."""
        user_id = self.get_next_user_id()

        gender = self._rng.choice(["Female", "Male"])
        first = self._rng.choice(FIRST_NAMES_FEMALE if gender == "Female" else FIRST_NAMES_MALE)
        last = self._rng.choice(LAST_NAMES)
        person_name = f"{first} {last}"

        current_year = 2026
        age = self._rng.randint(20, 80)
        birth_year = current_year - age
        birth_month = self._rng.randint(1, 12)
        retire_age = self._rng.randint(62, 70)

        street_no = self._rng.randint(10, 9999)
        street_name = self._rng.choice(STREET_NAMES)
        address = f"{street_no} {street_name}"
        apt = f"Apt {self._rng.randint(1, 200)}" if self._rng.random() < 0.25 else None

        city, state, zip_code, lat, lon = self._rng.choice(US_LOCATIONS)

        yearly_income = self._rng.randint(25000, 180000)
        per_capita = int(yearly_income * self._rng.uniform(0.4, 0.7))
        debt = int(yearly_income * self._rng.uniform(0.1, 2.5))
        fico = self._rng.randint(550, 840)
        num_cards = self._rng.randint(1, 5)

        payload_dict = {
            "User": user_id,
            "Person": person_name,
            "Current Age": age,
            "Retirement Age": retire_age,
            "Birth Year": birth_year,
            "Birth Month": birth_month,
            "Gender": gender,
            "Address": address,
            "Apartment": apt,
            "City": city,
            "State": state,
            "Zipcode": zip_code,
            "Latitude": lat,
            "Longitude": lon,
            "Per Capita Income - Zipcode": f"${per_capita}",
            "Yearly Income - Person": f"${yearly_income}",
            "Total Debt": f"${debt}",
            "FICO Score": fico,
            "Num Credit Cards": num_cards,
        }

        user_payload = UserPayload(**payload_dict)
        with self._lock:
            self.generated_users[user_id] = user_payload

        envelope = EventEnvelope(
            event_type="user.created",
            source_system=source_system,
            run_id=run_id,
            payload=payload_dict,
        )

        return user_payload, envelope
