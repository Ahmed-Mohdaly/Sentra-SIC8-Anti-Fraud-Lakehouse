"""Classification taxonomy for payment-entry methods and geographic coordinate validation."""

from __future__ import annotations

import csv
import logging
from pathlib import Path
import random
from typing import Any, Dict, List, Optional, Set, Tuple, Union

logger = logging.getLogger(__name__)

# ==============================================================================
# 1. Transaction Entry Method Taxonomy
# ==============================================================================

# Allowed transaction_entry_method values mapped to the original Use Chip values
ENTRY_METHODS_BY_USE_CHIP: Dict[str, Set[str]] = {
    "Swipe Transaction": {
        "magnetic_stripe_swipe",
        "unknown",
    },
    "Chip Transaction": {
        "contact_chip",
        "contactless_chip",
        "unknown",
    },
    "Online Transaction": {
        "ecommerce",
        "food_delivery",
        "digital_subscription",
        "digital_goods",
        "online_bill_payment",
        "in_app_purchase",
        "online_service_payment",
        "unknown",
    },
}

ALL_VALID_ENTRY_METHODS: Set[str] = {
    method for methods in ENTRY_METHODS_BY_USE_CHIP.values() for method in methods
}


def is_valid_entry_method(use_chip: str, entry_method: Optional[str]) -> bool:
    """Validate whether an entry method is recognized and compatible with the original Use Chip field."""
    if entry_method is None:
        return True
    if entry_method == "unknown":
        return True
    allowed = ENTRY_METHODS_BY_USE_CHIP.get(use_chip)
    if not allowed:
        return False
    return entry_method in allowed


def sample_entry_method(
    use_chip: str,
    source_system: str,
    mcc: int,
    scenario: str = "normal_spend",
    rng: Optional[random.Random] = None,
) -> str:
    """Assign an entry method using deterministic, explicit business rules based on the transaction context."""
    r = rng or random.Random()

    if use_chip == "Swipe Transaction":
        # Physical legacy swipe
        return "magnetic_stripe_swipe"

    if use_chip == "Chip Transaction":
        # Source systems like DIGITAL_WALLET tap-to-pay are contactless
        if source_system == "DIGITAL_WALLET":
            return "contactless_chip"
        # ATMs require physical chip insertion
        if source_system == "ATM_NETWORK":
            return "contact_chip"
        # For POS terminals, fast-casual and transit lean contactless, large retail lean contact
        if mcc in {5814, 5411, 4111, 5541}:  # Fast food, grocery, transit, service stations
            return r.choices(["contactless_chip", "contact_chip"], weights=[0.60, 0.40], k=1)[0]
        return r.choices(["contact_chip", "contactless_chip"], weights=[0.70, 0.30], k=1)[0]

    if use_chip == "Online Transaction":
        # Recurring billings for utilities and telecom
        if source_system == "RECURRING_BILLING":
            if mcc in {4900, 4814, 6300}:  # Utilities, telecom, insurance
                return "online_bill_payment"
            return "digital_subscription"

        # Food delivery and dining MCCs
        if mcc in {5812, 5814}:  # Eating places, fast food
            return r.choices(["food_delivery", "ecommerce"], weights=[0.80, 0.20], k=1)[0]

        # Digital goods, gaming, and app stores
        if mcc in {5816, 7993, 7372, 5735}:  # Digital games, amusement, software, record stores
            return r.choices(["digital_goods", "in_app_purchase"], weights=[0.55, 0.45], k=1)[0]

        # Subscription services (cable, streaming, magazines)
        if mcc in {4899, 5968, 7841}:
            return "digital_subscription"

        # Utilities and government/school fees
        if mcc in {4900, 4814, 8220, 9399}:
            return "online_bill_payment"

        # General digital wallet in-app vs web ecommerce
        if source_system == "DIGITAL_WALLET":
            return r.choices(["in_app_purchase", "ecommerce"], weights=[0.65, 0.35], k=1)[0]

        # General service payments
        if mcc in {7399, 8999, 7299}:
            return "online_service_payment"

        # Standard online shopping
        return "ecommerce"

    return "unknown"


# ==============================================================================
# 2. Geographic Coordinate Validation and Normalization
# ==============================================================================

def validate_coordinates(latitude: Optional[float], longitude: Optional[float]) -> bool:
    """Ensure latitude and longitude adhere to valid geographic boundaries and co-occurrence.
    
    Rules:
    - Latitude must be between -90.0 and 90.0.
    - Longitude must be between -180.0 and 180.0.
    - Latitude and longitude must be either both present or both absent (None).
    """
    if latitude is None and longitude is None:
        return True
    if latitude is None or longitude is None:
        return False
    if not (-90.0 <= latitude <= 90.0):
        return False
    if not (-180.0 <= longitude <= 180.0):
        return False
    return True


def normalize_city_state(
    city: Optional[str],
    state: Optional[str]
) -> Tuple[Optional[str], Optional[str]]:
    """Normalize city and state strings for deterministic, case-insensitive lookups."""
    if not city:
        return None, None
    norm_city = city.strip().title()
    norm_state = state.strip().upper() if state else None
    return norm_city, norm_state


class CityResolver:
    """In-memory cache for fast O(1) resolution of city/state pairs to reference coordinates."""

    def __init__(self):
        self._cache: Dict[Tuple[str, str], Tuple[float, float]] = {}

    def populate(self, cities: List[Dict[str, Any]]) -> None:
        """Load reference cities into resolver cache."""
        for c in cities:
            city_name = c.get("city_name")
            state_code = c.get("state_code")
            lat = c.get("latitude")
            lon = c.get("longitude")
            if city_name and state_code and lat is not None and lon is not None:
                key = (city_name.strip().lower(), state_code.strip().lower())
                self._cache[key] = (float(lat), float(lon))

    def load_from_csv(self, csv_path: Union[str, Path]) -> int:
        """Load reference coordinates from a CSV file (e.g. city_reference.csv)."""
        p = Path(csv_path)
        if not p.exists():
            fallback = Path("data") / p.name
            if fallback.exists():
                p = fallback
            else:
                repo_fallback = Path(__file__).resolve().parent.parent.parent / "data" / p.name
                if repo_fallback.exists():
                    p = repo_fallback
                else:
                    return 0
        loaded = 0
        with open(p, "r", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            next(reader, None)  # Skip header
            for row in reader:
                if not row or len(row) < 5:
                    continue
                c_name, s_code = row[0].strip(), row[1].strip()
                try:
                    lat, lon = float(row[3]), float(row[4])
                    self._cache[(c_name.lower(), s_code.lower())] = (lat, lon)
                    loaded += 1
                except ValueError:
                    continue
        return loaded

    def resolve(self, city: Optional[str], state: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
        """Resolve city and state to approximate coordinates, or (None, None) if unmatched."""
        if not city or not state:
            return None, None
        norm_c = city.strip().lower()
        if norm_c == "online":
            return None, None
        norm_s = state.strip().lower()
        key = (norm_c, norm_s)
        return self._cache.get(key, (None, None))

    def __len__(self) -> int:
        return len(self._cache)
