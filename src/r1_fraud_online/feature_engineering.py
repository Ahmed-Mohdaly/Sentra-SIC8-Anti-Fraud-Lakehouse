"""Unified Feature Engineering engine for offline training and online streaming inference.
Ensures zero train-serve skew between historical CSV datasets and real-time Kafka events.
"""

from __future__ import annotations

import math
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple, Union
import numpy as np


def haversine_distance(
    lat1: Optional[float],
    lon1: Optional[float],
    lat2: Optional[float],
    lon2: Optional[float]
) -> float:
    """Calculate Great-Circle (Haversine) distance between two GPS coordinates in kilometers."""
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return 0.0
    try:
        lat1_f, lon1_f = float(lat1), float(lon1)
        lat2_f, lon2_f = float(lat2), float(lon2)
    except (ValueError, TypeError):
        return 0.0

    # Earth radius in kilometers
    r = 6371.0
    phi1 = math.radians(lat1_f)
    phi2 = math.radians(lat2_f)
    delta_phi = math.radians(lat2_f - lat1_f)
    delta_lambda = math.radians(lon2_f - lon1_f)

    a = (
        math.sin(delta_phi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * (math.sin(delta_lambda / 2.0) ** 2)
    )
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(max(0.0, 1.0 - a)))
    return float(r * c)


def clean_currency(val: Any, default: float = 0.0) -> float:
    """Safely parse currency strings like '$123.45' or numeric values into float."""
    if val is None:
        return default
    if isinstance(val, (int, float, Decimal)):
        return float(val)
    s = str(val).strip().replace("$", "").replace(",", "")
    try:
        return float(s)
    except ValueError:
        return default


# Categorical Encodings (deterministic bidirectional mapping)
USE_CHIP_MAP: Dict[str, int] = {
    "Swipe Transaction": 0,
    "Chip Transaction": 1,
    "Online Transaction": 2,
    "unknown": -1,
}

CARD_BRAND_MAP: Dict[str, int] = {
    "Visa": 0,
    "Mastercard": 1,
    "Amex": 2,
    "Discover": 3,
    "unknown": -1,
}

CARD_TYPE_MAP: Dict[str, int] = {
    "Debit": 0,
    "Credit": 1,
    "Debit (Prepaid)": 2,
    "unknown": -1,
}

ENTRY_METHOD_MAP: Dict[str, int] = {
    "magnetic_stripe_swipe": 0,
    "contact_chip": 1,
    "contactless_chip": 2,
    "ecommerce": 3,
    "food_delivery": 4,
    "digital_subscription": 5,
    "digital_goods": 6,
    "online_bill_payment": 7,
    "in_app_purchase": 8,
    "online_service_payment": 9,
    "unknown": -1,
}

# Cold-start baseline defaults for new users / missing profiles
GLOBAL_DEFAULTS = {
    "user_avg_spend": 48.50,
    "user_std_spend": 38.20,
    "credit_limit": 5000.0,
    "fico_score": 700.0,
    "current_age": 42.0,
    "yearly_income": 55000.0,
    "total_debt": 25000.0,
    "num_credit_cards": 3.0,
    "global_fraud_rate": 0.0015,
}

# Canonical list of ordered feature names used as model inputs
FEATURE_NAMES: List[str] = [
    "amount",
    "log_amount",
    "spend_deviation_ratio",
    "credit_limit_utilization",
    "amount_zscore",
    "current_age",
    "fico_score",
    "yearly_income",
    "total_debt",
    "num_credit_cards",
    "credit_limit",
    "card_age_at_tx_years",
    "years_since_pin_changed",
    "distance_to_home_km",
    "time_since_last_tx_sec",
    "travel_speed_kmh",
    "tx_count_last_1h",
    "tx_count_last_24h",
    "spend_sum_last_1h",
    "is_night_tx",
    "sin_hour",
    "cos_hour",
    "has_chip",
    "card_on_dark_web",
    "use_chip_code",
    "card_brand_code",
    "card_type_code",
    "entry_method_code",
    "is_online_tx",
    "is_credential_error",
    "is_insufficient_balance",
    "is_refund",
    "mcc_high_risk_flag",
    "mcc_risk_score",
]


class FeatureExtractor:
    """Extracts consistent features for both batch datasets and real-time streaming transactions."""

    def __init__(
        self,
        mcc_risk_map: Optional[Dict[int, float]] = None,
        global_stats: Optional[Dict[str, float]] = None
    ):
        self.mcc_risk_map = mcc_risk_map or {}
        self.global_stats = {**GLOBAL_DEFAULTS, **(global_stats or {})}

    def extract_online_features(
        self,
        payload: Dict[str, Any],
        user_profile: Optional[Dict[str, Any]] = None,
        card_profile: Optional[Dict[str, Any]] = None,
        state: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, float]:
        """Extract a single-event feature vector from a real-time Kafka transaction payload.
        
        Args:
            payload: Raw transaction event payload dict (from Kafka EventEnvelope).
            user_profile: Static user attributes (age, fico, income, debt, home coords).
            card_profile: Static card attributes (credit limit, brand, type, dark web).
            state: Rolling window and velocity state from Redis / In-Memory cache.
        """
        user_p = user_profile or {}
        card_p = card_profile or {}
        st = state or {}

        # 1. Financial amount and profile baselines
        amount = clean_currency(payload.get("Amount"), default=0.0)
        credit_limit = clean_currency(
            card_p.get("Credit Limit", card_p.get("credit_limit")),
            default=self.global_stats["credit_limit"]
        )
        if credit_limit <= 0:
            credit_limit = self.global_stats["credit_limit"]

        user_avg_spend = clean_currency(
            st.get("user_avg_spend", user_p.get("user_avg_spend")),
            default=self.global_stats["user_avg_spend"]
        )
        user_std_spend = clean_currency(
            st.get("user_std_spend", user_p.get("user_std_spend")),
            default=self.global_stats["user_std_spend"]
        )
        if user_std_spend <= 0:
            user_std_spend = self.global_stats["user_std_spend"]

        spend_deviation_ratio = round(amount / max(user_avg_spend, 1.0), 4)
        credit_limit_utilization = round(amount / max(credit_limit, 1.0), 4)
        amount_zscore = round((amount - user_avg_spend) / user_std_spend, 4)

        # 2. Demographic & Credit features
        current_age = float(user_p.get("Current Age", user_p.get("current_age", self.global_stats["current_age"])))
        fico_score = float(user_p.get("FICO Score", user_p.get("fico_score", self.global_stats["fico_score"])))
        yearly_income = clean_currency(
            user_p.get("Yearly Income - Person", user_p.get("yearly_income")),
            default=self.global_stats["yearly_income"]
        )
        total_debt = clean_currency(
            user_p.get("Total Debt", user_p.get("total_debt")),
            default=self.global_stats["total_debt"]
        )
        num_credit_cards = float(user_p.get("Num Credit Cards", user_p.get("num_credit_cards", self.global_stats["num_credit_cards"])))

        # 3. Geospatial Features: Distance to Home
        user_lat = user_p.get("Latitude", user_p.get("latitude"))
        user_lon = user_p.get("Longitude", user_p.get("longitude"))
        merchant_lat = payload.get("Merchant Latitude", payload.get("merchant_latitude"))
        merchant_lon = payload.get("Merchant Longitude", payload.get("merchant_longitude"))
        distance_to_home_km = round(haversine_distance(merchant_lat, merchant_lon, user_lat, user_lon), 2)

        # 4. Velocity & Impossible Travel Speed
        time_since_last_tx_sec = float(st.get("time_since_last_tx_sec", 86400.0))
        prev_m_lat = st.get("prev_merchant_lat")
        prev_m_lon = st.get("prev_merchant_lon")

        if prev_m_lat is not None and prev_m_lon is not None and merchant_lat is not None and merchant_lon is not None:
            dist_prev_curr = haversine_distance(prev_m_lat, prev_m_lon, merchant_lat, merchant_lon)
            delta_hours = max(time_since_last_tx_sec / 3600.0, 0.001)  # At least ~3.6s
            travel_speed_kmh = round(dist_prev_curr / delta_hours, 2)
        else:
            travel_speed_kmh = 0.0

        tx_count_last_1h = float(st.get("tx_count_last_1h", 1.0))
        tx_count_last_24h = float(st.get("tx_count_last_24h", 1.0))
        spend_sum_last_1h = float(st.get("spend_sum_last_1h", amount))

        # 5. Temporal Features
        time_str = str(payload.get("Time", "12:00")).strip()
        try:
            hour = int(time_str.split(":")[0])
        except (ValueError, IndexError):
            hour = 12

        is_night_tx = 1 if (hour >= 23 or hour <= 5) else 0
        sin_hour = round(math.sin(2 * math.pi * hour / 24.0), 4)
        cos_hour = round(math.cos(2 * math.pi * hour / 24.0), 4)

        # 6. Categorical & Threat Flags
        has_chip_raw = str(card_p.get("Has Chip", card_p.get("has_chip", "YES"))).strip().upper()
        has_chip = 1 if has_chip_raw in ("YES", "Y", "TRUE", "1") else 0

        dark_web_raw = str(card_p.get("Card on Dark Web", card_p.get("card_on_dark_web", "No"))).strip().lower()
        card_on_dark_web = 1 if dark_web_raw in ("yes", "y", "true", "1") else 0

        use_chip_val = str(payload.get("Use Chip", "Swipe Transaction")).strip()
        use_chip_code = USE_CHIP_MAP.get(use_chip_val, -1)

        card_brand_val = str(card_p.get("Card Brand", card_p.get("card_brand", "Visa"))).strip()
        card_brand_code = CARD_BRAND_MAP.get(card_brand_val, -1)

        card_type_val = str(card_p.get("Card Type", card_p.get("card_type", "Debit"))).strip()
        card_type_code = CARD_TYPE_MAP.get(card_type_val, -1)

        entry_method_val = str(payload.get("Transaction Entry Method", "unknown")).strip()
        entry_method_code = ENTRY_METHOD_MAP.get(entry_method_val, -1)

        try:
            mcc = int(payload.get("MCC", 0))
        except (ValueError, TypeError):
            mcc = 0
        mcc_risk_score = round(self.mcc_risk_map.get(mcc, self.global_stats["global_fraud_rate"]), 5)

        # 7. S3 Conformed & EDA-Engineered Features
        log_amount = float(round(math.log1p(max(0.0, amount)), 4))
        is_refund = 1 if amount < 0 else 0
        is_online_tx = 1 if use_chip_val == "Online Transaction" else 0

        err_str = str(payload.get("Errors?", payload.get("Errors", "")) or "").strip()
        is_credential_error = 1 if any(e in err_str for e in ("Bad PIN", "Bad CVV", "Bad Expiration")) else 0
        is_insufficient_balance = 1 if "Insufficient Balance" in err_str else 0

        tx_year = int(payload.get("Year", 2018))
        acct_open_raw = str(card_p.get("Acct Open Date", card_p.get("acct_open_date", "01/2000"))).strip()
        try:
            acct_year = int(acct_open_raw.split("/")[-1])
        except Exception:
            acct_year = 2000
        card_age_at_tx_years = float(max(0, tx_year - acct_year))

        pin_year_raw = card_p.get("Year PIN last Changed", card_p.get("year_pin_last_changed", acct_year))
        try:
            pin_year = int(pin_year_raw)
        except Exception:
            pin_year = acct_year
        years_since_pin_changed = float(max(0, tx_year - pin_year))

        mcc_high_risk_flag = 1 if mcc in {4829, 5311, 5300, 5310, 5815, 5732} else 0

        features: Dict[str, float] = {
            "amount": float(amount),
            "log_amount": float(log_amount),
            "spend_deviation_ratio": float(spend_deviation_ratio),
            "credit_limit_utilization": float(credit_limit_utilization),
            "amount_zscore": float(amount_zscore),
            "current_age": float(current_age),
            "fico_score": float(fico_score),
            "yearly_income": float(yearly_income),
            "total_debt": float(total_debt),
            "num_credit_cards": float(num_credit_cards),
            "credit_limit": float(credit_limit),
            "card_age_at_tx_years": float(card_age_at_tx_years),
            "years_since_pin_changed": float(years_since_pin_changed),
            "distance_to_home_km": float(distance_to_home_km),
            "time_since_last_tx_sec": float(time_since_last_tx_sec),
            "travel_speed_kmh": float(travel_speed_kmh),
            "tx_count_last_1h": float(tx_count_last_1h),
            "tx_count_last_24h": float(tx_count_last_24h),
            "spend_sum_last_1h": float(spend_sum_last_1h),
            "is_night_tx": int(is_night_tx),
            "sin_hour": float(sin_hour),
            "cos_hour": float(cos_hour),
            "has_chip": int(has_chip),
            "card_on_dark_web": int(card_on_dark_web),
            "use_chip_code": int(use_chip_code),
            "card_brand_code": int(card_brand_code),
            "card_type_code": int(card_type_code),
            "entry_method_code": int(entry_method_code),
            "is_online_tx": int(is_online_tx),
            "is_credential_error": int(is_credential_error),
            "is_insufficient_balance": int(is_insufficient_balance),
            "is_refund": int(is_refund),
            "mcc_high_risk_flag": int(mcc_high_risk_flag),
            "mcc_risk_score": float(mcc_risk_score),
        }
        return features

    def to_feature_vector(self, feature_dict: Dict[str, float]) -> List[float]:
        """Convert a feature dictionary into a strictly ordered feature vector for LightGBM."""
        return [float(feature_dict[name]) for name in FEATURE_NAMES]
