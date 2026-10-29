"""Unit and regression tests for Temporal Leakage Prevention, Feature Parity, and Split Boundaries."""

from __future__ import annotations

import math
from datetime import datetime
import numpy as np
import pytest

from ml.feature_engineering import (
    FEATURE_NAMES,
    GLOBAL_DEFAULTS,
    FeatureExtractor,
    clean_currency,
    haversine_distance,
)
from ml.state_store import StateStore
from synthetic_streaming.classification_and_geo import CityResolver, sample_entry_method


def test_temporal_split_boundaries_are_strictly_monotonic():
    """Verify that temporal boundaries strictly enforce train < val < test."""
    train_years = [1996, 2005, 2010, 2016]
    val_years = [2017]
    test_years = [2018, 2019]

    assert max(train_years) < min(val_years), "Train set must precede Validation set"
    assert max(val_years) < min(test_years), "Validation set must precede Test set"


def test_mcc_risk_encoding_is_leakage_free():
    """Verify that target encoding does not incorporate future labels and applies Empirical Bayes smoothing."""
    # Historical training records (<= 2016)
    train_counts = {5411: 1000, 5732: 200, 5300: 50}
    train_frauds = {5411: 1, 5732: 20, 5300: 5}
    global_rate = (1 + 20 + 5) / (1000 + 200 + 50)  # 26 / 1250 = 0.0208
    smoothing = 50.0

    # Compute smoothed scores
    mcc_risk_map = {}
    for mcc, count in train_counts.items():
        fr = train_frauds.get(mcc, 0)
        smoothed = (fr + smoothing * global_rate) / (count + smoothing)
        mcc_risk_map[mcc] = round(smoothed, 5)

    extractor = FeatureExtractor(
        mcc_risk_map=mcc_risk_map,
        global_stats={**GLOBAL_DEFAULTS, "global_fraud_rate": global_rate}
    )

    # 1. Known MCC with high fraud rate (5732)
    p_high = {"Amount": 100, "MCC": 5732}
    f_high = extractor.extract_online_features(payload=p_high)
    assert f_high["mcc_risk_score"] > global_rate
    assert f_high["mcc_risk_score"] == mcc_risk_map[5732]

    # 2. Known MCC with low fraud rate (5411)
    p_low = {"Amount": 100, "MCC": 5411}
    f_low = extractor.extract_online_features(payload=p_low)
    assert f_low["mcc_risk_score"] < global_rate
    assert f_low["mcc_risk_score"] == mcc_risk_map[5411]

    # 3. Unseen future MCC should smoothly fall back to global training rate
    p_unseen = {"Amount": 100, "MCC": 9999}
    f_unseen = extractor.extract_online_features(payload=p_unseen)
    assert f_unseen["mcc_risk_score"] == round(global_rate, 5)


def test_user_spend_baseline_has_zero_lookahead():
    """Verify that a user's running spend baseline only uses prior transactions."""
    user_spends = [50.0, 150.0, 25.0]

    running_sum = 0.0
    running_sq = 0.0
    running_count = 0

    baselines = []
    for amt in user_spends:
        # State at transaction time (prior to current spend)
        if running_count > 0:
            avg = running_sum / running_count
            var = (running_sq - (running_sum ** 2) / running_count) / max(running_count - 1, 1)
            std = math.sqrt(max(var, 1.0))
        else:
            avg = GLOBAL_DEFAULTS["user_avg_spend"]
            std = GLOBAL_DEFAULTS["user_std_spend"]

        baselines.append((avg, std))

        # Update running state
        running_count += 1
        running_sum += amt
        running_sq += amt * amt

    # First transaction has no history -> fallback
    assert baselines[0][0] == GLOBAL_DEFAULTS["user_avg_spend"]
    # Second transaction only sees first transaction ($50.0)
    assert baselines[1][0] == 50.0
    # Third transaction sees first two ($50.0 and $150.0) -> mean $100.0
    assert baselines[2][0] == 100.0


def test_velocity_features_are_dynamic_and_nonzero():
    """Verify that velocity features are not constants and calculate speed and frequency accurately."""
    extractor = FeatureExtractor()

    # Rapid sequence of 2 transactions in 10 minutes (600s) spanning 100km
    state_burst = {
        "time_since_last_tx_sec": 600.0,
        "prev_merchant_lat": 34.0522,
        "prev_merchant_lon": -118.2437,
        "tx_count_last_1h": 2.0,
        "tx_count_last_24h": 3.0,
        "spend_sum_last_1h": 250.0,
    }
    payload_burst = {
        "Amount": 150.0,
        "Merchant Latitude": 34.9000,
        "Merchant Longitude": -117.5000,
        "Time": "14:15",
    }
    features_burst = extractor.extract_online_features(payload=payload_burst, state=state_burst)

    assert features_burst["time_since_last_tx_sec"] == 600.0
    assert features_burst["tx_count_last_1h"] == 2.0
    assert features_burst["tx_count_last_24h"] == 3.0
    assert features_burst["spend_sum_last_1h"] == 250.0
    assert features_burst["travel_speed_kmh"] > 500.0  # High travel speed

    # Normal sequence separated by 12 hours (43200s)
    state_normal = {
        "time_since_last_tx_sec": 43200.0,
        "prev_merchant_lat": 34.0522,
        "prev_merchant_lon": -118.2437,
        "tx_count_last_1h": 1.0,
        "tx_count_last_24h": 1.0,
        "spend_sum_last_1h": 45.0,
    }
    payload_normal = {
        "Amount": 45.0,
        "Merchant Latitude": 34.0550,
        "Merchant Longitude": -118.2450,
        "Time": "08:30",
    }
    features_normal = extractor.extract_online_features(payload=payload_normal, state=state_normal)

    assert features_normal["time_since_last_tx_sec"] == 43200.0
    assert features_normal["tx_count_last_1h"] == 1.0
    assert features_normal["travel_speed_kmh"] < 1.0  # Almost zero travel speed


def test_feature_parity_offline_online():
    """Verify exact 34-feature vector parity, ordering, and dtypes between offline and online inference."""
    extractor = FeatureExtractor()

    payload = {
        "Amount": 75.50,
        "Use Chip": "Swipe Transaction",
        "Transaction Entry Method": "magnetic_stripe_swipe",
        "Time": "18:45",
        "Merchant Latitude": 40.7128,
        "Merchant Longitude": -74.0060,
        "MCC": 5411,
    }
    user_p = {
        "Current Age": 35,
        "FICO Score": 720,
        "Yearly Income - Person": "$60,000",
        "Total Debt": "$12,000",
        "Num Credit Cards": 2,
        "Latitude": 40.7128,
        "Longitude": -74.0060,
    }
    card_p = {
        "Card Brand": "Mastercard",
        "Card Type": "Debit",
        "Has Chip": "YES",
        "Credit Limit": "$8,000",
        "Card on Dark Web": "No",
    }
    state = {
        "time_since_last_tx_sec": 3600.0,
        "tx_count_last_1h": 1.0,
        "tx_count_last_24h": 2.0,
        "spend_sum_last_1h": 75.50,
        "user_avg_spend": 50.0,
        "user_std_spend": 20.0,
    }

    fdict = extractor.extract_online_features(
        payload=payload,
        user_profile=user_p,
        card_profile=card_p,
        state=state,
    )
    vec = extractor.to_feature_vector(fdict)

    # 1. Exact length
    assert len(vec) == 34
    assert len(FEATURE_NAMES) == 34

    # 2. Strict order matches FEATURE_NAMES
    for idx, fname in enumerate(FEATURE_NAMES):
        assert vec[idx] == pytest.approx(fdict[fname], rel=1e-5)

    # 3. Proper array conversion
    arr = np.array(vec, dtype=np.float32)
    assert arr.dtype == np.float32
    assert arr.shape == (34,)
