"""Unit and regression tests for StateStore feature parity, update order, and variance calculation."""

from __future__ import annotations

import json
import math
import time
from typing import Any, Dict, List, Optional, Tuple
import numpy as np
import pytest

from ml.feature_engineering import GLOBAL_DEFAULTS, FeatureExtractor
from ml.state_store import StateStore


class MockRedisPipeline:
    """Mock Redis Pipeline simulating atomic batch commands."""

    def __init__(self, store: MockRedisClient):
        self.store = store
        self.commands: List[Tuple[str, tuple, dict]] = []

    def get(self, key: str):
        self.commands.append(("get", (key,), {}))
        return self

    def zcount(self, key: str, min_score: float, max_score: float):
        self.commands.append(("zcount", (key, min_score, max_score), {}))
        return self

    def zrangebyscore(self, key: str, min_score: float, max_score: float):
        self.commands.append(("zrangebyscore", (key, min_score, max_score), {}))
        return self

    def hgetall(self, key: str):
        self.commands.append(("hgetall", (key,), {}))
        return self

    def set(self, key: str, value: str, ex: Optional[int] = None):
        self.commands.append(("set", (key, value), {"ex": ex}))
        return self

    def zadd(self, key: str, mapping: Dict[str, float]):
        self.commands.append(("zadd", (key, mapping), {}))
        return self

    def zremrangebyscore(self, key: str, min_score: Any, max_score: float):
        self.commands.append(("zremrangebyscore", (key, min_score, max_score), {}))
        return self

    def expire(self, key: str, ttl: int):
        self.commands.append(("expire", (key, ttl), {}))
        return self

    def hincrbyfloat(self, key: str, field: str, amount: float):
        self.commands.append(("hincrbyfloat", (key, field, amount), {}))
        return self

    def hincrby(self, key: str, field: str, amount: int):
        self.commands.append(("hincrby", (key, field, amount), {}))
        return self

    def execute(self) -> List[Any]:
        results = []
        for cmd, args, kwargs in self.commands:
            if cmd == "get":
                results.append(self.store.data.get(args[0]))
            elif cmd == "zcount":
                k, min_s, max_s = args
                zset = self.store.zsets.get(k, {})
                c = sum(1 for score in zset.values() if min_s <= score <= max_s)
                results.append(c)
            elif cmd == "zrangebyscore":
                k, min_s, max_s = args
                zset = self.store.zsets.get(k, {})
                members = [m for m, score in zset.items() if min_s <= score <= max_s]
                results.append(members)
            elif cmd == "hgetall":
                results.append(dict(self.store.hashes.get(args[0], {})))
            elif cmd == "set":
                self.store.data[args[0]] = args[1]
                results.append(True)
            elif cmd == "zadd":
                k, mapping = args
                zset = self.store.zsets.setdefault(k, {})
                zset.update(mapping)
                results.append(len(mapping))
            elif cmd == "zremrangebyscore":
                k, min_s, max_s = args
                zset = self.store.zsets.setdefault(k, {})
                to_del = [m for m, score in zset.items() if score <= max_s]
                for m in to_del:
                    del zset[m]
                results.append(len(to_del))
            elif cmd == "expire":
                results.append(True)
            elif cmd == "hincrbyfloat":
                k, field, val = args
                h = self.store.hashes.setdefault(k, {})
                curr = float(h.get(field, 0.0))
                h[field] = str(curr + float(val))
                results.append(curr + float(val))
            elif cmd == "hincrby":
                k, field, val = args
                h = self.store.hashes.setdefault(k, {})
                curr = int(float(h.get(field, 0)))
                h[field] = str(curr + int(val))
                results.append(curr + int(val))
        self.commands.clear()
        return results


class MockRedisClient:
    """Mock Redis Client supporting in-memory key-value, zset, and hash storage."""

    def __init__(self):
        self.data: Dict[str, str] = {}
        self.zsets: Dict[str, Dict[str, float]] = {}
        self.hashes: Dict[str, Dict[str, str]] = {}
        self.fail_mode = False

    def ping(self):
        if self.fail_mode:
            raise ConnectionError("Simulated Redis Connection Refused")
        return True

    def pipeline(self):
        if self.fail_mode:
            raise ConnectionError("Simulated Redis Pipeline Error")
        return MockRedisPipeline(self)


def test_first_transaction_cold_start():
    """Verify exact cold-start defaults on the first transaction for a user/card."""
    store = StateStore(enable_in_memory_fallback=True)
    store._using_fallback = True

    t0 = 1700000000.0
    st = store.get_and_update_state(
        user_id=101,
        card_index=0,
        amount=125.00,
        epoch_sec=t0,
        merchant_lat=37.7749,
        merchant_lon=-122.4194,
    )

    assert st["time_since_last_tx_sec"] == 86400.0
    assert st["prev_merchant_lat"] is None
    assert st["prev_merchant_lon"] is None
    assert st["tx_count_last_1h"] == 1.0
    assert st["tx_count_last_24h"] == 1.0
    assert st["spend_sum_last_1h"] == 125.00
    assert st["user_avg_spend"] == GLOBAL_DEFAULTS["user_avg_spend"]  # 48.50
    assert st["user_std_spend"] == GLOBAL_DEFAULTS["user_std_spend"]  # 38.20


def test_insufficient_history_one_prior_transaction():
    """Verify that with exactly 1 prior transaction, user_avg reflects tx1 while user_std defaults to 38.20."""
    store = StateStore(enable_in_memory_fallback=True)
    store._using_fallback = True

    t0 = 1700000000.0
    # Tx 1: $100.00
    store.get_and_update_state(
        user_id=202,
        card_index=0,
        amount=100.00,
        epoch_sec=t0,
        merchant_lat=37.7749,
        merchant_lon=-122.4194,
    )

    # Tx 2: $250.00 (30 minutes later)
    st2 = store.get_and_update_state(
        user_id=202,
        card_index=0,
        amount=250.00,
        epoch_sec=t0 + 1800.0,
        merchant_lat=37.7800,
        merchant_lon=-122.4200,
    )

    # Mean must reflect Tx 1 only ($100.00)
    assert st2["user_avg_spend"] == 100.00
    # Standard deviation with N=1 is undefined (N-1=0) -> must fallback to 38.20
    assert st2["user_std_spend"] == GLOBAL_DEFAULTS["user_std_spend"]
    # Tx 2's own amount ($250.00) must NOT be included in user_avg_spend
    assert st2["user_avg_spend"] != (100.00 + 250.00) / 2.0


def test_multiple_transactions_running_mean_and_sample_std():
    """Verify exact formula parity for running mean and sample standard deviation over multiple transactions."""
    store = StateStore(enable_in_memory_fallback=True)
    store._using_fallback = True

    t0 = 1700000000.0
    spends = [100.0, 200.0, 300.0, 400.0]

    states = []
    for i, amt in enumerate(spends):
        st = store.get_and_update_state(
            user_id=303,
            card_index=0,
            amount=amt,
            epoch_sec=t0 + i * 3600.0,
            merchant_lat=40.7128,
            merchant_lon=-74.0060,
        )
        states.append(st)

    # Tx 1: Prior [] -> Default
    assert states[0]["user_avg_spend"] == 48.50
    assert states[0]["user_std_spend"] == 38.20

    # Tx 2: Prior [100.0] -> Mean 100.0, Std 38.20
    assert states[1]["user_avg_spend"] == 100.0
    assert states[1]["user_std_spend"] == 38.20

    # Tx 3: Prior [100.0, 200.0]
    # Expected mean = 150.0
    # Expected sample variance = ((100-150)^2 + (200-150)^2) / 1 = 5000.0
    # Expected sample std = sqrt(5000) ~= 70.710678
    assert states[2]["user_avg_spend"] == 150.0
    assert states[2]["user_std_spend"] == pytest.approx(math.sqrt(5000.0), rel=1e-5)

    # Tx 4: Prior [100.0, 200.0, 300.0]
    # Expected mean = (100+200+300)/3 = 200.0
    # Expected sample variance = ((100-200)^2 + 0 + (300-200)^2) / 2 = 20000 / 2 = 10000.0
    # Expected sample std = sqrt(10000) = 100.0
    assert states[3]["user_avg_spend"] == 200.0
    assert states[3]["user_std_spend"] == pytest.approx(100.0, rel=1e-5)


def test_redis_path_parity_with_in_memory_fallback():
    """Verify that the Redis-backed pipeline and In-Memory fallback produce IDENTICAL state results."""
    mock_redis = MockRedisClient()

    # Store A: Redis-backed
    store_redis = StateStore(enable_in_memory_fallback=False, redis_client=mock_redis)

    # Store B: In-Memory
    store_mem = StateStore(enable_in_memory_fallback=True)
    store_mem._using_fallback = True

    t0 = 1700000000.0
    test_transactions = [
        (45.50, t0, 34.05, -118.25),
        (120.00, t0 + 600.0, 34.10, -118.20),
        (85.00, t0 + 1200.0, 34.15, -118.15),
        (350.00, t0 + 7200.0, 34.20, -118.10),
    ]

    for amt, epoch, lat, lon in test_transactions:
        st_redis = store_redis.get_and_update_state(
            user_id=404,
            card_index=1,
            amount=amt,
            epoch_sec=epoch,
            merchant_lat=lat,
            merchant_lon=lon,
        )
        st_mem = store_mem.get_and_update_state(
            user_id=404,
            card_index=1,
            amount=amt,
            epoch_sec=epoch,
            merchant_lat=lat,
            merchant_lon=lon,
        )

        assert st_redis["time_since_last_tx_sec"] == pytest.approx(st_mem["time_since_last_tx_sec"])
        assert st_redis["prev_merchant_lat"] == st_mem["prev_merchant_lat"]
        assert st_redis["prev_merchant_lon"] == st_mem["prev_merchant_lon"]
        assert st_redis["tx_count_last_1h"] == st_mem["tx_count_last_1h"]
        assert st_redis["tx_count_last_24h"] == st_mem["tx_count_last_24h"]
        assert st_redis["spend_sum_last_1h"] == pytest.approx(st_mem["spend_sum_last_1h"])
        assert st_redis["user_avg_spend"] == pytest.approx(st_mem["user_avg_spend"], rel=1e-5)
        assert st_redis["user_std_spend"] == pytest.approx(st_mem["user_std_spend"], rel=1e-5)


def test_state_store_failure_fallback_and_recovery():
    """Verify that StateStore gracefully falls back to in-memory mode when Redis throws exceptions."""
    mock_redis = MockRedisClient()
    mock_redis.fail_mode = True  # Trigger failure

    store = StateStore(enable_in_memory_fallback=True, redis_client=mock_redis)

    t0 = 1700000000.0
    # Should not raise exception; must catch and fallback to in-memory
    st = store.get_and_update_state(
        user_id=505,
        card_index=0,
        amount=60.0,
        epoch_sec=t0,
        merchant_lat=30.0,
        merchant_lon=-90.0,
    )
    assert st["tx_count_last_1h"] == 1.0
    assert st["user_avg_spend"] == 48.50


def test_equal_timestamp_duplicate_event_handling():
    """Verify safe behavior when two transactions arrive with identical timestamps (dt = 0)."""
    store = StateStore(enable_in_memory_fallback=True)
    store._using_fallback = True

    t0 = 1700000000.0
    st1 = store.get_and_update_state(
        user_id=606,
        card_index=0,
        amount=10.0,
        epoch_sec=t0,
        merchant_lat=40.0,
        merchant_lon=-75.0,
    )
    assert st1["time_since_last_tx_sec"] == 86400.0

    # Second transaction at the EXACT same timestamp on the same card
    st2 = store.get_and_update_state(
        user_id=606,
        card_index=0,
        amount=20.0,
        epoch_sec=t0,
        merchant_lat=40.0,
        merchant_lon=-75.0,
    )
    # Must floor dt at 0.1s to avoid division by zero
    assert st2["time_since_last_tx_sec"] == 0.1
    # Sliding window counts must see prior transaction
    assert st2["tx_count_last_1h"] == 2.0
    assert st2["spend_sum_last_1h"] == 30.0  # 10 + 20


def test_explicit_warning_logged_on_redis_failure(caplog):
    """Verify that Redis pipeline failure logs an explicit WARNING indicating in-memory degradation."""
    import logging

    mock_redis = MockRedisClient()
    mock_redis.fail_mode = True

    store = StateStore(enable_in_memory_fallback=True, redis_client=mock_redis)

    with caplog.at_level(logging.WARNING):
        st = store.get_and_update_state(
            user_id=707,
            card_index=0,
            amount=50.0,
            epoch_sec=1700000000.0,
            merchant_lat=35.0,
            merchant_lon=-119.0,
            event_id="evt-warn-123",
        )

    # State computation completed via in-memory fallback
    assert st["tx_count_last_1h"] == 1.0
    # Observable WARNING must have been emitted
    assert any("Redis operation failed" in record.message for record in caplog.records)
    assert any("degrading to in-memory fallback" in record.message for record in caplog.records)
    assert any("evt-warn-123" in record.message for record in caplog.records)


def test_profile_lookup_fallback_on_redis_error(caplog):
    """Verify that profile lookup degrades to local cache with an observable WARNING when Redis fails."""
    import logging

    mock_redis = MockRedisClient()
    mock_redis.fail_mode = True

    store = StateStore(enable_in_memory_fallback=True, redis_client=mock_redis)
    store._local_users[808] = {"user_id": 808, "person_name": "Test User"}
    store._local_cards["808:0"] = {"user_id": 808, "card_index": 0, "card_brand": "Visa"}

    with caplog.at_level(logging.WARNING):
        u_prof = store.get_user_profile(808)
        c_prof = store.get_card_profile(808, 0)

    assert u_prof is not None and u_prof["person_name"] == "Test User"
    assert c_prof is not None and c_prof["card_brand"] == "Visa"
    assert any("Redis user profile lookup failed" in record.message for record in caplog.records)
    assert any("Redis card profile lookup failed" in record.message for record in caplog.records)


def test_tls_rediss_url_support():
    """Verify that StateStore accepts a rediss:// URL for in-transit TLS encryption."""
    store = StateStore(
        redis_url="rediss://dummy-endpoint.cache.amazonaws.com:6379",
        enable_in_memory_fallback=True,
    )
    assert store.redis_url.startswith("rediss://")
    # Since endpoint is unreachable locally, it gracefully falls back
    assert store.is_using_fallback is True

