"""Redis-backed real-time state store with in-memory fallback for low-latency feature hydration."""

from __future__ import annotations

import csv
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
import math

try:
    from ml.feature_engineering import GLOBAL_DEFAULTS
except ImportError:
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

try:
    import redis
    HAS_REDIS = True
except ImportError:
    redis = None  # type: ignore
    HAS_REDIS = False

logger = logging.getLogger(__name__)


class StateStore:
    """Manages static reference caches and dynamic sliding-window behavioral state for fraud scoring."""

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        enable_in_memory_fallback: bool = True,
        ttl_seconds: int = 172800,  # 48 hours TTL
        redis_client: Optional[Any] = None,
    ):
        self.redis_url = redis_url
        self.enable_fallback = enable_in_memory_fallback
        self.ttl = ttl_seconds
        self._redis_client = redis_client
        self._using_fallback = False

        # Local in-memory structures for fallback
        self._local_users: Dict[int, Dict[str, Any]] = {}
        self._local_cards: Dict[str, Dict[str, Any]] = {}
        self._local_user_tx_history: Dict[str, List[Tuple[float, float]]] = {}  # card_key -> list of (timestamp_epoch, amount)
        self._local_last_tx: Dict[str, Dict[str, Any]] = {}  # card_key -> {lat, lon, epoch}
        self._local_user_spend_stats: Dict[int, Dict[str, float]] = {}  # user_id -> {sum, count, sq_sum}
        self._seen_event_ids: set[str] = set()

        if self._redis_client is None:
            self._connect()

    def _connect(self) -> None:
        """Attempt connection to Redis instance."""
        if not HAS_REDIS:
            logger.warning("redis-py is not installed; operating in In-Memory fallback mode.")
            self._using_fallback = True
            return

        try:
            client = redis.Redis.from_url(
                self.redis_url,
                socket_connect_timeout=2.0,
                socket_timeout=2.0,
                decode_responses=True
            )
            client.ping()
            self._redis_client = client
            self._using_fallback = False
            logger.info("Successfully connected to Redis feature store at %s", self.redis_url)
        except Exception as ex:
            if self.enable_fallback:
                logger.warning("Redis connection failed (%s); switching to In-Memory fallback mode.", ex)
                self._using_fallback = True
                self._redis_client = None
            else:
                raise

    @property
    def is_using_fallback(self) -> bool:
        return self._using_fallback

    def preload_reference_data(
        self,
        users_csv_path: Union[str, Path] = "sd254_users.csv",
        cards_csv_path: Union[str, Path] = "sd254_cards.csv",
    ) -> Tuple[int, int]:
        """Preload static demographic user and card profiles into cache."""
        users_p = Path(users_csv_path)
        cards_p = Path(cards_csv_path)
        user_count = 0
        card_count = 0

        # Load users
        if users_p.exists():
            with open(users_p, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for idx, row in enumerate(reader):
                    u_id = idx  # IBM sd254_users implicit index is User ID
                    profile = {
                        "user_id": u_id,
                        "person_name": row.get("Person", ""),
                        "current_age": float(row.get("Current Age", 40)),
                        "gender": row.get("Gender", "Unknown"),
                        "city": row.get("City", ""),
                        "state": row.get("State", ""),
                        "latitude": float(row.get("Latitude", 0.0) or 0.0),
                        "longitude": float(row.get("Longitude", 0.0) or 0.0),
                        "yearly_income": row.get("Yearly Income - Person", "$50000"),
                        "total_debt": row.get("Total Debt", "$20000"),
                        "fico_score": float(row.get("FICO Score", 700)),
                        "num_credit_cards": float(row.get("Num Credit Cards", 3)),
                    }
                    if not self._using_fallback and self._redis_client:
                        try:
                            self._redis_client.set(f"profile:user:{u_id}", json.dumps(profile))
                        except Exception as ex:
                            logger.warning("Redis preload user %s failed: %s; storing in local cache", u_id, ex)
                            self._local_users[u_id] = profile
                    else:
                        self._local_users[u_id] = profile
                    user_count += 1

        # Load cards
        if cards_p.exists():
            with open(cards_p, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        u_id = int(row.get("User", 0))
                        c_idx = int(row.get("CARD INDEX", 0))
                    except (ValueError, TypeError):
                        continue
                    card_key = f"{u_id}:{c_idx}"
                    card_prof = {
                        "user_id": u_id,
                        "card_index": c_idx,
                        "card_brand": row.get("Card Brand", "Visa"),
                        "card_type": row.get("Card Type", "Debit"),
                        "has_chip": row.get("Has Chip", "YES"),
                        "credit_limit": row.get("Credit Limit", "$5000"),
                        "card_on_dark_web": row.get("Card on Dark Web", "No"),
                        "Acct Open Date": row.get("Acct Open Date", "01/2000"),
                        "Year PIN last Changed": row.get("Year PIN last Changed", 2000),
                    }
                    if not self._using_fallback and self._redis_client:
                        try:
                            self._redis_client.set(f"profile:card:{card_key}", json.dumps(card_prof))
                        except Exception as ex:
                            logger.warning("Redis preload card %s failed: %s; storing in local cache", card_key, ex)
                            self._local_cards[card_key] = card_prof
                    else:
                        self._local_cards[card_key] = card_prof
                    card_count += 1

        logger.info("Preloaded %d users and %d cards into state store (fallback=%s)", user_count, card_count, self._using_fallback)
        return user_count, card_count

    def get_user_profile(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Fetch static user profile."""
        if not self._using_fallback and self._redis_client:
            try:
                raw = self._redis_client.get(f"profile:user:{user_id}")
                if raw:
                    return json.loads(raw)
            except Exception as ex:
                logger.warning("Redis user profile lookup failed for user %s (%s); falling back to local cache", user_id, ex)
        return self._local_users.get(user_id)

    def get_card_profile(self, user_id: int, card_index: int) -> Optional[Dict[str, Any]]:
        """Fetch static card profile."""
        key = f"{user_id}:{card_index}"
        if not self._using_fallback and self._redis_client:
            try:
                raw = self._redis_client.get(f"profile:card:{key}")
                if raw:
                    return json.loads(raw)
            except Exception as ex:
                logger.warning("Redis card profile lookup failed for card %s (%s); falling back to local cache", key, ex)
        return self._local_cards.get(key)

    def get_and_update_state(
        self,
        user_id: int,
        card_index: int,
        amount: float,
        epoch_sec: float,
        merchant_lat: Optional[float],
        merchant_lon: Optional[float],
        event_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Query rolling window features and update state atomically.
        
        Calculates:
        - time_since_last_tx_sec
        - prev_merchant_lat, prev_merchant_lon
        - tx_count_last_1h, tx_count_last_24h
        - spend_sum_last_1h
        - user_avg_spend, user_std_spend
        """
        card_key = f"{user_id}:{card_index}"
        user_key = f"{user_id}"

        # Default fallback values (matching GLOBAL_DEFAULTS)
        time_since_last_tx_sec = 86400.0
        prev_m_lat = None
        prev_m_lon = None
        tx_count_1h = 1.0
        tx_count_24h = 1.0
        spend_sum_1h = amount
        user_avg = GLOBAL_DEFAULTS["user_avg_spend"]
        user_std = GLOBAL_DEFAULTS["user_std_spend"]

        if not self._using_fallback and self._redis_client:
            try:
                pipe = self._redis_client.pipeline()
                last_tx_key = f"last_tx:{card_key}"
                window_key = f"tx_window:{card_key}"
                stats_key = f"spend_stats:{user_key}"

                # 1. Get last tx metadata
                pipe.get(last_tx_key)

                # 2. Query counts in 1h (3600s) and 24h (86400s)
                cutoff_1h = epoch_sec - 3600.0
                cutoff_24h = epoch_sec - 86400.0
                pipe.zcount(window_key, cutoff_1h, epoch_sec)
                pipe.zcount(window_key, cutoff_24h, epoch_sec)
                pipe.zrangebyscore(window_key, cutoff_1h, epoch_sec)

                # 3. Running user stats
                pipe.hgetall(stats_key)

                results = pipe.execute()
                last_tx_raw = results[0]
                c_1h = results[1]
                c_24h = results[2]
                items_1h = results[3]
                stats_raw = results[4]

                # Parse last tx
                if last_tx_raw:
                    last_obj = json.loads(last_tx_raw)
                    prev_epoch = float(last_obj.get("epoch", epoch_sec))
                    time_since_last_tx_sec = max(0.1, epoch_sec - prev_epoch)
                    prev_m_lat = last_obj.get("lat")
                    prev_m_lon = last_obj.get("lon")

                tx_count_1h = float(c_1h + 1)
                tx_count_24h = float(c_24h + 1)

                # Sum amounts in 1h
                sum_1h = amount
                for it in items_1h:
                    try:
                        amt_part = float(it.split(":")[0])
                        sum_1h += amt_part
                    except (ValueError, IndexError):
                        pass
                spend_sum_1h = sum_1h

                # 4. Compute running user spend statistics prior to current transaction
                if stats_raw and "total_sum" in stats_raw and "total_count" in stats_raw:
                    tot_s = float(stats_raw["total_sum"])
                    tot_c = float(stats_raw["total_count"])
                    tot_sq = float(stats_raw.get("total_sq_sum", tot_s * tot_s))
                    if tot_c > 0:
                        user_avg = tot_s / tot_c
                        if tot_c >= 2:
                            var = (tot_sq - (tot_s ** 2) / tot_c) / (tot_c - 1.0)
                            user_std = math.sqrt(max(var, 1.0))
                        else:
                            user_std = GLOBAL_DEFAULTS["user_std_spend"]
                    else:
                        user_avg = GLOBAL_DEFAULTS["user_avg_spend"]
                        user_std = GLOBAL_DEFAULTS["user_std_spend"]

                # 5. Write updates back atomically (skipping state increment if event is duplicate)
                is_duplicate = False
                if event_id:
                    # Idempotent registration with NX (set only if not exists)
                    dedup_key = f"dedup:{event_id}"
                    was_set = self._redis_client.set(dedup_key, "1", nx=True, ex=self.ttl)
                    if not was_set:
                        is_duplicate = True
                        logger.debug("Duplicate event %s detected in Redis; skipping state mutation.", event_id)

                if not is_duplicate:
                    update_pipe = self._redis_client.pipeline()
                    # Store new last tx
                    new_last = json.dumps({"lat": merchant_lat, "lon": merchant_lon, "epoch": epoch_sec})
                    update_pipe.set(last_tx_key, new_last, ex=self.ttl)

                    # Add to sorted set (score=epoch_sec, member=f"{amount}:{epoch_sec}")
                    update_pipe.zadd(window_key, {f"{amount}:{epoch_sec}": epoch_sec})
                    # Prune older than 24h
                    update_pipe.zremrangebyscore(window_key, "-inf", cutoff_24h)
                    update_pipe.expire(window_key, self.ttl)

                    # Increment running spend stats
                    update_pipe.hincrbyfloat(stats_key, "total_sum", float(amount))
                    update_pipe.hincrby(stats_key, "total_count", 1)
                    update_pipe.hincrbyfloat(stats_key, "total_sq_sum", float(amount * amount))
                    update_pipe.expire(stats_key, self.ttl)

                    update_pipe.execute()

                return {
                    "time_since_last_tx_sec": time_since_last_tx_sec,
                    "prev_merchant_lat": prev_m_lat,
                    "prev_merchant_lon": prev_m_lon,
                    "tx_count_last_1h": tx_count_1h,
                    "tx_count_last_24h": tx_count_24h,
                    "spend_sum_last_1h": spend_sum_1h,
                    "user_avg_spend": user_avg,
                    "user_std_spend": user_std,
                }
            except Exception as ex:
                logger.warning(
                    "Redis operation failed (%s); degrading to in-memory fallback for event %s. Note: local in-memory state does not reflect shared Redis state.",
                    ex,
                    event_id,
                )

        # In-Memory fallback implementation
        last_info = self._local_last_tx.get(card_key)
        if last_info:
            prev_epoch = last_info["epoch"]
            time_since_last_tx_sec = max(0.1, epoch_sec - prev_epoch)
            prev_m_lat = last_info.get("lat")
            prev_m_lon = last_info.get("lon")

        # Sliding window counts
        history = self._local_user_tx_history.setdefault(card_key, [])
        cutoff_1h = epoch_sec - 3600.0
        cutoff_24h = epoch_sec - 86400.0

        # Prune older than 24h
        history = [entry for entry in history if entry[0] >= cutoff_24h]
        self._local_user_tx_history[card_key] = history

        # Calculate counts and sum
        c_1h = sum(1 for e in history if e[0] >= cutoff_1h)
        s_1h = sum(e[1] for e in history if e[0] >= cutoff_1h) + amount

        tx_count_1h = float(c_1h + 1)
        tx_count_24h = float(len(history) + 1)
        spend_sum_1h = s_1h

        # Compute running user spend stats prior to current transaction
        ustats = self._local_user_spend_stats.get(user_id)
        if ustats and ustats.get("count", 0.0) > 0:
            tot_c = float(ustats["count"])
            tot_s = float(ustats["sum"])
            tot_sq = float(ustats.get("sq_sum", tot_s * tot_s))
            user_avg = tot_s / tot_c
            if tot_c >= 2:
                var = (tot_sq - (tot_s ** 2) / tot_c) / (tot_c - 1.0)
                user_std = math.sqrt(max(var, 1.0))
            else:
                user_std = GLOBAL_DEFAULTS["user_std_spend"]
        else:
            user_avg = GLOBAL_DEFAULTS["user_avg_spend"]
            user_std = GLOBAL_DEFAULTS["user_std_spend"]

        # Check in-memory duplication before mutating local state
        is_mem_dup = False
        if event_id:
            if event_id in self._seen_event_ids:
                is_mem_dup = True
                logger.debug("Duplicate event %s detected in-memory; skipping state mutation.", event_id)
            else:
                if len(self._seen_event_ids) > 50000:
                    self._seen_event_ids.clear()
                self._seen_event_ids.add(event_id)

        if not is_mem_dup:
            # Append new transaction
            history.append((epoch_sec, amount))
            self._local_last_tx[card_key] = {
                "lat": merchant_lat,
                "lon": merchant_lon,
                "epoch": epoch_sec,
            }

            # Update running user spend stats AFTER computing features for current transaction
            if ustats is None:
                self._local_user_spend_stats[user_id] = {
                    "sum": float(amount),
                    "count": 1.0,
                    "sq_sum": float(amount * amount),
                }
            else:
                ustats["sum"] += float(amount)
                ustats["count"] += 1.0
                ustats["sq_sum"] = ustats.get("sq_sum", 0.0) + float(amount * amount)

        return {
            "time_since_last_tx_sec": time_since_last_tx_sec,
            "prev_merchant_lat": prev_m_lat,
            "prev_merchant_lon": prev_m_lon,
            "tx_count_last_1h": tx_count_1h,
            "tx_count_last_24h": tx_count_24h,
            "spend_sum_last_1h": spend_sum_1h,
            "user_avg_spend": user_avg,
            "user_std_spend": user_std,
        }
