# Step-by-Step Code Walkthrough: AWS Redis Feature Store Integration

This document provides a line-by-line and function-by-function technical inspection of all source files modified or introduced to support the **AWS ElastiCache for Redis Online Feature Store** integration.

---

## 1. `src/ml/state_store.py` — Real-Time State Store Engine

- **File Path:** [`src/ml/state_store.py`](file:///home/syd/Downloads/archive/src/ml/state_store.py)
- **Primary Responsibility:** Manages static reference dimension caching (users and cards) and atomic sliding-window state manipulation in Redis with an automated in-memory fallback mechanism.

### Key Classes and Methods

#### `StateStore.__init__(...)` (Lines 41–64)
- **Inputs:** `redis_url` (str), `enable_in_memory_fallback` (bool), `ttl_seconds` (int, default 172800 / 48h), `redis_client` (optional pre-instantiated client).
- **Outputs:** Initialized `StateStore` instance with either an active Redis connection or in-memory fallback dictionaries.
- **Implementation Detail:** Sets up both Redis connection handlers and local dictionary structures (`_local_users`, `_local_cards`, `_local_user_tx_history`, `_local_last_tx`, `_local_user_spend_stats`, and `_seen_event_ids`).

#### `StateStore._connect()` (Lines 65–90)
```python
client = redis.Redis.from_url(
    self.redis_url,
    socket_connect_timeout=2.0,
    socket_timeout=2.0,
    decode_responses=True
)
client.ping()
self._redis_client = client
self._using_fallback = False
```
- **Why this choice was made:**
  - Strict 2.0s socket connect and read timeouts prevent blocking the streaming consumer indefinitely during network disruptions.
  - `decode_responses=True` ensures Redis strings, Sorted Set members, and Hash fields are returned directly as Python `str` rather than `bytes`, avoiding repetitive decoding overhead.
  - If `HAS_REDIS` is False (package missing) or `client.ping()` fails, the exception is caught, setting `self._using_fallback = True` to preserve pipeline continuity.

#### `StateStore.get_and_update_state(...)` (Lines 194–336)
- **Inputs:** `user_id: int`, `card_index: int`, `amount: float`, `epoch_sec: float`, `merchant_lat: float`, `merchant_lon: float`, `event_id: Optional[str]`.
- **Outputs:** `Dict[str, Any]` containing:
  - `time_since_last_tx_sec`: float
  - `prev_merchant_lat`: Optional[float]
  - `prev_merchant_lon`: Optional[float]
  - `tx_count_last_1h`: float
  - `tx_count_last_24h`: float
  - `spend_sum_last_1h`: float
  - `user_avg_spend`: float
  - `user_std_spend`: float

##### Read Phase: Pipeline 1 (Lines 228–252)
```python
pipe = self._redis_client.pipeline()
pipe.get(last_tx_key)
pipe.zcount(window_key, cutoff_1h, epoch_sec)
pipe.zcount(window_key, cutoff_24h, epoch_sec)
pipe.zrangebyscore(window_key, cutoff_1h, epoch_sec)
pipe.hgetall(stats_key)
results = pipe.execute()
```
- **Why this choice was made:**
  - Combines 5 distinct Redis read operations into a single TCP round-trip. At 1–2ms network round-trip time across subnets, pipelining reduces query latency from ~8ms down to ~1.5ms.

##### Idempotency and Deduplication Check (Lines 291–299)
```python
if event_id:
    dedup_key = f"dedup:{event_id}"
    was_set = self._redis_client.set(dedup_key, "1", nx=True, ex=self.ttl)
    if not was_set:
        is_duplicate = True
```
- **Why this choice was made:**
  - `nx=True` (SET if Not eXists) provides an atomic test-and-set primitive in Redis. If a Kafka consumer group rebalances or re-delivers an offset, `was_set` evaluates to `None`/`False`, skipping mutation and preventing count inflation.

##### Write Phase: Pipeline 2 (Lines 301–319)
```python
update_pipe = self._redis_client.pipeline()
update_pipe.set(last_tx_key, new_last, ex=self.ttl)
update_pipe.zadd(window_key, {f"{amount}:{epoch_sec}": epoch_sec})
update_pipe.zremrangebyscore(window_key, "-inf", cutoff_24h)
update_pipe.expire(window_key, self.ttl)
update_pipe.hincrbyfloat(stats_key, "total_sum", float(amount))
update_pipe.hincrby(stats_key, "total_count", 1)
update_pipe.hincrbyfloat(stats_key, "total_sq_sum", float(amount * amount))
update_pipe.expire(stats_key, self.ttl)
update_pipe.execute()
```
- **Why this choice was made:**
  - `zremrangebyscore` automatically evicts any transaction older than 24 hours ($O(\log N + M)$), ensuring constant memory footprint per active card.
  - `hincrbyfloat` and `hincrby` update online accumulators atomically without requiring client-side read-modify-write locks.

- **Unit Test Coverage:**
  - `test_state_store_sliding_window` in [`tests/unit/test_ml_fraud.py`](file:///home/syd/Downloads/archive/tests/unit/test_ml_fraud.py#L114)
  - `test_redis_path_parity_with_in_memory_fallback` in [`tests/unit/test_state_store_parity.py`](file:///home/syd/Downloads/archive/tests/unit/test_state_store_parity.py#L235)
  - `test_duplicate_event_handling_idempotency` in [`tests/unit/test_ml_fraud.py`](file:///home/syd/Downloads/archive/tests/unit/test_ml_fraud.py#L193)
- **Remaining Edge Cases:**
  - If Redis fails mid-stream, fallback state is isolated to container RAM; no reconciliation occurs when Redis reconnects.

---

## 2. `src/ml/fraud_consumer.py` — Dedicated Streaming Consumer

- **File Path:** [`src/ml/fraud_consumer.py`](file:///home/syd/Downloads/archive/src/ml/fraud_consumer.py)
- **Primary Responsibility:** Coordinates Kafka ingestion from MSK, feature extraction, LightGBM inference, alert dispatch, and manual offset commits.

### Key Classes and Methods

#### `FraudStreamingConsumer.process_transaction(...)` (Lines 144–279)
- **Inputs:** `event_envelope: Dict[str, Any]` (deserialized JSON Kafka message).
- **Outputs:** `Dict[str, Any]` alert structure containing prediction decision (`ALERT` or `PASS`), `fraud_probability`, `risk_factors`, and `latency_ms`.
- **Execution Flow:**
  1. Validates dictionary structure; handles corrupt envelopes safely.
  2. Extracts user ID, card index, amount, and GPS coordinates.
  3. Hydrates user and card profiles from `StateStore`.
  4. Calls `StateStore.get_and_update_state(...)`.
  5. Computes feature dictionary using `FeatureExtractor.extract_online_features(...)`.
  6. Converts dictionary to 34-element vector using `FeatureExtractor.to_feature_vector(...)`.
  7. Invokes `model.predict_proba(X_in)[0][1]`.
  8. Flags fraud if `fraud_prob >= self.threshold` (0.20 or metadata threshold).
  9. Evaluates explainability factors via `identify_risk_factors(...)`.

#### `FraudStreamingConsumer.consume_and_process_single_message(...)` (Lines 281–356)
```python
# 2. Score transaction
alert = self.process_transaction(envelope)

# 3. Publish alert if required
requires_alert = alert.get("is_fraud_predicted", False)
if requires_alert:
    prod.produce(
        topic=alert_topic,
        key=str(alert.get("user_id", 0)).encode("utf-8"),
        value=alert_bytes,
        on_delivery=_on_delivery,
    )
    unflushed = prod.flush(timeout=5.0)
    if unflushed > 0 or delivery_error is not None:
        raise RuntimeError(...)

# 4. Manually and synchronously commit offset for this message
if cons is not None:
    cons.commit(message=msg, asynchronous=False)
```
- **Why this choice was made:**
  - **Manual Offset Commit:** `enable.auto.commit` is disabled (`False`). The Kafka consumer offset is committed strictly *after* successful model scoring and broker acknowledgment of any required alert.
  - **Synchronous Flush:** `prod.flush(timeout=5.0)` guarantees that alerts are delivered to MSK before advancing the consumer offset.
  - **Fault Barrier:** If `prod.produce` or `prod.flush` fails, a `RuntimeError` is raised, bypassing `consumer.commit`. This guarantees at-least-once processing semantics.

- **Unit Test Coverage:**
  - `test_manual_offset_commit_on_pass` in [`tests/unit/test_ml_fraud.py`](file:///home/syd/Downloads/archive/tests/unit/test_ml_fraud.py#L295)
  - `test_manual_offset_commit_on_alert` in [`tests/unit/test_ml_fraud.py`](file:///home/syd/Downloads/archive/tests/unit/test_ml_fraud.py#L335)
  - `test_producer_delivery_failure_prevents_offset_commit` in [`tests/unit/test_ml_fraud.py`](file:///home/syd/Downloads/archive/tests/unit/test_ml_fraud.py#L380)

---

## 3. `src/ml/feature_engineering.py` — Unified Feature Extractor

- **File Path:** [`src/ml/feature_engineering.py`](file:///home/syd/Downloads/archive/src/ml/feature_engineering.py)
- **Primary Responsibility:** Enforces zero train-serve skew by implementing identical mathematical transformations for offline training and online streaming.

### Key Implementation Details

#### Great-Circle Haversine Distance (Lines 14–42)
```python
def haversine_distance(lat1, lon1, lat2, lon2) -> float:
    r = 6371.0 # Earth radius in kilometers
    # ... spherical trigonometry computation ...
    return float(r * c)
```
- Used to calculate both `distance_to_home_km` (between merchant and user coordinates) and `travel_speed_kmh` (between successive merchant coordinates).

#### Canonical Feature Ordering: `FEATURE_NAMES` (Lines 108–143)
- Strictly defines the 34 input dimensions expected by the trained LightGBM model:
  `amount`, `log_amount`, `spend_deviation_ratio`, `credit_limit_utilization`, `amount_zscore`, `current_age`, `fico_score`, `yearly_income`, `total_debt`, `num_credit_cards`, `credit_limit`, `card_age_at_tx_years`, `years_since_pin_changed`, `distance_to_home_km`, `time_since_last_tx_sec`, `travel_speed_kmh`, `tx_count_last_1h`, `tx_count_last_24h`, `spend_sum_last_1h`, `is_night_tx`, `sin_hour`, `cos_hour`, `has_chip`, `card_on_dark_web`, `use_chip_code`, `card_brand_code`, `card_type_code`, `entry_method_code`, `is_online_tx`, `is_credential_error`, `is_insufficient_balance`, `is_refund`, `mcc_high_risk_flag`, `mcc_risk_score`.

- **Unit Test Coverage:**
  - `test_haversine_distance` in [`tests/unit/test_ml_fraud.py`](file:///home/syd/Downloads/archive/tests/unit/test_ml_fraud.py#L19)
  - `test_clean_currency` in [`tests/unit/test_ml_fraud.py`](file:///home/syd/Downloads/archive/tests/unit/test_ml_fraud.py#L34)
  - `test_feature_extractor_online` in [`tests/unit/test_ml_fraud.py`](file:///home/syd/Downloads/archive/tests/unit/test_ml_fraud.py#L41)

---

## 4. `src/synthetic_streaming/config.py` — Dynamic Configuration Mapping

- **File Path:** [`src/synthetic_streaming/config.py`](file:///home/syd/Downloads/archive/src/synthetic_streaming/config.py)
- **Primary Responsibility:** Manages typed, hierarchical application settings using Pydantic Settings v2.

### Code Modification for Redis Feature Store
```python
class MLFraudConfig(BaseModel):
    """Configuration for real-time fraud scoring consumer."""
    model_path: str = "models/fraud_lgbm_model.joblib"
    redis_url: str = Field(
        default_factory=lambda: os.environ.get("REDIS_URL", "redis://localhost:6379"),
        description="Redis connection URL for real-time feature state"
    )
    fraud_threshold: float = 0.20
    fraud_alerts_topic: str = "fraud_alerts"
    enable_in_memory_fallback: bool = True
    consumer_group: str = "fraud-detection-engine"
    emit_all_evaluations: bool = False
```
- **Integration Mechanism:**
  - Added `ml: MLFraudConfig = Field(default_factory=MLFraudConfig)` to `GeneratorConfig` (Line 164).
  - Pydantic Settings parses environment variables with double-underscore delimiters:
    `SYNTH_ML__REDIS_URL="rediss://master.sic-feature-store..."` maps directly to `config.ml.redis_url`.

---

## 5. `requirements-fraud-consumer.txt` and `Dockerfile.fraud-consumer`

- **Requirements Path:** [`requirements-fraud-consumer.txt`](file:///home/syd/Downloads/archive/requirements-fraud-consumer.txt)
- **Dockerfile Path:** [`Dockerfile.fraud-consumer`](file:///home/syd/Downloads/archive/Dockerfile.fraud-consumer)

### Container Build Strategy
1. **Dependency Pinning:**
   Includes `redis>=5.0.0` for Redis cluster and TLS support, `confluent-kafka==2.8.2`, and `lightgbm==4.5.0`.
2. **C++ Runtime Requirement:**
   `libgomp1` is explicitly installed in the Debian runtime stage. LightGBM relies on OpenMP shared libraries for parallel tree inference; omitting `libgomp1` causes runtime `ImportError: libgomp.so.1: cannot open shared object file`.
3. **Least Privilege Execution:**
   The container creates user `appuser` (`UID 10001`) and switches to `USER 10001:10001`, blocking container breakouts.
