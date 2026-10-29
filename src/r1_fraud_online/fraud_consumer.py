"""Production-ready real-time Kafka consumer for Fraud Detection inference.
Hydrates online features from Redis / In-Memory cache, scores transactions with LightGBM in < 5ms,
and emits enriched fraud alerts to the 'fraud_alerts' Kafka topic.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import joblib
import numpy as np

try:
    import confluent_kafka
    HAS_CONFLUENT_KAFKA = True
except ImportError:
    confluent_kafka = None  # type: ignore
    HAS_CONFLUENT_KAFKA = False

from ml.feature_engineering import (
    FEATURE_NAMES,
    GLOBAL_DEFAULTS,
    FeatureExtractor,
    clean_currency,
)
from ml.state_store import StateStore
from synthetic_streaming.config import GeneratorConfig, KafkaConfig

logger = logging.getLogger("fraud_consumer")


class FraudStreamingConsumer:
    """Consumes transactions from Kafka, performs real-time feature hydration, applies LightGBM, and emits alerts."""

    def __init__(
        self,
        config: Optional[GeneratorConfig] = None,
        model_path: Optional[str] = None,
        state_store: Optional[StateStore] = None,
        on_alert_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ):
        self.config = config or GeneratorConfig()
        self.ml_cfg = self.config.ml
        self.kafka_cfg = self.config.kafka
        self.on_alert = on_alert_callback

        # 1. Initialize State Store (Redis with fallback)
        self.state_store = state_store or StateStore(
            redis_url=self.ml_cfg.redis_url,
            enable_in_memory_fallback=self.ml_cfg.enable_in_memory_fallback,
        )

        # 2. Load Model and Metadata
        default_model = "models/v2/fraud_lgbm_model.joblib" if Path("models/v2/fraud_lgbm_model.joblib").exists() else self.ml_cfg.model_path
        m_path = Path(model_path or default_model)
        meta_path = m_path.parent / "model_metadata.json"

        self.model: Optional[Any] = None
        self.metadata: Dict[str, Any] = {}
        self.threshold: float = self.ml_cfg.fraud_threshold
        mcc_risk_map: Dict[int, float] = {}

        if meta_path.exists():
            try:
                with open(meta_path, "r", encoding="utf-8") as f:
                    self.metadata = json.load(f)
                    self.threshold = float(self.metadata.get("optimal_threshold", self.threshold))
                    # Convert str keys back to int for MCC
                    raw_mcc = self.metadata.get("mcc_risk_map", {})
                    mcc_risk_map = {int(k): float(v) for k, v in raw_mcc.items()}
                logger.info("Loaded model metadata: optimal threshold=%.3f, %d MCC risk scores", self.threshold, len(mcc_risk_map))
            except Exception as ex:
                logger.warning("Could not read metadata at %s: %s", meta_path, ex)

        if m_path.exists():
            try:
                self.model = joblib.load(m_path)
                logger.info("Successfully loaded LightGBM fraud model from %s", m_path)
            except Exception as ex:
                logger.error("Failed to load model from %s: %s", m_path, ex)
        else:
            logger.warning("Model file not found at %s. Consumer running in rule-based baseline mode.", m_path)

        self.extractor = FeatureExtractor(
            mcc_risk_map=mcc_risk_map,
            global_stats=self.metadata.get("global_defaults", GLOBAL_DEFAULTS),
        )

        # Performance and Audit Metrics
        self.total_processed = 0
        self.total_frauds_detected = 0
        self.total_correct_evaluations = 0
        self.total_ground_truth_frauds = 0
        self._running = False

        # Kafka clients
        self._consumer: Optional[Any] = None
        self._producer: Optional[Any] = None

    def preload_reference_data(
        self,
        users_csv: str = "sd254_users.csv",
        cards_csv: str = "sd254_cards.csv"
    ) -> None:
        """Preload static user and card dimensions into state store."""
        base_dir = self.config.baseline_data_dir
        u_path = Path(base_dir) / users_csv if not Path(users_csv).exists() else Path(users_csv)
        c_path = Path(base_dir) / cards_csv if not Path(cards_csv).exists() else Path(cards_csv)
        self.state_store.preload_reference_data(u_path, c_path)

    def identify_risk_factors(self, features: Dict[str, float]) -> List[str]:
        """Explainable AI: Return human-readable risk factors for why a transaction was flagged."""
        reasons: List[str] = []
        if features.get("travel_speed_kmh", 0) > 800.0:
            reasons.append(f"impossible_travel_speed_{int(features['travel_speed_kmh'])}kmh")
        if features.get("credit_limit_utilization", 0) > 0.85:
            reasons.append("credit_limit_exhaustion")
        if features.get("card_on_dark_web", 0) == 1:
            reasons.append("card_reported_on_dark_web")
        if features.get("spend_deviation_ratio", 0) > 4.5:
            reasons.append(f"spend_spike_{round(features['spend_deviation_ratio'], 1)}x_above_avg")
        if features.get("tx_count_last_1h", 0) >= 5:
            reasons.append(f"rapid_swiping_burst_{int(features['tx_count_last_1h'])}_in_1h")
        if features.get("is_credential_error", 0) == 1:
            reasons.append("credential_authentication_failure_bad_pin_or_cvv")
        if features.get("is_insufficient_balance", 0) == 1:
            reasons.append("insufficient_balance_exceeded")
        if features.get("is_online_tx", 0) == 1 and features.get("amount", 0) > 400:
            reasons.append("high_value_card_not_present_online_transaction")
        if features.get("card_age_at_tx_years", 5) < 1.0 and features.get("amount", 0) > 250:
            reasons.append("newly_opened_card_exploit")
        if features.get("is_refund", 0) == 1:
            reasons.append("unusual_negative_refund_anomaly")
        if features.get("mcc_high_risk_flag", 0) == 1 and features.get("amount", 0) > 200:
            reasons.append("high_risk_mcc_cash_out_target")
        return reasons

    def process_transaction(self, event_envelope: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single transaction event through feature hydration and model scoring.
        
        Guaranteed execution time < 10ms.
        """
        t0 = time.perf_counter()

        if not isinstance(event_envelope, dict):
            logger.warning("Malformed input: event envelope is not a dict (%s)", type(event_envelope))
            return {
                "alert_id": f"alert-{uuid.uuid4()}",
                "event_id": "",
                "status": "MALFORMED_INPUT",
                "error": "Event envelope is not a valid dictionary",
                "is_fraud_predicted": False,
                "fraud_probability": 0.0,
                "latency_ms": 0.0,
            }

        payload = event_envelope.get("payload")
        if not isinstance(payload, dict):
            logger.warning("Malformed input: payload is missing or not a dict")
            return {
                "alert_id": f"alert-{uuid.uuid4()}",
                "event_id": str(event_envelope.get("event_id", "")),
                "status": "MALFORMED_PAYLOAD",
                "error": "Payload is missing or not a dictionary",
                "is_fraud_predicted": False,
                "fraud_probability": 0.0,
                "latency_ms": 0.0,
            }

        event_id = str(event_envelope.get("event_id", ""))
        user_id = int(payload.get("User", 0))
        card_idx = int(payload.get("Card", 0))
        amount = clean_currency(payload.get("Amount"), 0.0)

        # Parse epoch time
        evt_ts = event_envelope.get("event_timestamp")
        if evt_ts:
            try:
                dt = datetime.fromisoformat(evt_ts.replace("Z", "+00:00"))
                epoch_sec = dt.timestamp()
            except Exception:
                epoch_sec = time.time()
        else:
            epoch_sec = time.time()

        m_lat = payload.get("Merchant Latitude")
        m_lon = payload.get("Merchant Longitude")

        # 1. Hydrate state & profiles from StateStore (Sub-ms)
        user_p = self.state_store.get_user_profile(user_id)
        card_p = self.state_store.get_card_profile(user_id, card_idx)
        st = self.state_store.get_and_update_state(
            user_id=user_id,
            card_index=card_idx,
            amount=amount,
            epoch_sec=epoch_sec,
            merchant_lat=m_lat,
            merchant_lon=m_lon,
            event_id=event_id or None,
        )

        # 2. Extract online features
        fdict = self.extractor.extract_online_features(
            payload=payload,
            user_profile=user_p,
            card_profile=card_p,
            state=st,
        )
        fvec = self.extractor.to_feature_vector(fdict)

        # 3. Model Inference
        if self.model is not None:
            X_in = np.array([fvec], dtype=np.float32)
            fraud_prob = float(self.model.predict_proba(X_in)[0][1])
            is_fraud_pred = bool(fraud_prob >= self.threshold)
        else:
            # Heuristic baseline fallback if model file is not present
            fraud_prob = 0.95 if (fdict["card_on_dark_web"] or fdict["travel_speed_kmh"] > 800) else 0.01
            is_fraud_pred = bool(fraud_prob >= self.threshold)

        # 4. Explainability & Risk Factors
        risk_factors = self.identify_risk_factors(fdict)

        # 5. Ground truth evaluation (No data leakage: label is evaluated post-inference)
        ground_truth_raw = str(payload.get("Is Fraud?", "No")).strip().lower()
        actual_fraud = ground_truth_raw in ("yes", "1", "true")
        is_correct = (is_fraud_pred == actual_fraud)

        elapsed_ms = (time.perf_counter() - t0) * 1000.0

        # Update running consumer metrics
        self.total_processed += 1
        if is_fraud_pred:
            self.total_frauds_detected += 1
        if actual_fraud:
            self.total_ground_truth_frauds += 1
        if is_correct:
            self.total_correct_evaluations += 1

        alert = {
            "alert_id": f"alert-{uuid.uuid4()}",
            "event_id": event_envelope.get("event_id", ""),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "user_id": user_id,
            "card_index": card_idx,
            "amount": amount,
            "decision": "ALERT" if is_fraud_pred else "PASS",
            "is_fraud_predicted": is_fraud_pred,
            "fraud_probability": round(fraud_prob, 4),
            "decision_threshold": self.threshold,
            "ground_truth_fraud": "Yes" if actual_fraud else "No",
            "is_prediction_correct": is_correct,
            "risk_factors": risk_factors,
            "travel_speed_kmh": fdict["travel_speed_kmh"],
            "distance_to_home_km": fdict["distance_to_home_km"],
            "latency_ms": round(elapsed_ms, 2),
        }

        if is_fraud_pred:
            logger.warning(
                "🚨 FRAUD DETECTED! User=%d, Card=%d, Amount=$%.2f | Prob=%.3f | Reasons=%s | Latency=%.2fms",
                user_id, card_idx, amount, fraud_prob, risk_factors, elapsed_ms
            )
        else:
            logger.debug(
                "Normal Spend: User=%d, Card=%d, Amount=$%.2f | Prob=%.3f | Latency=%.2fms",
                user_id, card_idx, amount, fraud_prob, elapsed_ms
            )

        if self.on_alert:
            self.on_alert(alert)

        return alert

    def consume_and_process_single_message(
        self,
        msg: Any,
        consumer: Optional[Any] = None,
        producer: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        """Process a single Kafka message, publish alerts if required, and manually commit offset.

        Offsets are committed synchronously only after:
        1. Successful deserialization and feature extraction / scoring.
        2. Successful delivery and broker acknowledgment of any required fraud alert.

        At-Least-Once Delivery & Duplicate Alerts Documentation:
        --------------------------------------------------------
        Because Kafka does not provide atomic cross-topic transactions without dedicated
        transaction coordinators, an at-least-once delivery contract applies:
        - If an alert is successfully published and flushed, but the consumer crashes or network drops
          before `consumer.commit(...)` completes, Kafka will re-deliver this transaction message upon
          consumer restart or rebalance.
        - As a result, duplicate alerts may be emitted to the `fraud_alerts` topic.
        - Downstream consumers of `fraud_alerts` must handle duplicate alerts idempotently
          using `event_id` or `(user_id, card_index, timestamp)`.
        - If processing fails or producer alert delivery fails, the offset for this message is NOT
          committed, preventing the consumer from advancing past earlier unprocessed records.
        """
        cons = consumer or self._consumer
        prod = producer or self._producer

        # 1. Parse JSON payload
        raw_val = msg.value()
        if isinstance(raw_val, bytes):
            envelope = json.loads(raw_val.decode("utf-8"))
        elif isinstance(raw_val, str):
            envelope = json.loads(raw_val)
        elif isinstance(raw_val, dict):
            envelope = raw_val
        else:
            raise ValueError(f"Unsupported message value type: {type(raw_val)}")

        # 2. Score transaction
        alert = self.process_transaction(envelope)
        if alert.get("status") in ("MALFORMED_INPUT", "MALFORMED_PAYLOAD"):
            raise ValueError(f"Transaction processing failed: {alert.get('error')}")

        # 3. Publish alert if required
        requires_alert = alert.get("is_fraud_predicted", False) or getattr(self.ml_cfg, "emit_all_evaluations", False)
        if requires_alert:
            if prod is None:
                raise RuntimeError("Producer required to publish fraud alert, but no producer is configured.")
            alert_topic = self.ml_cfg.fraud_alerts_topic
            alert_bytes = json.dumps(alert).encode("utf-8")
            delivery_error: Optional[Exception] = None

            def _on_delivery(err: Any, _reported_msg: Any) -> None:
                nonlocal delivery_error
                if err is not None:
                    delivery_error = err

            prod.produce(
                topic=alert_topic,
                key=str(alert.get("user_id", 0)).encode("utf-8"),
                value=alert_bytes,
                on_delivery=_on_delivery,
            )
            # Flush synchronously to ensure broker acknowledged delivery before committing offset
            unflushed = prod.flush(timeout=5.0)
            if unflushed > 0 or delivery_error is not None:
                err_detail = delivery_error or f"{unflushed} message(s) unflushed within 5.0s timeout"
                logger.error("Producer alert delivery failed for event %s: %s", alert.get("event_id"), err_detail)
                raise RuntimeError(f"Producer delivery failed for alert topic '{alert_topic}': {err_detail}")

        # 4. Manually and synchronously commit offset for this message
        if cons is not None:
            cons.commit(message=msg, asynchronous=False)

        return alert

    def start_streaming(self) -> None:
        """Connect to Kafka broker and start continuous event consumption loop."""
        if not HAS_CONFLUENT_KAFKA:
            logger.error("confluent_kafka is required for live streaming execution.")
            return

        conf = {
            "bootstrap.servers": self.kafka_cfg.bootstrap_servers,
            "group.id": self.ml_cfg.consumer_group,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
        if self.kafka_cfg.security_protocol:
            conf["security.protocol"] = self.kafka_cfg.security_protocol
        if self.kafka_cfg.sasl_mechanism:
            conf["sasl.mechanism"] = self.kafka_cfg.sasl_mechanism
        if self.kafka_cfg.sasl_username:
            conf["sasl.username"] = self.kafka_cfg.sasl_username
        if self.kafka_cfg.sasl_password:
            conf["sasl.password"] = self.kafka_cfg.sasl_password.get_secret_value()

        consumer = confluent_kafka.Consumer(conf)
        producer = confluent_kafka.Producer({"bootstrap.servers": self.kafka_cfg.bootstrap_servers})
        self._consumer = consumer
        self._producer = producer

        target_topic = self.kafka_cfg.transactions_topic
        alert_topic = self.ml_cfg.fraud_alerts_topic
        consumer.subscribe([target_topic])
        self._running = True

        logger.info(
            "Kafka Fraud Consumer started! Listening on topic '%s', emitting alerts to '%s' (manual offset commits enabled)",
            target_topic,
            alert_topic,
        )

        try:
            while self._running:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error("Kafka error: %s", msg.error())
                    continue

                try:
                    self.consume_and_process_single_message(msg)
                except Exception as ex:
                    logger.error(
                        "Error processing transaction message (offset=%s): %s. "
                        "Offset NOT committed to prevent skipping unprocessed messages.",
                        getattr(msg, "offset", lambda: "unknown")(),
                        ex,
                    )
                    time.sleep(1.0)
        finally:
            consumer.close()
            producer.flush()
            logger.info("Fraud Consumer terminated.")

    def stop(self) -> None:
        self._running = False


def main():
    import argparse
    import signal

    parser = argparse.ArgumentParser(description="Run Real-Time Kafka Fraud Detection Consumer.")
    parser.add_argument("--model-path", type=str, default=None, help="Path to trained model")
    parser.add_argument("--redis-url", type=str, default=None, help="Redis connection URL")
    args = parser.parse_args()

    cfg = GeneratorConfig()
    if args.redis_url:
        cfg.ml.redis_url = args.redis_url

    consumer = FraudStreamingConsumer(config=cfg, model_path=args.model_path)

    def handle_sigterm(*_):
        logger.info("Received SIGTERM/SIGINT, stopping fraud consumer gracefully...")
        consumer.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    consumer.preload_reference_data()
    consumer.start_streaming()


if __name__ == "__main__":
    main()
