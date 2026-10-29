"""Generator service coordinating lifecycle, streaming loops, and thread safety."""

from __future__ import annotations

import logging
import os
import random
import threading
import time
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session

from synthetic_streaming.baseline_loader import BaselineLoader
from synthetic_streaming.cards_generator import CardsGenerator
from synthetic_streaming.config import GeneratorConfig
from synthetic_streaming.database import (
    CardModel,
    CityReferenceModel,
    MerchantCategoryCodeModel,
    TransactionModel,
    UserModel,
    check_db_health,
    create_db_engine,
    init_db,
    persist_prerequisite_entities,
    persist_transaction,
    seed_reference_data,
)
from synthetic_streaming.event_schema import EventEnvelope
from synthetic_streaming.kafka_publisher import (
    EventPublisher,
    KafkaEventPublisher,
    MockEventPublisher,
)
from synthetic_streaming.rate_controller import RateController
from synthetic_streaming.relationship_validator import RelationshipValidator
from synthetic_streaming.scenario_engine import ScenarioEngine
from synthetic_streaming.source_simulator import SourceSimulator
from synthetic_streaming.transactions_generator import TransactionsGenerator
from synthetic_streaming.users_generator import UsersGenerator

logger = logging.getLogger(__name__)


class ServiceState(str, Enum):
    STOPPED = "STOPPED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"


class GeneratorService:
    """Core service managing transaction generation loop, database persistence, and optional stream publishing."""

    def __init__(
        self,
        config: GeneratorConfig,
        publisher: Optional[EventPublisher] = None,
        db_engine: Optional[Engine] = None,
    ):
        self.config = config
        self._lock = threading.Lock()
        self._state = ServiceState.STOPPED
        self._worker_thread: Optional[threading.Thread] = None
        self._start_time: Optional[float] = None
        self._stop_event = threading.Event()
        self._run_id = f"run-{config.worker_id}-{int(time.time())}"
        self._last_loop_error_time = 0.0

        # Initialize Relational Database Persistence
        self.db_engine = db_engine or create_db_engine(config.database_url)
        init_db(self.db_engine)
        if config.auto_seed:
            try:
                seed_reference_data(self.db_engine, data_dir=config.baseline_data_dir)
            except Exception as e:
                logger.warning(f"Auto-seeding reference data skipped or failed: {e}")
        self.db_session_factory = sessionmaker(bind=self.db_engine, expire_on_commit=False)

        # Initialize PRNG with deterministic seed if configured
        effective_seed = (config.random_seed + config.worker_id) if config.random_seed is not None else None
        self._rng = random.Random(effective_seed) if effective_seed is not None else random.Random()

        # Initialize Rate Controller
        self.rate_controller = RateController(target_tps=config.target_tps)

        # Initialize Publisher (Decoupled: defaults to in-memory publisher unless Kafka is explicitly enabled)
        if publisher is not None:
            self.publisher = publisher
        else:
            kafka_enabled = os.environ.get("ENABLE_KAFKA", "").lower() in ("true", "1")
            if kafka_enabled:
                try:
                    self.publisher = KafkaEventPublisher(config=config.kafka)
                except Exception as e:
                    logger.warning(f"Failed to connect to Kafka ({e}). Using in-memory publisher.")
                    self.publisher = MockEventPublisher()
            else:
                self.publisher = MockEventPublisher()

        # Baseline Loader
        self.baseline = BaselineLoader(data_dir=config.baseline_data_dir)
        self.baseline.load()


        # Users and Cards Generator: Shard high-water mark across workers to prevent ID collision
        initial_hwm = self.baseline.high_water_mark + (config.worker_id * 1_000_000)
        self.users_gen = UsersGenerator(
            initial_high_water_mark=initial_hwm,
            db_engine=self.db_engine,
            worker_id=config.worker_id,
            rng=self._rng,
        )
        self.cards_gen = CardsGenerator(rng=self._rng)
        self.source_sim = SourceSimulator(weights=config.source_weights, rng=self._rng)
        self.scenario_engine = ScenarioEngine(
            weights=config.scenario_weights,
            inject_invalid_events=config.inject_invalid_events,
            invalid_event_rate=config.invalid_event_rate,
            rng=self._rng,
        )
        self.validator = RelationshipValidator()

        # Transaction Generator
        self.tx_gen = TransactionsGenerator(
            baseline_loader=self.baseline,
            users_generator=self.users_gen,
            cards_generator=self.cards_gen,
            source_simulator=self.source_sim,
            scenario_engine=self.scenario_engine,
            relationship_validator=self.validator,
            new_user_ratio=config.new_user_transaction_ratio,
            rng=self._rng,
        )

        # Event counts
        self.total_tx_generated = 0
        self.total_prereq_events_generated = 0
        self.total_tx_delivered = 0
        self.total_tx_failed = 0

    @property
    def state(self) -> ServiceState:
        with self._lock:
            return self._state

    @property
    def is_running(self) -> bool:
        return self.state == ServiceState.RUNNING

    def start(self) -> bool:
        """Start the continuous transaction generation loop in a background thread."""
        with self._lock:
            if self._state in (ServiceState.RUNNING, ServiceState.STARTING):
                logger.warning("GeneratorService is already running.")
                return False
            self._state = ServiceState.STARTING

        try:
            pub_health = self.publisher.health()
            if pub_health.get("status") == "unhealthy":
                logger.error(f"Cannot start generator: publisher is unhealthy: {pub_health}")
                with self._lock:
                    self._state = ServiceState.STOPPED
                return False

            self._stop_event.clear()
            self._start_time = time.time()
            self.rate_controller.start()

            self._worker_thread = threading.Thread(
                target=self._generation_loop,
                name="GeneratorWorkerThread",
                daemon=True,
            )
            self._worker_thread.start()

            with self._lock:
                self._state = ServiceState.RUNNING

            logger.info(f"GeneratorService started [worker_id={self.config.worker_id}] target_tps={self.config.target_tps}")
            return True
        except Exception as e:
            logger.error(f"Failed to start generator: {e}")
            with self._lock:
                self._state = ServiceState.STOPPED
            raise

    def stop(self, timeout: float = 10.0) -> None:
        """Gracefully stop the generation loop and flush publisher buffers."""
        with self._lock:
            if self._state not in (ServiceState.RUNNING, ServiceState.STARTING):
                return
            self._state = ServiceState.STOPPING

        logger.info("Stopping GeneratorService...")
        self._stop_event.set()
        self.rate_controller.stop()

        if self._worker_thread and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=timeout)

        # Flush Kafka publisher
        unresolved = self.publisher.flush(timeout=timeout)
        if unresolved > 0:
            logger.warning(f"GeneratorService shutdown completed with {unresolved} unacknowledged messages remaining.")

        with self._lock:
            self._state = ServiceState.STOPPED

        logger.info("GeneratorService stopped cleanly.")

    def update_runtime_config(
        self,
        target_tps: Optional[float] = None,
        new_user_ratio: Optional[float] = None
    ) -> Dict[str, Any]:
        """Update runtime configurable generator parameters."""
        with self._lock:
            if target_tps is not None and target_tps > 0:
                self.config.target_tps = target_tps
                self.rate_controller.set_target_tps(target_tps)
            if new_user_ratio is not None and 0.0 <= new_user_ratio <= 1.0:
                self.config.new_user_transaction_ratio = new_user_ratio
                self.tx_gen.new_user_ratio = new_user_ratio

        return {
            "target_tps": self.config.target_tps,
            "new_user_transaction_ratio": self.config.new_user_transaction_ratio,
        }

    def _generation_loop(self) -> None:
        """Main streaming worker loop executing paced event emissions."""
        while not self._stop_event.is_set():
            # Rate limiting / pacing
            if not self.rate_controller.wait_for_slot():
                break

            # Check limits: max_events or run_duration_seconds
            if self.config.max_events and self.total_tx_generated >= self.config.max_events:
                logger.info(f"Reached max_events limit ({self.config.max_events}). Stopping loop.")
                break

            if self.config.run_duration_seconds and self._start_time:
                if (time.time() - self._start_time) >= self.config.run_duration_seconds:
                    logger.info("Reached run_duration_seconds limit. Stopping loop.")
                    break

            try:
                # Generate transaction and prerequisite events
                tx_envelope, prereq_envelopes = self.tx_gen.generate_next_transaction(run_id=self._run_id)

                # Persist prerequisite entities (users and cards) to PostgreSQL for Debezium CDC
                if prereq_envelopes:
                    session = self.db_session_factory()
                    try:
                        persist_prerequisite_entities(session, prereq_envelopes, commit=True)
                    finally:
                        session.close()
                    self.total_prereq_events_generated += len(prereq_envelopes)

                # Publish transaction event to Amazon MSK streaming topic
                tx_user_id = tx_envelope.payload.get("User", 0)
                tx_key = str(tx_user_id)
                tx_id = tx_envelope.payload.get("transaction_id", tx_envelope.event_id)

                def _on_stream_delivery(err: Optional[Exception], msg: Any) -> None:
                    if err is not None:
                        with self._lock:
                            self.total_tx_failed += 1
                        logger.error(f"Stream delivery failed for transaction_id={tx_id}: {err}")
                    else:
                        with self._lock:
                            self.total_tx_delivered += 1

                self.publisher.publish(
                    topic=self.config.kafka.transactions_topic,
                    key=tx_key,
                    value=tx_envelope.to_bytes(),
                    on_delivery=_on_stream_delivery,
                )
                self.total_tx_generated += 1

                # Serve asynchronous delivery callbacks
                self.publisher.poll(timeout=0.0)

            except Exception as e:
                now = time.monotonic()
                if now - self._last_loop_error_time >= 5.0:
                    self._last_loop_error_time = now
                    logger.error(f"Error in generator loop: {e}")
                time.sleep(0.05)

        with self._lock:
            if self._state == ServiceState.RUNNING:
                self._state = ServiceState.STOPPED

    def generate_batch(self, count: int = 50, run_id: Optional[str] = None) -> List[TransactionModel]:
        """Synchronously generate a batch of transactions, publish to MSK, and persist prerequisites in DB."""
        effective_run_id = run_id or f"batch-{int(time.time())}"
        records: List[TransactionModel] = []
        tx_to_publish: List[Tuple[str, bytes]] = []
        new_prereq_count = 0
        batch_delivery_errors: List[str] = []

        def _on_batch_delivery(err: Optional[Exception], msg: Any) -> None:
            if err is not None:
                batch_delivery_errors.append(str(err))

        session = self.db_session_factory()
        try:
            with session.begin():
                for _ in range(count):
                    tx_envelope, prereq_envelopes = self.tx_gen.generate_next_transaction(run_id=effective_run_id)
                    if prereq_envelopes:
                        persist_prerequisite_entities(session, prereq_envelopes, commit=False)
                        new_prereq_count += len(prereq_envelopes)

                    # In-memory transaction representation (not added to DB)
                    rec = persist_transaction(session, tx_envelope, prereq_envelopes=None, commit=False)
                    records.append(rec)

                    tx_user_id = tx_envelope.payload.get("User", 0)
                    tx_to_publish.append((str(tx_user_id), tx_envelope.to_bytes()))

            # After atomic DB commit succeeds, publish transactions to MSK streaming topic
            for tx_key, tx_bytes in tx_to_publish:
                self.publisher.publish(
                    topic=self.config.kafka.transactions_topic,
                    key=tx_key,
                    value=tx_bytes,
                    on_delivery=_on_batch_delivery,
                )

            # Wait for all outstanding batch messages to be acknowledged by broker
            unresolved = self.publisher.flush(timeout=10.0)
            if unresolved > 0 or batch_delivery_errors:
                err_msg = (
                    f"Batch transaction delivery failed: {unresolved} unacknowledged messages, "
                    f"{len(batch_delivery_errors)} errors: {batch_delivery_errors[:3]}"
                )
                logger.error(err_msg)
                with self._lock:
                    self.total_tx_failed += (unresolved + len(batch_delivery_errors))
                raise RuntimeError(err_msg)

            with self._lock:
                self.total_tx_delivered += len(records)
            self.total_tx_generated += len(records)
            self.total_prereq_events_generated += new_prereq_count
            return records
        finally:
            session.close()

    def get_status(self) -> Dict[str, Any]:
        """Return operational status, throughput snapshot, and persistent database counts."""
        uptime = (time.time() - self._start_time) if self._start_time and self._state == ServiceState.RUNNING else 0.0
        total_tx = self.tx_gen.existing_user_tx_count + self.tx_gen.new_user_tx_count
        new_ratio_observed = (self.tx_gen.new_user_tx_count / total_tx) if total_tx > 0 else 0.0
        pub_health = self.publisher.health()
        db_health = check_db_health(self.db_engine)

        db_counts = {"transactions": 0, "users": 0, "cards": 0, "mcc": 0, "city_reference": 0}
        try:
            session = self.db_session_factory()
            try:
                db_counts["transactions"] = session.query(TransactionModel).count()
                db_counts["users"] = session.query(UserModel).count()
                db_counts["cards"] = session.query(CardModel).count()
                db_counts["mcc"] = session.query(MerchantCategoryCodeModel).count()
                db_counts["city_reference"] = session.query(CityReferenceModel).count()
            finally:
                session.close()
        except Exception as e:
            logger.warning(f"Unable to query database counts: {e}")

        return {
            "worker_id": self.config.worker_id,
            "total_workers": self.config.total_workers,
            "state": self._state.value,
            "target_tps": self.config.target_tps,
            "actual_generated_tps": round(self.rate_controller.actual_tps, 2),
            "uptime_seconds": round(uptime, 2),
            "total_transactions_generated": self.total_tx_generated,
            "total_transactions_delivered": self.total_tx_delivered,
            "total_transactions_failed": self.total_tx_failed,
            "total_prerequisite_events_generated": self.total_prereq_events_generated,
            "existing_user_transactions": self.tx_gen.existing_user_tx_count,
            "new_user_transactions": self.tx_gen.new_user_tx_count,
            "observed_new_user_ratio": round(new_ratio_observed, 4),
            "publisher_health": pub_health,
            "database_health": db_health,
            "database_counts": db_counts,
            "in_flight_messages": self.publisher.get_queue_length(),
            "failed_deliveries": pub_health.get("total_failed", 0),
            "last_error": pub_health.get("last_error"),
        }

