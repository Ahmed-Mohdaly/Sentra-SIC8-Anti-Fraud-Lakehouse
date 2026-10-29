"""Kafka publisher adapter and mock publisher with bounded backpressure and delivery callbacks."""

from __future__ import annotations

import collections
import logging
import threading
import time
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional
try:
    import confluent_kafka
    HAS_CONFLUENT_KAFKA = True
except ImportError:
    confluent_kafka = None  # type: ignore[assignment]
    HAS_CONFLUENT_KAFKA = False

from synthetic_streaming.config import KafkaConfig

logger = logging.getLogger(__name__)


class EventPublisher(ABC):
    """Abstract interface for streaming event publication."""

    @abstractmethod
    def publish(
        self,
        topic: str,
        key: str,
        value: bytes,
        headers: Optional[Dict[str, str]] = None,
        on_delivery: Optional[Callable[[Optional[Exception], Any], None]] = None
    ) -> None:
        """Asynchronously enqueue an event for publication."""
        pass

    @abstractmethod
    def poll(self, timeout: float = 0.0) -> int:
        """Serve delivery callbacks and dispatch events."""
        pass

    @abstractmethod
    def flush(self, timeout: float = 10.0) -> int:
        """Wait for all outstanding messages to be delivered. Returns remaining count."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Cleanly terminate the publisher."""
        pass

    @abstractmethod
    def health(self) -> Dict[str, Any]:
        """Report connectivity and operational health without blocking probes."""
        pass

    def check_connectivity(self, timeout: float = 2.0) -> bool:
        """Explicit broker metadata probe."""
        return True

    @abstractmethod
    def get_queue_length(self) -> int:
        """Return the current number of buffered in-flight messages."""
        pass

    @abstractmethod
    def is_buffer_full(self) -> bool:
        """Return True if local buffer capacity is reached."""
        pass


class KafkaEventPublisher(EventPublisher):
    """Production Kafka publisher adapter using confluent-kafka supporting SASL/TLS and Azure Event Hubs."""

    def __init__(self, config: KafkaConfig):
        self.config = config
        self._lock = threading.Lock()
        self._is_closed = False
        self._last_error: Optional[str] = None
        self._total_published = 0
        self._total_delivered = 0
        self._total_failed = 0
        self._last_error_log_time = 0.0

        # Build librdkafka configuration dictionary
        conf: Dict[str, Any] = {
            "bootstrap.servers": config.bootstrap_servers,
            "security.protocol": config.security_protocol,
            "acks": config.acks,
            "enable.idempotence": config.enable_idempotence,
            "max.in.flight.requests.per.connection": config.max_in_flight,
            "retries": config.retries,
            "retry.backoff.ms": config.retry_backoff_ms,
            "delivery.timeout.ms": config.delivery_timeout_ms,
            "queue.buffering.max.messages": config.queue_buffering_max_messages,
            "queue.buffering.max.kbytes": config.queue_buffering_max_kbytes,
            "batch.num.messages": config.batch_num_messages,
            "linger.ms": config.linger_ms,
        }

        # Apply SASL / TLS authentication if configured (e.g. Azure Event Hubs)
        if config.sasl_mechanism:
            conf["sasl.mechanism"] = config.sasl_mechanism
        if config.sasl_username:
            conf["sasl.username"] = config.sasl_username
        if config.sasl_password:
            conf["sasl.password"] = config.sasl_password.get_secret_value()
        if config.ssl_ca_location:
            conf["ssl.ca.location"] = config.ssl_ca_location

        if not HAS_CONFLUENT_KAFKA or confluent_kafka is None:
            self._producer = None
            self._initialized = False
            self._last_error = (
                "confluent-kafka is not installed. To use KafkaEventPublisher, "
                "install the optional package: pip install 'synthetic-streaming[kafka]'"
            )
            logger.error(self._last_error)
            raise RuntimeError(self._last_error)

        try:
            self._producer = confluent_kafka.Producer(conf)
            self._initialized = True
            logger.info("Kafka producer initialized successfully.")
        except Exception as e:
            logger.error("Failed to instantiate confluent_kafka.Producer.")
            self._producer = None
            self._initialized = False
            self._last_error = str(e)
            raise

    def publish(
        self,
        topic: str,
        key: str,
        value: bytes,
        headers: Optional[Dict[str, str]] = None,
        on_delivery: Optional[Callable[[Optional[Exception], Any], None]] = None
    ) -> None:
        if self._is_closed or self._producer is None:
            raise RuntimeError("Cannot publish to closed or uninitialized Kafka publisher")

        # Check queue backpressure before enqueueing
        queue_len = len(self._producer)
        max_messages = self.config.queue_buffering_max_messages

        # Apply proactive drain if buffer exceeds 90% capacity
        if queue_len >= max_messages * 0.90:
            self._producer.poll(0.05)

        with self._lock:
            self._total_published += 1

        def _delivery_callback(err: Any, msg: Any) -> None:
            if err is not None:
                err_str = str(err)
                with self._lock:
                    self._total_failed += 1
                    self._last_error = err_str

                # Throttle error logging to avoid flooding stdout during extended broker outages
                now = time.monotonic()
                if now - self._last_error_log_time >= 5.0:
                    self._last_error_log_time = now
                    logger.error(f"Kafka delivery failed for topic {topic}: {err_str}")

                if on_delivery:
                    on_delivery(Exception(err_str), msg)
            else:
                with self._lock:
                    self._total_delivered += 1
                if on_delivery:
                    on_delivery(None, msg)

        # Encode headers if provided
        header_list = [(k, v.encode("utf-8")) for k, v in headers.items()] if headers else None

        retries = 3
        while retries > 0:
            try:
                self._producer.produce(
                    topic=topic,
                    key=key.encode("utf-8") if isinstance(key, str) else key,
                    value=value,
                    headers=header_list,
                    on_delivery=_delivery_callback,
                )
                break
            except BufferError:
                retries -= 1
                self._producer.poll(0.1)
                if retries == 0:
                    with self._lock:
                        self._total_failed += 1
                        self._last_error = "Local queue BufferError full"
                    if on_delivery:
                        try:
                            on_delivery(BufferError("Local queue BufferError full"), None)
                        except Exception:
                            pass
                    raise

    def poll(self, timeout: float = 0.0) -> int:
        if self._producer and not self._is_closed:
            return self._producer.poll(timeout)
        return 0

    def flush(self, timeout: float = 10.0) -> int:
        if self._producer:
            logger.info(f"Flushing Kafka producer (timeout={timeout}s)...")
            remaining = self._producer.flush(timeout)
            if remaining > 0:
                logger.warning(f"Kafka flush completed with {remaining} unacknowledged messages remaining.")
            return remaining
        return 0

    def check_connectivity(self, timeout: float = 2.0) -> bool:
        """Explicit broker metadata probe (for diagnostics/integration tests, not for regular health probes)."""
        if not self._producer or self._is_closed:
            return False
        try:
            meta = self._producer.list_topics(timeout=timeout)
            return len(meta.brokers) > 0 if meta and meta.brokers else False
        except Exception as e:
            self._last_error = str(e)
            return False

    def close(self) -> None:
        with self._lock:
            if not self._is_closed:
                if self._producer:
                    self.flush(self.config.shutdown_flush_timeout_sec)
                self._is_closed = True
                logger.info("Kafka publisher closed.")

    def health(self) -> Dict[str, Any]:
        """Non-blocking health probe check."""
        if not self._initialized or self._producer is None or self._is_closed:
            return {"status": "unhealthy", "error": self._last_error or "Producer not initialized or closed"}

        buffered = len(self._producer)
        is_degraded = buffered >= (self.config.queue_buffering_max_messages * 0.95)
        return {
            "status": "degraded" if is_degraded else "healthy",
            "buffered_messages": buffered,
            "total_published": self._total_published,
            "total_delivered": self._total_delivered,
            "total_failed": self._total_failed,
            "last_error": self._last_error,
        }

    def get_queue_length(self) -> int:
        return len(self._producer) if self._producer else 0

    def is_buffer_full(self) -> bool:
        if not self._producer:
            return False
        return len(self._producer) >= self.config.queue_buffering_max_messages


class MockEventPublisher(EventPublisher):
    """In-memory event publisher for hermetic unit testing and simulated environments."""

    def __init__(self, max_buffer: int = 50000):
        self.max_buffer = max_buffer
        self._published_history: collections.deque = collections.deque(maxlen=10000)
        self._lock = threading.Lock()
        self._is_closed = False
        self.simulate_failure = False
        self.simulate_buffer_full = False
        self._total_published = 0
        self._total_delivered = 0
        self._total_failed = 0
        self._last_error: Optional[str] = None

    @property
    def published_messages(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._published_history)

    def publish(
        self,
        topic: str,
        key: str,
        value: bytes,
        headers: Optional[Dict[str, str]] = None,
        on_delivery: Optional[Callable[[Optional[Exception], Any], None]] = None
    ) -> None:
        with self._lock:
            if self._is_closed:
                raise RuntimeError("Cannot publish to closed MockEventPublisher")
            if self.simulate_buffer_full or len(self._published_history) >= self.max_buffer:
                raise BufferError("Simulated buffer full")

            self._total_published += 1

            record = {
                "topic": topic,
                "key": key,
                "value": value,
                "headers": headers,
                "timestamp": time.monotonic(),
            }
            self._published_history.append(record)

            if self.simulate_failure:
                self._total_failed += 1
                self._last_error = "Simulated delivery failure"
                if on_delivery:
                    on_delivery(Exception("Simulated delivery failure"), None)
            else:
                self._total_delivered += 1
                if on_delivery:
                    on_delivery(None, record)

    def poll(self, timeout: float = 0.0) -> int:
        return 0

    def flush(self, timeout: float = 10.0) -> int:
        return 0

    def close(self) -> None:
        with self._lock:
            self._is_closed = True

    def health(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "status": "healthy" if not self.simulate_failure and not self._is_closed else "unhealthy",
                "mock": True,
                "buffered_messages": len(self._published_history),
                "total_published": self._total_published,
                "total_delivered": self._total_delivered,
                "total_failed": self._total_failed,
                "last_error": self._last_error,
            }

    def get_queue_length(self) -> int:
        with self._lock:
            return len(self._published_history)

    def is_buffer_full(self) -> bool:
        with self._lock:
            return self.simulate_buffer_full or len(self._published_history) >= self.max_buffer
