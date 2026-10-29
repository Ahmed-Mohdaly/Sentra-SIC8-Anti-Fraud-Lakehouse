"""Worker service process with internal control and health endpoints."""

from __future__ import annotations

from contextlib import asynccontextmanager
import logging
import signal
import threading
from typing import Any, Dict, Optional
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
import uvicorn
from synthetic_streaming.config import GeneratorConfig
from synthetic_streaming.generator_service import GeneratorService

logger = logging.getLogger(__name__)


class UpdateWorkerConfigRequest(BaseModel):
    target_tps: Optional[float] = Field(None, gt=0)
    new_user_transaction_ratio: Optional[float] = Field(None, ge=0.0, le=1.0)


def create_worker_internal_app(service: GeneratorService) -> FastAPI:
    """Internal HTTP application for worker health, status, and remote control."""

    @asynccontextmanager
    async def worker_lifespan(app: FastAPI):
        yield
        try:
            service.stop(timeout=5.0)
        except Exception as e:
            logger.warning(f"Error stopping GeneratorService during worker shutdown: {e}")

    app = FastAPI(
        title="Generator Worker Internal Endpoint",
        description="Internal health, status, and control interface for containerized worker.",
        version="0.1.0",
        lifespan=worker_lifespan,
    )

    @app.get("/health")
    def health():
        """Liveness probe."""
        return {"status": "ok", "role": "worker", "worker_id": service.config.worker_id}

    @app.get("/ready")
    def readiness():
        """Readiness probe checking publisher initialization."""
        pub_health = service.publisher.health()
        if pub_health.get("status") == "unhealthy":
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Publisher not ready: {pub_health}",
            )
        return {"status": "ready", "worker_id": service.config.worker_id, "publisher": pub_health}

    @app.get("/status")
    def get_status() -> Dict[str, Any]:
        """Current execution status and throughput counters."""
        return service.get_status()

    @app.get("/control/config")
    def get_config() -> Dict[str, Any]:
        """Safe view of current worker configuration."""
        return service.config.to_safe_dict()

    @app.post("/control/start")
    def start_worker():
        """Trigger worker generator loop start."""
        success = service.start()
        if not success:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Worker generator is already running or failed to initialize.",
            )
        return {"message": "Worker started successfully", "status": service.get_status()}

    @app.post("/control/stop")
    def stop_worker():
        """Trigger worker generator loop graceful stop."""
        service.stop()
        return {"message": "Worker stopped successfully", "status": service.get_status()}

    @app.patch("/control/config")
    @app.post("/control/config")
    def update_config(req: UpdateWorkerConfigRequest):
        """Update runtime configurable generator parameters."""
        updated = service.update_runtime_config(
            target_tps=req.target_tps,
            new_user_ratio=req.new_user_transaction_ratio,
        )
        return {"message": "Worker configuration updated", "updated": updated}

    return app


class WorkerRunner:
    """Manages the lifecycle of a dedicated generator worker process."""

    def __init__(self, config: GeneratorConfig, autostart: bool = True, publisher: Optional[Any] = None):
        self.config = config
        self.service = GeneratorService(config=config, publisher=publisher)
        self.autostart = autostart
        self._server: Optional[uvicorn.Server] = None
        self._server_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def run(self) -> None:
        """Run worker process with signal handling and internal control server."""
        logger.info(f"Starting WorkerRunner [worker_id={self.config.worker_id}]...")

        def handle_signal(sig, frame):
            logger.info(f"Received signal {sig} in worker. Initiating graceful shutdown...")
            self._stop_event.set()

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)

        # Start internal HTTP control and probe server
        app = create_worker_internal_app(self.service)
        server_config = uvicorn.Config(
            app,
            host="0.0.0.0",
            port=self.config.worker_port,
            log_level="warning",
        )
        self._server = uvicorn.Server(server_config)
        self._server_thread = threading.Thread(target=self._server.run, name="WorkerHTTPServer", daemon=True)
        self._server_thread.start()
        logger.info(f"Worker internal health/control endpoint listening on port {self.config.worker_port}")

        if self.autostart:
            logger.info("Autostarting transaction generation loop...")
            self.service.start()

        # Block main thread until stop signal received
        try:
            while not self._stop_event.is_set():
                self._stop_event.wait(1.0)
        except (KeyboardInterrupt, SystemExit):
            pass
        finally:
            self.stop()

    def stop(self) -> None:
        """Gracefully stop generator and internal server."""
        if not self._stop_event.is_set():
            self._stop_event.set()
        logger.info("Stopping WorkerRunner...")
        self.service.stop(timeout=self.config.kafka.shutdown_flush_timeout_sec)
        if self._server:
            self._server.should_exit = True
        logger.info("WorkerRunner stopped cleanly.")
