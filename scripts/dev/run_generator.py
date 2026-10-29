#!/usr/bin/env python3
"""Unified execution entrypoint for Synthetic Transaction Streaming Platform supporting worker and API roles."""

from __future__ import annotations

import argparse
import logging
import signal
import sys
import time
from pathlib import Path

# Add src to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import uvicorn
from synthetic_streaming.api import create_app
from synthetic_streaming.config import GeneratorConfig
from synthetic_streaming.generator_service import GeneratorService
from synthetic_streaming.kafka_publisher import MockEventPublisher
from synthetic_streaming.worker import WorkerRunner


def setup_logging(log_level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )


def main():
    parser = argparse.ArgumentParser(description="Synthetic Transaction Streaming Service")
    parser.add_argument("role_arg", nargs="?", choices=["generator-worker", "generator-api", "worker", "api", "all"], help="Execution role")
    parser.add_argument("--role", choices=["all", "worker", "api"], help="Execution role override")
    parser.add_argument("--config", default=None, help="Path to YAML config file")
    parser.add_argument("--tps", type=float, help="Override target TPS")
    parser.add_argument("--bootstrap", type=str, help="Override Kafka bootstrap servers")
    parser.add_argument("--max-events", type=int, help="Stop after N transaction events")
    parser.add_argument("--duration", type=float, help="Stop after N seconds")
    parser.add_argument("--data-dir", type=str, help="Path to baseline CSV data directory")
    parser.add_argument("--seed", type=int, help="Deterministic random seed")
    parser.add_argument("--mock", action="store_true", help="Use in-memory Mock publisher instead of Kafka")
    parser.add_argument("--worker-url", type=str, help="Worker control URL for API mode")
    parser.add_argument("--worker-port", type=int, help="Internal worker port")
    parser.add_argument("--api-port", type=int, help="Management API port")
    parser.add_argument("--no-api", action="store_true", help="Run standalone generator loop without FastAPI")
    args = parser.parse_args()

    # Load configuration
    config = GeneratorConfig.load(args.config)
    if args.tps:
        config.target_tps = args.tps
    if args.bootstrap:
        config.kafka.bootstrap_servers = args.bootstrap
    if args.max_events:
        config.max_events = args.max_events
    if args.duration:
        config.run_duration_seconds = args.duration
    if args.data_dir:
        config.baseline_data_dir = Path(args.data_dir)
    if args.seed is not None:
        config.random_seed = args.seed
    if args.worker_url:
        config.worker_control_url = args.worker_url
    if args.worker_port:
        config.worker_port = args.worker_port
    if args.api_port:
        config.api_port = args.api_port

    # Determine execution role
    role = config.role
    if args.role:
        role = args.role
    elif args.role_arg:
        if args.role_arg in ("generator-worker", "worker"):
            role = "worker"
        elif args.role_arg in ("generator-api", "api"):
            role = "api"
        elif args.role_arg == "all":
            role = "all"

    setup_logging(config.log_level)
    logger = logging.getLogger("synthetic_streaming")

    logger.info("Synthetic Transaction Streaming Platform initializing")
    logger.info(f"Role: {role} | Target TPS: {config.target_tps} | Bootstrap: {config.kafka.bootstrap_servers}")

    if role == "worker":
        # Dedicated worker process: continuous streaming + internal health/control port
        logger.info(f"Starting generator worker [id={config.worker_id}] on internal port {config.worker_port}")
        publisher = MockEventPublisher() if args.mock else None
        runner = WorkerRunner(config=config, autostart=True, publisher=publisher)
        runner.run()

    elif role == "api":
        # Unified synthetic transaction provider service and REST API process
        logger.info(f"Starting synthetic transaction provider API on {config.api_host}:{config.api_port}")
        publisher = MockEventPublisher() if args.mock else None
        service = GeneratorService(config=config, publisher=publisher)
        app = create_app(service=service, config=config)
        uvicorn.run(app, host=config.api_host, port=config.api_port, log_level="warning")

    else:
        # Unified standalone development mode (runs generator service + API together)
        publisher = MockEventPublisher() if args.mock else None
        service = GeneratorService(config=config, publisher=publisher)

        def handle_signal(sig, frame):
            logger.info("Shutdown signal received. Stopping gracefully...")
            service.stop()
            sys.exit(0)

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)

        service.start()

        if args.no_api:
            logger.info("Running in standalone streaming mode without API (Ctrl+C to stop)")
            try:
                while service.state.value in ("RUNNING", "STARTING"):
                    time.sleep(2.0)
                    status = service.get_status()
                    logger.info(
                        f"Generated: {status['total_transactions_generated']} tx | "
                        f"Actual TPS: {status['actual_generated_tps']} | "
                        f"Uptime: {status['uptime_seconds']}s"
                    )
            finally:
                service.stop()
        else:
            logger.info(f"Starting unified management server on {config.api_host}:{config.api_port}")
            app = create_app(service=service, config=config)
            uvicorn.run(app, host=config.api_host, port=config.api_port, log_level="warning")


if __name__ == "__main__":
    main()
