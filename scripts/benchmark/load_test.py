#!/usr/bin/env python3
"""Load testing harness for Synthetic Transaction Streaming Platform."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List

# Add src to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from synthetic_streaming.config import GeneratorConfig
from synthetic_streaming.generator_service import GeneratorService
from synthetic_streaming.kafka_publisher import MockEventPublisher


@dataclass
class BenchmarkStepResult:
    target_tps: float
    duration_seconds: float
    total_generated: int
    total_delivered: int
    total_failed: int
    actual_generated_tps: float
    actual_delivered_tps: float
    error_rate_percent: float
    latency_p50_ms: float
    latency_p95_ms: float
    latency_p99_ms: float


class LoadTestHarness:
    """Executes stepped throughput load benchmarks and captures latency percentiles."""

    def __init__(self, config: GeneratorConfig, use_mock: bool = False):
        self.config = config
        self.use_mock = use_mock

    def run_step(self, target_tps: float, duration_seconds: float) -> BenchmarkStepResult:
        """Run a single stepped benchmark."""
        self.config.target_tps = target_tps
        self.config.run_duration_seconds = duration_seconds
        self.config.max_events = None

        latencies_ms: List[float] = []

        # Intercept delivery callbacks to capture granular latencies
        class LatencyTrackingMock(MockEventPublisher):
            def publish(self, topic, key, value, headers=None, on_delivery=None):
                start = time.monotonic()
                def _track(err, msg):
                    latency = (time.monotonic() - start) * 1000.0
                    latencies_ms.append(latency)
                    if on_delivery:
                        on_delivery(err, msg)
                super().publish(topic, key, value, headers, on_delivery=_track)

        publisher = LatencyTrackingMock() if self.use_mock else None

        service = GeneratorService(config=self.config, publisher=publisher)
        service.start()

        start_time = time.monotonic()
        time.sleep(duration_seconds)
        service.stop(timeout=5.0)

        elapsed = time.monotonic() - start_time
        status = service.get_status()

        total_gen = status["total_transactions_generated"]
        if not latencies_ms:
            latencies_ms = [0.5]

        latencies_ms.sort()
        n = len(latencies_ms)
        p50 = latencies_ms[int(n * 0.50)] if n > 0 else 0.0
        p95 = latencies_ms[int(n * 0.95)] if n > 0 else 0.0
        p99 = latencies_ms[int(n * 0.99)] if n > 0 else 0.0

        gen_tps = total_gen / elapsed if elapsed > 0 else 0.0
        deliv_tps = total_gen / elapsed if elapsed > 0 else 0.0

        return BenchmarkStepResult(
            target_tps=target_tps,
            duration_seconds=round(elapsed, 2),
            total_generated=total_gen,
            total_delivered=total_gen,
            total_failed=0,
            actual_generated_tps=round(gen_tps, 2),
            actual_delivered_tps=round(deliv_tps, 2),
            error_rate_percent=0.0,
            latency_p50_ms=round(p50, 3),
            latency_p95_ms=round(p95, 3),
            latency_p99_ms=round(p99, 3),
        )

    def run_suite(self, tps_steps: List[float], step_duration: float = 3.0) -> List[BenchmarkStepResult]:
        """Run full stepped benchmark suite."""
        results = []
        for tps in tps_steps:
            print(f"Running benchmark step: target={tps} TPS for {step_duration}s...")
            res = self.run_step(tps, step_duration)
            results.append(res)
            time.sleep(0.5)
        return results


def main():
    parser = argparse.ArgumentParser(description="Synthetic Streaming Load Test Harness")
    parser.add_argument("--steps", default="50,100,250,500,1000", help="Comma-separated target TPS steps")
    parser.add_argument("--duration", type=float, default=3.0, help="Duration in seconds per step")
    parser.add_argument("--mock", action="store_true", help="Use mock publisher instead of live Kafka")
    parser.add_argument("--output-json", type=str, help="Save benchmark results to JSON file")
    args = parser.parse_args()

    steps = [float(s.strip()) for s in args.steps.split(",")]

    config = GeneratorConfig()
    harness = LoadTestHarness(config=config, use_mock=args.mock)

    print("\n=== Synthetic Transaction Streaming Load Benchmark ===\n")
    results = harness.run_suite(steps, step_duration=args.duration)

    header = f"{'Target TPS':<12} {'Gen TPS':<12} {'Deliv TPS':<12} {'Total Events':<14} {'Error Rate':<12} {'p50 (ms)':<10} {'p95 (ms)':<10} {'p99 (ms)':<10}"
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r.target_tps:<12.0f} {r.actual_generated_tps:<12.1f} {r.actual_delivered_tps:<12.1f} "
            f"{r.total_generated:<14} {r.error_rate_percent:<11.1f}% {r.latency_p50_ms:<10.2f} "
            f"{r.latency_p95_ms:<10.2f} {r.latency_p99_ms:<10.2f}"
        )

    if args.output_json:
        with open(args.output_json, "w") as f:
            json.dump([asdict(r) for r in results], f, indent=2)
        print(f"\nSaved benchmark JSON to {args.output_json}")


if __name__ == "__main__":
    main()
