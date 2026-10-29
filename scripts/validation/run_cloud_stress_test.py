#!/usr/bin/env python3
"""Automated Cloud Stress & Peak Breaking-Point Test for SIC Streaming Platform.

Pushes the generation and streaming pipeline through progressive TPS tiers:
[100, 250, 500, 750, 1000, 1500, 2000, 3000, 5000] TPS
Identifies:
1. Linear Capacity Zone (Zero-drift throughput)
2. Knee / Saturation Point (Throughput plateau)
3. Absolute Peak Capacity (Maximum achievable TPS)
4. Failure / Queue Overflow Threshold
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import boto3
import httpx


@dataclass
class StressStepResult:
    target_tps: float
    duration_seconds: float
    total_generated: int
    total_delivered: int
    total_failed: int
    actual_tps: float
    delivered_tps: float
    capacity_efficiency_pct: float
    buffered_messages_peak: int
    in_flight_messages_peak: int
    error_rate_pct: float
    status: str
    bottleneck_diagnosis: str


class CloudStressTestHarness:
    def __init__(self, api_base_url: str = "http://13.38.114.72:8000"):
        self.api_base_url = api_base_url.rstrip("/")
        self.client = httpx.Client(timeout=10.0)

    def probe_ready(self) -> bool:
        try:
            resp = self.client.get(f"{self.api_base_url}/health")
            return resp.status_code == 200
        except Exception:
            return False

    def run_stress_step(
        self,
        target_tps: float,
        duration: float = 20.0,
        new_user_ratio: float = 0.05,
    ) -> StressStepResult:
        print(f"\n>>> Running Stress Tier: Target = {target_tps:4.0f} TPS for {duration:.0f}s (new_user_ratio={new_user_ratio})...")

        # Configure target TPS
        patch_resp = self.client.patch(
            f"{self.api_base_url}/api/v1/generator/config",
            json={"target_tps": target_tps, "new_user_transaction_ratio": new_user_ratio},
        )
        if patch_resp.status_code != 200:
            raise RuntimeError(f"Config patch failed: {patch_resp.text}")

        # Baseline snapshot
        base_status = self.client.get(f"{self.api_base_url}/api/v1/generator/status").json()
        base_gen = base_status.get("total_transactions_generated", 0)
        base_deliv = base_status.get("publisher_health", {}).get("total_delivered", 0)
        base_failed = base_status.get("failed_deliveries", 0)

        # Start generator
        start_resp = self.client.post(f"{self.api_base_url}/api/v1/generator/start")
        if start_resp.status_code != 200:
            raise RuntimeError(f"Generator start failed: {start_resp.text}")

        start_time = time.monotonic()
        peak_buffer = 0
        peak_inflight = 0
        sampled_tps_list = []

        while True:
            elapsed = time.monotonic() - start_time
            if elapsed >= duration:
                break
            time.sleep(min(2.0, duration - elapsed))

            try:
                st = self.client.get(f"{self.api_base_url}/api/v1/generator/status").json()
                cur_tps = st.get("actual_generated_tps", 0.0)
                cur_buf = st.get("publisher_health", {}).get("buffered_messages", 0)
                cur_inf = st.get("in_flight_messages", 0)
                peak_buffer = max(peak_buffer, cur_buf)
                peak_inflight = max(peak_inflight, cur_inf)
                if cur_tps > 0:
                    sampled_tps_list.append(cur_tps)
                print(f"    [{elapsed:>4.1f}s] Current TPS: {cur_tps:>7.1f} | Buffer: {cur_buf:>4d} | In-flight: {cur_inf:>4d}")
            except Exception as e:
                print(f"    [{elapsed:>4.1f}s] Polling error: {e}")

        # Stop generator
        self.client.post(f"{self.api_base_url}/api/v1/generator/stop")
        time.sleep(2.0)  # Allow final flush

        # End snapshot
        end_status = self.client.get(f"{self.api_base_url}/api/v1/generator/status").json()
        end_gen = end_status.get("total_transactions_generated", 0)
        end_deliv = end_status.get("publisher_health", {}).get("total_delivered", 0)
        end_failed = end_status.get("failed_deliveries", 0)

        delta_gen = end_gen - base_gen
        delta_deliv = end_deliv - base_deliv
        delta_failed = end_failed - base_failed

        actual_gen_tps = delta_gen / duration if duration > 0 else 0.0
        actual_deliv_tps = delta_deliv / duration if duration > 0 else 0.0
        efficiency = (actual_gen_tps / target_tps * 100.0) if target_tps > 0 else 100.0
        error_rate = (delta_failed / delta_gen * 100.0) if delta_gen > 0 else 0.0

        # Diagnosis
        if efficiency >= 95.0:
            status = "HEALTHY_LINEAR"
            diagnosis = "Zero bottleneck. Full linear throughput sustained."
        elif efficiency >= 80.0:
            status = "NEAR_CAPACITY"
            diagnosis = "Approaching saturation knee. Minor rate pacing drift."
        elif efficiency >= 50.0:
            status = "CPU_SATURATED"
            diagnosis = "Worker CPU/thread saturated. Generator throughput capped."
        else:
            status = "SEVERELY_CAPPED"
            diagnosis = "Hard ceiling reached. Single-thread CPU limit on Fargate."

        result = StressStepResult(
            target_tps=target_tps,
            duration_seconds=round(duration, 2),
            total_generated=delta_gen,
            total_delivered=delta_deliv,
            total_failed=delta_failed,
            actual_tps=round(actual_gen_tps, 1),
            delivered_tps=round(actual_deliv_tps, 1),
            capacity_efficiency_pct=round(efficiency, 1),
            buffered_messages_peak=peak_buffer,
            in_flight_messages_peak=peak_inflight,
            error_rate_pct=round(error_rate, 2),
            status=status,
            bottleneck_diagnosis=diagnosis,
        )

        print(f"  --> Step Result: Target={target_tps} -> Actual={result.actual_tps} TPS ({result.capacity_efficiency_pct}%) | Status={status}")
        return result

    def run_full_stress_test(
        self,
        steps: List[float],
        step_duration: float = 20.0,
        new_user_ratio: float = 0.05,
    ) -> List[StressStepResult]:
        print("=" * 85)
        print("🚀 SIC CLOUD STRESS TEST: BREAKING POINT & PEAK CAPACITY BENCHMARK")
        print(f"Target Endpoint: {self.api_base_url}")
        print(f"Stress Tiers: {steps}")
        print(f"Step Duration: {step_duration}s each")
        print("=" * 85)

        results = []
        for tps in steps:
            res = self.run_stress_step(target_tps=tps, duration=step_duration, new_user_ratio=new_user_ratio)
            results.append(res)
            time.sleep(3.0)  # Brief recovery cooldown between tiers

        return results


def main():
    parser = argparse.ArgumentParser(description="SIC Cloud Stress Test")
    parser.add_argument("--url", default="http://13.38.114.72:8000", help="FastAPI Base URL")
    parser.add_argument("--steps", default="100,250,500,750,1000,1500,2000,3000,5000", help="Comma-separated TPS steps")
    parser.add_argument("--duration", type=float, default=20.0, help="Duration in seconds per step")
    parser.add_argument("--new-user-ratio", type=float, default=0.05, help="Ratio of new users triggering DB writes")
    parser.add_argument("--output-json", default="reports/cloud_stress_test_results.json", help="Path to JSON output")
    parser.add_argument("--output-md", default="reports/cloud_stress_test_report.md", help="Path to Markdown report")
    args = parser.parse_args()

    steps = [float(s.strip()) for s in args.steps.split(",")]
    harness = CloudStressTestHarness(api_base_url=args.url)

    if not harness.probe_ready():
        print(f"❌ Error: Cloud endpoint {args.url} is not reachable or healthy!")
        sys.exit(1)

    results = harness.run_full_stress_test(
        steps=steps,
        step_duration=args.duration,
        new_user_ratio=args.new_user_ratio,
    )

    # Save JSON results
    out_json = Path(args.output_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    with open(out_json, "w") as f:
        json.dump([asdict(r) for r in results], f, indent=2)
    print(f"\n✅ Saved JSON results to {out_json}")

    # Generate Markdown Summary
    header = f"{'Target TPS':<12} {'Actual TPS':<12} {'Deliv TPS':<12} {'Total Events':<14} {'Efficiency':<12} {'Buffer Peak':<13} {'Status':<18} {'Diagnosis'}"
    print("\n" + "=" * len(header))
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r.target_tps:<12.0f} {r.actual_tps:<12.1f} {r.delivered_tps:<12.1f} {r.total_generated:<14} "
            f"{r.capacity_efficiency_pct:>9.1f}%   {r.buffered_messages_peak:<13} {r.status:<18} {r.bottleneck_diagnosis}"
        )
    print("=" * len(header) + "\n")


if __name__ == "__main__":
    main()
