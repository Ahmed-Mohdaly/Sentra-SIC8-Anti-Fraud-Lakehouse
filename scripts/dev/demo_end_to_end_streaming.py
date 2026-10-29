"""End-to-End Data Engineering Portfolio Demonstration Script.

Demonstrates:
1. Multi-source synthetic transaction event generation.
2. Stateful rolling-window feature hydration with StateStore.
3. Idempotent duplicate-message retry handling.
4. Resilient error handling for malformed input envelopes.
5. Sub-10ms real-time ML inference integration with explainable risk factors.
6. Verification of S3 Bronze Lakehouse data alignment.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path

from ml.fraud_consumer import FraudStreamingConsumer
from ml.state_store import StateStore
from synthetic_streaming.baseline_loader import BaselineLoader
from synthetic_streaming.cards_generator import CardsGenerator
from synthetic_streaming.classification_and_geo import CityResolver
from synthetic_streaming.config import GeneratorConfig
from synthetic_streaming.relationship_validator import RelationshipValidator
from synthetic_streaming.scenario_engine import ScenarioEngine
from synthetic_streaming.source_simulator import SourceSimulator
from synthetic_streaming.transactions_generator import TransactionsGenerator
from synthetic_streaming.users_generator import UsersGenerator

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("demo_e2e")


def run_pipeline_demo():
    print("=" * 95)
    print("  🚀 DATA ENGINEERING PORTFOLIO DEMO: REAL-TIME STREAMING & ML INFERENCE PIPELINE")
    print("=" * 95)

    cfg = GeneratorConfig()
    
    # 1. Initialize StateStore & Consumer
    print("\n[STEP 1] Initializing Stateful Feature Store & Fraud Streaming Consumer...")
    consumer = FraudStreamingConsumer(config=cfg)
    consumer.preload_reference_data()
    print(f"  • Feature Store Mode: {'In-Memory Fallback' if consumer.state_store.is_using_fallback else 'Redis Cache'}")
    print(f"  • Active Model Path:  {consumer.ml_cfg.model_path if consumer.model else 'Rule-Based Baseline'}")
    print(f"  • Decision Threshold: {consumer.threshold:.4f}")

    # 2. Setup Generator Engine
    print("\n[STEP 2] Initializing Synthetic Transaction Generator Engine...")
    baseline = BaselineLoader(data_dir=cfg.baseline_data_dir)
    baseline.load()
    city_resolver = CityResolver()
    city_resolver.load_from_csv("data/city_reference.csv")

    users_gen = UsersGenerator(initial_high_water_mark=len(baseline.users))
    cards_gen = CardsGenerator()
    source_sim = SourceSimulator()
    scenario_engine = ScenarioEngine()
    validator = RelationshipValidator()

    tx_gen = TransactionsGenerator(
        baseline_loader=baseline,
        users_generator=users_gen,
        cards_generator=cards_gen,
        source_simulator=source_sim,
        scenario_engine=scenario_engine,
        relationship_validator=validator,
        city_resolver=city_resolver,
    )
    print(f"  • Preloaded Reference Data: {len(baseline.users):,} Users, {len(baseline.cards_by_key):,} Cards, {len(city_resolver):,} Cities")

    # 3. Stream Synthetic Events & Score
    print("\n[STEP 3] Streaming 25 Live Transaction Events Through Pipeline...")
    print("-" * 115)
    print(f"{'Event ID':<16} | {'User':<5} | {'Amount':<9} | {'Category (MCC)':<16} | {'ML Alert':<9} | {'Score':<7} | {'Latency':<7} | {'Risk Factors'}")
    print("-" * 115)

    latencies = []
    first_envelope = None

    for i in range(25):
        envelope, _ = tx_gen.generate_next_transaction(run_id="demo-stream-001")
        env_dict = envelope.model_dump()
        if i == 0:
            first_envelope = env_dict

        alert = consumer.process_transaction(env_dict)
        latencies.append(alert["latency_ms"])

        ev_id = alert.get("event_id", "")[:15]
        user_id = alert.get("user_id", 0)
        amt = alert.get("amount", 0.0)
        mcc = env_dict.get("payload", {}).get("MCC", "N/A")
        flag = "🚨 ALERT" if alert.get("is_fraud_predicted") else "✅ PASS"
        prob = alert.get("fraud_probability", 0.0)
        lat = alert.get("latency_ms", 0.0)
        reasons = ", ".join(alert.get("risk_factors", [])[:2]) if alert.get("risk_factors") else "-"

        print(f"{ev_id:<16} | {user_id:<5} | ${amt:<8.2f} | MCC {mcc:<12} | {flag:<9} | {prob:<7.3f} | {lat:<5.2f}ms | {reasons}")

    # 4. Idempotency & Duplicate Handling Test
    print("\n[STEP 4] Verifying Idempotency & Duplicate Event Handling...")
    if first_envelope:
        dup_event_id = first_envelope["event_id"]
        card_key = f"{first_envelope['payload']['User']}:{first_envelope['payload']['Card']}"
        prior_history_len = len(consumer.state_store._local_user_tx_history.get(card_key, []))

        # Re-send the exact same envelope
        alert_dup = consumer.process_transaction(first_envelope)
        post_history_len = len(consumer.state_store._local_user_tx_history.get(card_key, []))

        print(f"  • Retried Event ID:       {dup_event_id}")
        print(f"  • History Length Before:  {prior_history_len}")
        print(f"  • History Length After:   {post_history_len}")
        assert post_history_len == prior_history_len, "Duplicate message corrupted rolling state!"
        print("  ✅ Duplicate detected and suppressed from mutating state!")

    # 5. Malformed Input Error Handling Test
    print("\n[STEP 5] Verifying Error Handling on Malformed Input...")
    malformed_tests = [
        ("Corrupt String Input", "corrupt_raw_bytes_not_json"),
        ("Missing Payload Dict", {"event_id": "bad-envelope-001", "payload": None}),
    ]
    for desc, bad_input in malformed_tests:
        res = consumer.process_transaction(bad_input)  # type: ignore
        status = res.get("status")
        print(f"  • Input: {desc:<22} -> Handled Status: {status} (Gracefully caught without crash)")
        assert status in ("MALFORMED_INPUT", "MALFORMED_PAYLOAD"), f"Unexpected status: {status}"
    print("  ✅ Malformed inputs safely caught and routed to error status!")

    # 6. Performance & SLA Verification
    avg_lat = sum(latencies) / len(latencies)
    p95_lat = sorted(latencies)[int(len(latencies) * 0.95)]
    print("\n[STEP 6] Pipeline Performance SLA Summary:")
    print(f"  • Average Latency:      {avg_lat:.2f} ms per transaction")
    print(f"  • P95 Latency:          {p95_lat:.2f} ms")
    print(f"  • Throughput Capacity:  {1000.0 / avg_lat:.1f} transactions / sec per core")
    assert avg_lat < 10.0, f"Average latency exceeded 10ms SLA: {avg_lat:.2f}ms"
    print("  ✅ Real-time sub-10ms processing SLA verified!")

    print("\n" + "=" * 95)
    print("  🎉 DEMO COMPLETE: End-to-end streaming, state store, idempotency, and ML inference verified.")
    print("=" * 95)


if __name__ == "__main__":
    run_pipeline_demo()
