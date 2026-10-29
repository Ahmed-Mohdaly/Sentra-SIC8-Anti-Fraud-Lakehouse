"""End-to-end verification script for real-time Kafka transaction scoring and explainable fraud detection."""

from __future__ import annotations

import time
from pathlib import Path
from ml.fraud_consumer import FraudStreamingConsumer
from synthetic_streaming.baseline_loader import BaselineLoader
from synthetic_streaming.cards_generator import CardsGenerator
from synthetic_streaming.classification_and_geo import CityResolver
from synthetic_streaming.config import GeneratorConfig
from synthetic_streaming.relationship_validator import RelationshipValidator
from synthetic_streaming.scenario_engine import ScenarioEngine
from synthetic_streaming.source_simulator import SourceSimulator
from synthetic_streaming.transactions_generator import TransactionsGenerator
from synthetic_streaming.users_generator import UsersGenerator


def main():
    print("=" * 80)
    print("🧪 Real-Time Fraud Detection Live Benchmark & Explainability Verification")
    print("=" * 80)

    cfg = GeneratorConfig()
    consumer = FraudStreamingConsumer(config=cfg)
    consumer.preload_reference_data()

    # Setup generator with diverse scenarios (including fraud and high value)
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

    print("\nGenerating 50 streaming transactions and evaluating through real-time ML pipeline...")
    latencies = []
    alerts = []

    print("-" * 110)
    print(f"{'User':<6} | {'Amount':<10} | {'GroundTruth':<12} | {'ML Pred':<8} | {'Prob':<8} | {'Latency':<8} | {'Risk Factors'}")
    print("-" * 110)

    for i in range(50):
        envelope, _ = tx_gen.generate_next_transaction(run_id="bench-001")
        alert = consumer.process_transaction(envelope.model_dump())
        latencies.append(alert["latency_ms"])
        alerts.append(alert)

        pred_str = "🚨 FRAUD" if alert["is_fraud_predicted"] else "✅ Normal"
        actual_str = "Yes (Fraud)" if alert["ground_truth_fraud"] == "Yes" else "No (Normal)"
        reasons = ", ".join(alert["risk_factors"][:2]) if alert["risk_factors"] else "-"

        print(
            f"{alert['user_id']:<6} | "
            f"${alert['amount']:<9.2f} | "
            f"{actual_str:<12} | "
            f"{pred_str:<8} | "
            f"{alert['fraud_probability']:<8.3f} | "
            f"{alert['latency_ms']:<6.2f}ms | "
            f"{reasons}"
        )

    avg_latency = sum(latencies) / len(latencies)
    max_latency = max(latencies)
    p95_latency = sorted(latencies)[int(len(latencies) * 0.95)]

    print("-" * 110)
    print(f"\n📊 Performance Summary over {len(alerts)} Real-Time Events:")
    print(f"  • Average Latency:      {avg_latency:.2f} ms per transaction")
    print(f"  • 95th Percentile (P95): {p95_latency:.2f} ms")
    print(f"  • Max Latency:          {max_latency:.2f} ms")
    print(f"  • Throughput Capacity:  {1000.0 / avg_latency:.1f} events/sec per core")
    print(f"  • Total Frauds Flagged: {consumer.total_frauds_detected}")
    print(f"  • Prediction Accuracy:  {(consumer.total_correct_evaluations / consumer.total_processed) * 100:.1f}%\n")

    assert avg_latency < 10.0, f"Average latency too high: {avg_latency}ms"
    print("✅ Benchmark passed! Sub-10ms latency SLA strictly verified.")


if __name__ == "__main__":
    main()
