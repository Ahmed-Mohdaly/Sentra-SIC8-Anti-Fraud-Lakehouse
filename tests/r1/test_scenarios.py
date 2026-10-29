import random
from synthetic_streaming.scenario_engine import ScenarioEngine
from synthetic_streaming.source_simulator import SourceSimulator


def test_scenario_engine_distribution():
    engine = ScenarioEngine(rng=random.Random(42))
    scenarios_sampled = set()
    for _ in range(500):
        outcome = engine.sample_scenario()
        scenarios_sampled.add(outcome.scenario_name)
        if outcome.scenario_name == "refund_return":
            assert outcome.amount < 0
        elif outcome.scenario_name == "high_value":
            assert outcome.amount >= 1000

    assert "normal_spend" in scenarios_sampled
    assert "refund_return" in scenarios_sampled
    assert "high_value" in scenarios_sampled


def test_invalid_event_injection_disabled_by_default():
    engine = ScenarioEngine(rng=random.Random(42))
    for _ in range(100):
        outcome = engine.sample_scenario()
        assert outcome.is_invalid is False
        assert outcome.scenario_name != "invalid_test_record"


def test_invalid_event_injection_enabled():
    engine = ScenarioEngine(inject_invalid_events=True, invalid_event_rate=1.0, rng=random.Random(42))
    outcome = engine.sample_scenario()
    assert outcome.is_invalid is True
    assert outcome.scenario_name == "invalid_test_record"


def test_source_simulator_combinations():
    sim = SourceSimulator(rng=random.Random(42))
    for _ in range(100):
        source, method = sim.sample_source_and_method()
        assert sim.is_valid_combination(source, method)
