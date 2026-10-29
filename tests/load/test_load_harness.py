try:
    from scripts.benchmark.load_test import LoadTestHarness
except ImportError:
    from scripts.load_test import LoadTestHarness
from synthetic_streaming.config import GeneratorConfig


def test_load_harness_step():
    config = GeneratorConfig()
    harness = LoadTestHarness(config=config, use_mock=True)
    res = harness.run_step(target_tps=30.0, duration_seconds=1.0)

    assert res.target_tps == 30.0
    assert res.total_generated > 0
    assert res.actual_generated_tps > 0
    assert res.error_rate_percent == 0.0
    assert res.latency_p50_ms >= 0.0
