import time
from synthetic_streaming.rate_controller import RateController


def test_rate_controller_pacing():
    target_tps = 20.0
    controller = RateController(target_tps=target_tps)
    controller.start()

    start_time = time.monotonic()
    event_count = 10
    for _ in range(event_count):
        controller.wait_for_slot()

    elapsed = time.monotonic() - start_time
    controller.stop()

    # 10 events at 20 TPS should take approx (10 - 1) / 20 = 0.45s, plus tolerance
    assert 0.35 <= elapsed <= 0.70


def test_rate_controller_dynamic_target_update():
    controller = RateController(target_tps=10.0)
    assert controller.target_tps == 10.0
    controller.set_target_tps(50.0)
    assert controller.target_tps == 50.0


def test_rate_controller_stop():
    controller = RateController(target_tps=10.0)
    controller.start()
    controller.stop()
    assert controller.wait_for_slot() is False
