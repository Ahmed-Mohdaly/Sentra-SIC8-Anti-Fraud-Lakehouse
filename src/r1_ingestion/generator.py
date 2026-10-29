"""Local deterministic *reference* generator, NOT Sayed's production simulator."""
from __future__ import annotations
import random
from datetime import datetime, timedelta, timezone
from typing import Any

DEFAULT_START = datetime(2026, 10, 25, 12, 0, tzinfo=timezone.utc)

def make_fixture(seed: int = 8, run_id: str = 'fixture-r1', start: datetime = DEFAULT_START) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rnd = random.Random(seed)
    events: list[dict[str, Any]] = []
    truth: list[dict[str, Any]] = []

    def event(scenario: str | None, account: str, card: str, channel: str, minute: float,
              amount: str, latitude: float | None = None, longitude: float | None = None,
              status: str = 'APPROVED', failure: str | None = None) -> None:
        index = len(events) + 1
        event_id = f'{run_id}:e{index:04d}'
        rec = dict(event_id=event_id, transaction_id=f'{run_id}:tx{index:04d}',
                   account_id=account, card_id=card, channel=channel, currency='USD',
                   amount=amount, status=status, event_time=(start + timedelta(minutes=minute)).isoformat(),
                   latitude=latitude, longitude=longitude,
                   location_precision='EXACT_SYNTHETIC' if latitude is not None else 'UNKNOWN',
                   terminal_id='TERM-1' if channel != 'ECOMMERCE' else None,
                   terminal_failure_reason=failure, run_id=run_id, mcc=5411)
        events.append(rec)
        if scenario:
            truth.append(dict(event_id=event_id, run_id=run_id, scenario_family=scenario))

    event(None,'normal','c-normal','POS',0,'12.00',40.7,-74.0)
    event(None,'normal','c-normal','ATM',5,'30.00',40.7,-74.0)
    event(None,'normal','c-normal','POS',6,'20.00',status='DECLINED')
    event(None,'normal','c-normal','ATM',7,'0.00',status='ERROR',failure='DEVICE_OFFLINE')
    # Three independent anomaly families required by SIC8
    for minute in (10,12,13):
        event('CNP_BURST','cnp','c-cnp','ECOMMERCE',minute,str(20+rnd.randint(1,20)))
    event('GEO_VELOCITY','geo','c-geo','POS',20,'100.00',40.7128,-74.006)
    event('GEO_VELOCITY','geo','c-geo','POS',25,'101.00',35.6762,139.6503)
    for minute in (30,31,32):
        event('MICRO_TESTING','micro','c-micro','ECOMMERCE',minute,'0.50')
    event(None,'other','c-other','ECOMMERCE',40,'-4.00')  # refund, excluded from positive attempt rules
    return events, truth
