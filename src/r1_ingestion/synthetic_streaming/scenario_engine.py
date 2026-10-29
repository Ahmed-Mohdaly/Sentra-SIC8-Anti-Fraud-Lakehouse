"""Scenario engine driving diverse financial patterns and edge cases."""

from __future__ import annotations

import random
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple
from synthetic_streaming.config import ScenarioWeights


ERROR_REASONS = [
    "Insufficient Balance",
    "Bad PIN",
    "Technical Glitch",
    "Bad Card Number",
    "Bad Expiration",
    "Bad CVV",
    "Bad PIN,Insufficient Balance",
]

ERROR_WEIGHTS = [0.55, 0.15, 0.12, 0.08, 0.04, 0.04, 0.02]


@dataclass
class ScenarioOutcome:
    scenario_name: str
    amount: Decimal
    errors: Optional[str]
    is_fraud: str
    business_time_offset_seconds: int
    is_invalid: bool = False
    custom_overrides: Optional[Dict[str, Any]] = None


class ScenarioEngine:
    """Generates scenario-specific amounts, error states, fraud flags, and temporal offsets."""

    def __init__(
        self,
        weights: Optional[ScenarioWeights] = None,
        inject_invalid_events: bool = False,
        invalid_event_rate: float = 0.01,
        rng: Optional[random.Random] = None
    ):
        self._weights = weights or ScenarioWeights()
        self._inject_invalid = inject_invalid_events
        self._invalid_rate = invalid_event_rate
        self._rng = rng or random.Random()

        self._scenarios = [
            "normal_spend",
            "high_value",
            "unusual_location",
            "fraud_labelled",
            "declined_error",
            "refund_return",
            "repeated_attempt",
            "late_event",
        ]
        self._scenario_weights = [
            self._weights.normal_spend,
            self._weights.high_value,
            self._weights.unusual_location,
            self._weights.fraud_labelled,
            self._weights.declined_error,
            self._weights.refund_return,
            self._weights.repeated_attempt,
            self._weights.late_event,
        ]

    def sample_scenario(self) -> ScenarioOutcome:
        """Sample a business scenario and generate amount, error, fraud, and time offset."""
        # Opt-in invalid data-quality test record
        if self._inject_invalid and self._rng.random() < self._invalid_rate:
            return ScenarioOutcome(
                scenario_name="invalid_test_record",
                amount=Decimal("-999999.99"),
                errors="Malformed Data Quality Test",
                is_fraud="No",
                business_time_offset_seconds=0,
                is_invalid=True,
                custom_overrides={"MCC": -1, "Use Chip": "Invalid_Method"},
            )

        scenario = self._rng.choices(self._scenarios, weights=self._scenario_weights, k=1)[0]

        if scenario == "normal_spend":
            cents = self._rng.randint(250, 25000)
            return ScenarioOutcome(
                scenario_name=scenario,
                amount=Decimal(cents) / Decimal(100),
                errors=None,
                is_fraud="No",
                business_time_offset_seconds=0,
            )

        elif scenario == "high_value":
            dollars = self._rng.randint(1000, 15000)
            cents = self._rng.randint(0, 99)
            return ScenarioOutcome(
                scenario_name=scenario,
                amount=Decimal(dollars) + (Decimal(cents) / Decimal(100)),
                errors=None,
                is_fraud="No",
                business_time_offset_seconds=0,
            )

        elif scenario == "unusual_location":
            cents = self._rng.randint(1500, 80000)
            return ScenarioOutcome(
                scenario_name=scenario,
                amount=Decimal(cents) / Decimal(100),
                errors=None,
                is_fraud="No",
                business_time_offset_seconds=0,
                custom_overrides={"Merchant City": "Online / International", "Merchant State": None, "Zip": None},
            )

        elif scenario == "fraud_labelled":
            cents = self._rng.randint(35000, 450000)
            return ScenarioOutcome(
                scenario_name=scenario,
                amount=Decimal(cents) / Decimal(100),
                errors=None,
                is_fraud="Yes",
                business_time_offset_seconds=0,
            )

        elif scenario == "declined_error":
            cents = self._rng.randint(1000, 50000)
            err = self._rng.choices(ERROR_REASONS, weights=ERROR_WEIGHTS, k=1)[0]
            return ScenarioOutcome(
                scenario_name=scenario,
                amount=Decimal(cents) / Decimal(100),
                errors=err,
                is_fraud="No",
                business_time_offset_seconds=0,
            )

        elif scenario == "refund_return":
            cents = self._rng.randint(500, 35000)
            refund_amount = -(Decimal(cents) / Decimal(100))
            return ScenarioOutcome(
                scenario_name=scenario,
                amount=refund_amount,
                errors=None,
                is_fraud="No",
                business_time_offset_seconds=0,
            )

        elif scenario == "repeated_attempt":
            cents = self._rng.randint(1500, 12000)
            return ScenarioOutcome(
                scenario_name=scenario,
                amount=Decimal(cents) / Decimal(100),
                errors="Bad PIN" if self._rng.random() < 0.5 else None,
                is_fraud="No",
                business_time_offset_seconds=0,
            )

        elif scenario == "late_event":
            cents = self._rng.randint(1000, 20000)
            # Offset by 2 to 7 days in the past
            offset_seconds = self._rng.randint(2 * 86400, 7 * 86400)
            return ScenarioOutcome(
                scenario_name=scenario,
                amount=Decimal(cents) / Decimal(100),
                errors=None,
                is_fraud="No",
                business_time_offset_seconds=offset_seconds,
            )

        # Fallback default
        return ScenarioOutcome(
            scenario_name="normal_spend",
            amount=Decimal("25.00"),
            errors=None,
            is_fraud="No",
            business_time_offset_seconds=0,
        )
