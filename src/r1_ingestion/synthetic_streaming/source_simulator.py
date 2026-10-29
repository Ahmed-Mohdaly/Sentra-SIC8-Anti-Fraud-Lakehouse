"""Simulation of originating source systems and their mapping to payment interaction methods."""

from __future__ import annotations

import random
from typing import Dict, List, Optional, Tuple
from synthetic_streaming.config import SourceSystemWeights


# Valid mapping between source systems and Use Chip payment methods
SOURCE_TO_USE_CHIP_MAP: Dict[str, List[Tuple[str, float]]] = {
    "POS_TERMINAL": [("Chip Transaction", 0.65), ("Swipe Transaction", 0.35)],
    "ECOMMERCE_GATEWAY": [("Online Transaction", 1.0)],
    "ATM_NETWORK": [("Chip Transaction", 0.60), ("Swipe Transaction", 0.40)],
    "DIGITAL_WALLET": [("Chip Transaction", 0.75), ("Online Transaction", 0.25)],
    "ISSUER_PROCESSOR": [("Chip Transaction", 0.50), ("Swipe Transaction", 0.50)],
    "RECURRING_BILLING": [("Online Transaction", 1.0)],
}


class SourceSimulator:
    """Selects source systems and compatible payment interaction methods according to configured distributions."""

    def __init__(self, weights: Optional[SourceSystemWeights] = None, rng: Optional[random.Random] = None):
        self._weights = weights or SourceSystemWeights()
        self._rng = rng or random.Random()
        self._sources = [
            "POS_TERMINAL",
            "ECOMMERCE_GATEWAY",
            "ATM_NETWORK",
            "DIGITAL_WALLET",
            "ISSUER_PROCESSOR",
            "RECURRING_BILLING",
        ]
        self._source_probabilities = [
            self._weights.pos_terminal,
            self._weights.ecommerce_gateway,
            self._weights.atm_network,
            self._weights.digital_wallet,
            self._weights.issuer_processor,
            self._weights.recurring_billing,
        ]

    def sample_source_and_method(self) -> Tuple[str, str]:
        """Sample an originating source system and a compatible Use Chip value.
        
        Returns:
            Tuple of (source_system, use_chip) where source_system is stored in
            event envelope metadata, and use_chip is one of the original 3 values.
        """
        source = self._rng.choices(self._sources, weights=self._source_probabilities, k=1)[0]
        methods_and_weights = SOURCE_TO_USE_CHIP_MAP[source]
        methods = [m[0] for m in methods_and_weights]
        weights = [m[1] for m in methods_and_weights]
        use_chip = self._rng.choices(methods, weights=weights, k=1)[0]
        return source, use_chip

    @staticmethod
    def is_valid_combination(source_system: str, use_chip: str) -> bool:
        """Validate if a given source system and Use Chip value form a recognized combination."""
        allowed = SOURCE_TO_USE_CHIP_MAP.get(source_system)
        if not allowed:
            return False
        return any(m[0] == use_chip for m in allowed)
