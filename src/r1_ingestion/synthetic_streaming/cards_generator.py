"""Synthetic card generator ensuring composite key uniqueness and non-routable PANs."""

from __future__ import annotations

import random
import threading
from typing import Dict, List, Optional, Set, Tuple
from synthetic_streaming.event_schema import CardPayload, EventEnvelope


CARD_BRANDS = ["Mastercard", "Visa", "Amex", "Discover"]
CARD_BRAND_WEIGHTS = [0.52, 0.38, 0.065, 0.035]

CARD_TYPES = ["Debit", "Credit", "Debit (Prepaid)"]
CARD_TYPE_WEIGHTS = [0.57, 0.33, 0.10]


def generate_synthetic_pan(brand: str, rng: random.Random) -> str:
    """Generate non-routable synthetic test PAN according to brand specification."""
    if brand == "Visa":
        prefix = "400000"
        remainder = "".join(str(rng.randint(0, 9)) for _ in range(10))
        return prefix + remainder  # 16 digits
    elif brand == "Mastercard":
        prefix = "510000"
        remainder = "".join(str(rng.randint(0, 9)) for _ in range(10))
        return prefix + remainder  # 16 digits
    elif brand == "Amex":
        prefix = "370000"
        remainder = "".join(str(rng.randint(0, 9)) for _ in range(9))
        return prefix + remainder  # 15 digits
    else:  # Discover
        prefix = "601100"
        remainder = "".join(str(rng.randint(0, 9)) for _ in range(10))
        return prefix + remainder  # 16 digits


class CardsGenerator:
    """Thread-safe generator for synthetic credit/debit cards preserving composite key constraints."""

    def __init__(self, rng: Optional[random.Random] = None):
        self._lock = threading.Lock()
        self._rng = rng or random.Random()
        self.cards_by_key: Dict[Tuple[int, int], CardPayload] = {}
        self.cards_by_user: Dict[int, List[CardPayload]] = {}

    def generate_cards_for_user(
        self,
        user_id: int,
        num_cards: int,
        run_id: str = "run-001",
        source_system: str = "ISSUER_PROCESSOR"
    ) -> List[Tuple[CardPayload, EventEnvelope]]:
        """Generate contiguous cards (index 0..N-1) for a user and return payloads + event envelopes."""
        results: List[Tuple[CardPayload, EventEnvelope]] = []

        with self._lock:
            for card_index in range(num_cards):
                brand = self._rng.choices(CARD_BRANDS, weights=CARD_BRAND_WEIGHTS, k=1)[0]
                card_type = self._rng.choices(CARD_TYPES, weights=CARD_TYPE_WEIGHTS, k=1)[0]
                pan = generate_synthetic_pan(brand, self._rng)
                cvv = f"{self._rng.randint(100, 999)}"
                
                exp_month = self._rng.randint(1, 12)
                exp_year = self._rng.randint(2027, 2031)
                expires = f"{exp_month:02d}/{exp_year}"

                open_month = self._rng.randint(1, 12)
                open_year = self._rng.randint(2015, 2025)
                acct_open = f"{open_month:02d}/{open_year}"

                has_chip = "YES" if self._rng.random() < 0.90 else "NO"
                cards_issued = self._rng.choices([1, 2, 3], weights=[0.51, 0.48, 0.01], k=1)[0]
                credit_limit = f"${self._rng.randint(2, 50) * 1000}"
                pin_year = self._rng.randint(open_year, 2026)

                payload_dict = {
                    "User": user_id,
                    "CARD INDEX": card_index,
                    "Card Brand": brand,
                    "Card Type": card_type,
                    "Card Number": pan,
                    "Expires": expires,
                    "CVV": cvv,
                    "Has Chip": has_chip,
                    "Cards Issued": cards_issued,
                    "Credit Limit": credit_limit,
                    "Acct Open Date": acct_open,
                    "Year PIN last Changed": pin_year,
                    "Card on Dark Web": "No",
                }

                card_payload = CardPayload(**payload_dict)
                self.cards_by_key[(user_id, card_index)] = card_payload
                if user_id not in self.cards_by_user:
                    self.cards_by_user[user_id] = []
                self.cards_by_user[user_id].append(card_payload)

                envelope = EventEnvelope(
                    event_type="card.created",
                    source_system=source_system,
                    run_id=run_id,
                    payload=payload_dict,
                )
                results.append((card_payload, envelope))

        return results

    def get_cards_for_user(self, user_id: int) -> List[CardPayload]:
        with self._lock:
            return list(self.cards_by_user.get(user_id, []))

    def get_card(self, user_id: int, card_index: int) -> Optional[CardPayload]:
        with self._lock:
            return self.cards_by_key.get((user_id, card_index))
