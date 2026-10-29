"""Validation of entity relationships, foreign keys, and business constraints."""

from __future__ import annotations

import re
from typing import Any, Dict, Optional, Set, Tuple
from synthetic_streaming.source_simulator import SourceSimulator


class RelationshipValidationError(Exception):
    """Raised when an entity relationship or foreign-key constraint is violated."""
    pass


class RelationshipValidator:
    """Validates that stream events obey strict relational integrity and schema rules."""

    def __init__(self):
        self.known_users: Set[int] = set()
        self.known_cards: Set[Tuple[int, int]] = set()

    def register_user(self, user_id: int) -> None:
        self.known_users.add(user_id)

    def register_card(self, user_id: int, card_index: int) -> None:
        self.known_users.add(user_id)
        self.known_cards.add((user_id, card_index))

    def validate_user_event(self, envelope: Dict[str, Any]) -> None:
        payload = envelope.get("payload", {})
        user_id = payload.get("User")
        if user_id is None:
            raise RelationshipValidationError("User event payload missing 'User' ID")
        if not isinstance(user_id, int):
            raise RelationshipValidationError(f"User ID must be integer, got {type(user_id)}")

    def validate_card_event(self, envelope: Dict[str, Any]) -> None:
        payload = envelope.get("payload", {})
        user_id = payload.get("User")
        card_index = payload.get("CARD INDEX")
        if user_id is None or card_index is None:
            raise RelationshipValidationError("Card event missing 'User' or 'CARD INDEX'")
        if user_id not in self.known_users:
            raise RelationshipValidationError(f"Card references unknown user {user_id}")

    def validate_transaction_event(self, envelope: Dict[str, Any]) -> None:
        """Validate transaction payload references valid user and card owned by that user."""
        scenario = envelope.get("scenario")
        # Allow intentional invalid test records if marked
        if scenario == "invalid_test_record":
            return

        payload = envelope.get("payload", {})
        user_id = payload.get("User")
        card_index = payload.get("Card")
        amount = payload.get("Amount")
        use_chip = payload.get("Use Chip")
        source_system = envelope.get("source_system")

        if user_id is None or card_index is None:
            raise RelationshipValidationError("Transaction missing User or Card index")

        if user_id not in self.known_users:
            raise RelationshipValidationError(f"Transaction references non-existent User {user_id}")

        if (user_id, card_index) not in self.known_cards:
            raise RelationshipValidationError(
                f"Transaction references invalid card ({user_id}, {card_index}). Card not registered for user."
            )

        # Validate amount format
        if not isinstance(amount, str) or not re.match(r"^\$-?\d+\.\d{2}$", amount):
            raise RelationshipValidationError(f"Invalid amount format: '{amount}'")

        # Validate source system and Use Chip combination
        if source_system and use_chip:
            if not SourceSimulator.is_valid_combination(source_system, use_chip):
                raise RelationshipValidationError(
                    f"Incompatible source_system '{source_system}' and Use Chip '{use_chip}'"
                )
