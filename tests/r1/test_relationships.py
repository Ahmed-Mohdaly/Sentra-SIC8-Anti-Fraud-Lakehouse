import pytest
from synthetic_streaming.relationship_validator import (
    RelationshipValidationError,
    RelationshipValidator,
)


def test_relationship_validator_valid():
    validator = RelationshipValidator()
    validator.register_user(10)
    validator.register_card(10, 0)
    validator.register_card(10, 1)

    envelope = {
        "source_system": "POS_TERMINAL",
        "scenario": "normal_spend",
        "payload": {
            "User": 10,
            "Card": 0,
            "Amount": "$25.00",
            "Use Chip": "Chip Transaction",
        },
    }
    validator.validate_transaction_event(envelope)


def test_relationship_validator_missing_user():
    validator = RelationshipValidator()
    envelope = {
        "source_system": "POS_TERMINAL",
        "scenario": "normal_spend",
        "payload": {
            "User": 999,
            "Card": 0,
            "Amount": "$25.00",
            "Use Chip": "Chip Transaction",
        },
    }
    with pytest.raises(RelationshipValidationError, match="non-existent User"):
        validator.validate_transaction_event(envelope)


def test_relationship_validator_unowned_card():
    validator = RelationshipValidator()
    validator.register_user(10)
    validator.register_card(10, 0)
    validator.register_user(20)
    validator.register_card(20, 1)

    envelope = {
        "source_system": "POS_TERMINAL",
        "scenario": "normal_spend",
        "payload": {
            "User": 10,
            "Card": 1,  # Belongs to user 20, not 10
            "Amount": "$25.00",
            "Use Chip": "Chip Transaction",
        },
    }
    with pytest.raises(RelationshipValidationError, match="invalid card"):
        validator.validate_transaction_event(envelope)


def test_relationship_validator_invalid_source_chip_combo():
    validator = RelationshipValidator()
    validator.register_user(10)
    validator.register_card(10, 0)

    envelope = {
        "source_system": "ATM_NETWORK",
        "scenario": "normal_spend",
        "payload": {
            "User": 10,
            "Card": 0,
            "Amount": "$25.00",
            "Use Chip": "Online Transaction",  # ATMs do not support Online Transaction
        },
    }
    with pytest.raises(RelationshipValidationError, match="Incompatible source_system"):
        validator.validate_transaction_event(envelope)
