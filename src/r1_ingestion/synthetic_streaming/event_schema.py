"""Versioned event envelopes and schemas for users, cards, and transactions."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Optional, Union
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


def current_utc_timestamp() -> str:
    """Return timezone-aware RFC 3339 / ISO 8601 UTC timestamp."""
    return datetime.now(timezone.utc).isoformat()


def generate_event_id() -> str:
    """Generate a stable unique event identifier."""
    return str(uuid.uuid4())


class DecimalEncoder(json.JSONEncoder):
    """JSON encoder supporting Decimal instances."""
    def default(self, obj: Any) -> Any:
        if isinstance(obj, Decimal):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class UserPayload(BaseModel):
    """Payload representing a synthetic user matching sd254_users schema."""
    model_config = ConfigDict(populate_by_name=True)

    User: int = Field(..., description="Implicit or explicit User ID")
    Person: str = Field(..., description="Synthetic person name")
    Current_Age: int = Field(..., alias="Current Age", ge=18, le=120)
    Retirement_Age: int = Field(..., alias="Retirement Age", ge=40, le=100)
    Birth_Year: int = Field(..., alias="Birth Year", ge=1900, le=2026)
    Birth_Month: int = Field(..., alias="Birth Month", ge=1, le=12)
    Gender: str = Field(..., description="Gender (Female/Male)")
    Address: str = Field(..., description="Synthetic street address")
    Apartment: Optional[str] = Field(None, description="Apartment or unit, nullable")
    City: str = Field(..., description="City")
    State: str = Field(..., description="US State")
    Zipcode: int = Field(..., description="5-digit zipcode")
    Latitude: float = Field(..., description="Latitude")
    Longitude: float = Field(..., description="Longitude")
    Per_Capita_Income: str = Field(..., alias="Per Capita Income - Zipcode")
    Yearly_Income: str = Field(..., alias="Yearly Income - Person")
    Total_Debt: str = Field(..., alias="Total Debt")
    FICO_Score: int = Field(..., alias="FICO Score", ge=300, le=850)
    Num_Credit_Cards: int = Field(..., alias="Num Credit Cards", ge=1, le=15)


class CardPayload(BaseModel):
    """Payload representing a synthetic card matching sd254_cards schema."""
    model_config = ConfigDict(populate_by_name=True)

    User: int = Field(..., description="User ID owner")
    CARD_INDEX: int = Field(..., alias="CARD INDEX", ge=0)
    Card_Brand: str = Field(..., alias="Card Brand")
    Card_Type: str = Field(..., alias="Card Type")
    Card_Number: str = Field(..., alias="Card Number", description="Synthetic masked or test PAN")
    Expires: str = Field(..., description="Expiration date MM/YYYY")
    CVV: str = Field(..., description="Synthetic CVV")
    Has_Chip: str = Field(..., alias="Has Chip", description="YES or NO")
    Cards_Issued: int = Field(..., alias="Cards Issued", ge=1)
    Credit_Limit: str = Field(..., alias="Credit Limit")
    Acct_Open_Date: str = Field(..., alias="Acct Open Date")
    Year_PIN_last_Changed: int = Field(..., alias="Year PIN last Changed")
    Card_on_Dark_Web: str = Field("No", alias="Card on Dark Web")


class TransactionPayload(BaseModel):
    """Payload representing a transaction matching credit_card_transactions schema."""
    model_config = ConfigDict(populate_by_name=True)

    User: int = Field(..., description="User ID")
    Card: int = Field(..., description="Card index referencing CARD INDEX")
    Year: int = Field(..., ge=1990, le=2050)
    Month: int = Field(..., ge=1, le=12)
    Day: int = Field(..., ge=1, le=31)
    Time: str = Field(..., description="Transaction time HH:MM")
    Amount: str = Field(..., description="Transaction amount string e.g. $12.34 or -$5.00")
    Use_Chip: str = Field(..., alias="Use Chip")
    Merchant_Name: Union[str, int] = Field(..., alias="Merchant Name")
    Merchant_City: str = Field(..., alias="Merchant City")
    Merchant_State: Optional[str] = Field(None, alias="Merchant State")
    Zip: Optional[Union[float, str]] = Field(None, description="Zipcode string or float")
    MCC: int = Field(..., description="Merchant Category Code")
    Errors: Optional[str] = Field(None, alias="Errors?")
    Is_Fraud: str = Field("No", alias="Is Fraud?")
    Transaction_Entry_Method: Optional[str] = Field("unknown", alias="Transaction Entry Method")
    Merchant_Latitude: Optional[float] = Field(None, alias="Merchant Latitude")
    Merchant_Longitude: Optional[float] = Field(None, alias="Merchant Longitude")

    @field_validator("Use_Chip")
    @classmethod
    def validate_use_chip(cls, v: str) -> str:
        valid_methods = {"Swipe Transaction", "Chip Transaction", "Online Transaction"}
        if v not in valid_methods:
            raise ValueError(f"Invalid Use Chip value '{v}'. Must be one of {valid_methods}")
        return v

    @field_validator("Is_Fraud")
    @classmethod
    def validate_fraud(cls, v: str) -> str:
        if v not in {"Yes", "No"}:
            raise ValueError(f"Is Fraud? must be 'Yes' or 'No', got '{v}'")
        return v

    @model_validator(mode="after")
    def validate_entry_method_and_coordinates(self) -> "TransactionPayload":
        from synthetic_streaming.classification_and_geo import (
            is_valid_entry_method,
            validate_coordinates,
        )
        if self.Transaction_Entry_Method is not None:
            if not is_valid_entry_method(self.Use_Chip, self.Transaction_Entry_Method):
                raise ValueError(
                    f"Invalid Transaction Entry Method '{self.Transaction_Entry_Method}' "
                    f"for Use Chip '{self.Use_Chip}'."
                )

        if not validate_coordinates(self.Merchant_Latitude, self.Merchant_Longitude):
            raise ValueError(
                f"Invalid Merchant Coordinates ({self.Merchant_Latitude}, {self.Merchant_Longitude}). "
                f"Latitude must be [-90, 90], longitude [-180, 180], and both must be present or both absent."
            )

        return self


class EventEnvelope(BaseModel):
    """Standard versioned event envelope for stream publication."""
    event_id: str = Field(default_factory=generate_event_id)
    event_type: str = Field(..., description="e.g. transaction.created, user.created, card.created")
    schema_version: int = Field(default=1)
    event_timestamp: str = Field(default_factory=current_utc_timestamp)
    source_system: str = Field(..., description="Simulated originating source system")
    run_id: str = Field(default="default-run")
    scenario: Optional[str] = Field(None, description="Scenario name for transaction events")
    payload: Dict[str, Any] = Field(..., description="Entity payload")

    def to_json(self) -> str:
        """Serialize event to JSON string safely without float precision loss."""
        return json.dumps(self.model_dump(by_alias=True), cls=DecimalEncoder)

    def to_bytes(self) -> bytes:
        """Encode event envelope directly as UTF-8 bytes for Kafka publication."""
        return self.to_json().encode("utf-8")
