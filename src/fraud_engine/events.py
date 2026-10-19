"""Immutable v1 event contract shared by simulator, producer and feature engine."""

import hashlib
import json
from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class TransactionEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: str = Field(min_length=1, max_length=128)
    transaction_id: str = Field(min_length=1, max_length=128)
    user_id: str = Field(min_length=1, max_length=128)
    event_version: Literal[1] = 1
    event_timestamp: datetime
    producer_timestamp: datetime
    amount: int = Field(gt=0)
    currency: Literal["INR"] = "INR"
    merchant_category: str = Field(min_length=1)
    country: str = Field(min_length=2, max_length=2)
    device_id: str = Field(min_length=1)
    payment_method: str = Field(min_length=1)
    account_age_days: int = Field(default=0, ge=0)

    @field_validator("event_timestamp", "producer_timestamp")
    @classmethod
    def require_aware_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must include timezone")
        return value.astimezone(UTC)

    def canonical_bytes(self) -> bytes:
        payload = self.model_dump(mode="json")
        return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")

    def digest(self) -> str:
        return hashlib.sha256(self.canonical_bytes()).hexdigest()


def parse_event(raw: bytes) -> TransactionEvent:
    return TransactionEvent.model_validate_json(raw)
