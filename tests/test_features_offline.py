from datetime import UTC, datetime, timedelta

import pytest

from fraud_engine.events import TransactionEvent
from fraud_engine.features import IdentityConflict, OfflineFeatureEngine

T = datetime(2025, 1, 1, tzinfo=UTC)


def event(n: int, at: datetime, *, user: str = "u", device: str = "d") -> TransactionEvent:
    return TransactionEvent(
        event_id=f"e{n}",
        transaction_id=f"t{n}",
        user_id=user,
        event_timestamp=at,
        producer_timestamp=at,
        amount=100 + n,
        merchant_category="groceries",
        country="IN",
        device_id=device,
        payment_method="card",
    )


def test_current_excluded_window_boundaries_and_duplicate():
    engine = OfflineFeatureEngine()
    first = event(1, T)
    assert engine.process(first).snapshot["transaction_count_5m"] == 0
    assert engine.process(first).duplicate
    boundary = engine.process(event(2, T + timedelta(minutes=5)))
    assert boundary.snapshot["transaction_count_5m"] == 1
    assert boundary.snapshot["transaction_count_1h"] == 1
    assert engine.process(event(3, T + timedelta(minutes=5))).snapshot["transaction_count_5m"] == 1


def test_inbound_out_of_order_and_late():
    engine = OfflineFeatureEngine()
    engine.process(event(1, T))
    engine.process(event(2, T + timedelta(minutes=5)))
    inbound = engine.process(event(3, T + timedelta(minutes=4)))
    assert inbound.snapshot["transaction_count_5m"] == 1
    late = engine.process(event(4, T + timedelta(minutes=2, seconds=59)))
    assert late.status == "LATE_EVENT"
    assert late.snapshot is None
    assert engine.process(event(5, T + timedelta(minutes=6))).snapshot["transaction_count_5m"] == 2


def test_identity_conflicts():
    engine = OfflineFeatureEngine()
    original = event(1, T)
    engine.process(original)
    with pytest.raises(IdentityConflict):
        engine.process(original.model_copy(update={"amount": 999}))
    with pytest.raises(IdentityConflict):
        engine.process(original.model_copy(update={"event_id": "other"}))
