from datetime import UTC, datetime, timedelta
from time import sleep

import fakeredis
import pytest

import fraud_engine.redis_features as redis_features
from fraud_engine.events import TransactionEvent
from fraud_engine.features import IdentityConflict, OfflineFeatureEngine
from fraud_engine.redis_features import (
    HistoryLimitExceeded,
    RedisFeatureEngine,
    StateRecoveryRequired,
)

T = datetime(2025, 1, 1, tzinfo=UTC)


def event(n, at, *, device="d", user="u"):
    return TransactionEvent(
        event_id=f"e{n}",
        transaction_id=f"t{n}",
        user_id=user,
        event_timestamp=at,
        producer_timestamp=at,
        amount=100 + n,
        merchant_category="fuel",
        country="IN",
        device_id=device,
        payment_method="card",
    )


def test_redis_matches_offline_for_order_duplicate_and_late():
    online = RedisFeatureEngine(fakeredis.FakeRedis())
    offline = OfflineFeatureEngine()
    sequence = [
        event(1, T),
        event(2, T + timedelta(minutes=5)),
        event(3, T + timedelta(minutes=4)),
        event(2, T + timedelta(minutes=5)),
        event(4, T + timedelta(minutes=2, seconds=59)),
        event(5, T + timedelta(hours=1)),
    ]
    for item in sequence:
        actual = online.process(item)
        expected = offline.process(item)
        assert actual.status == expected.status
        assert actual.duplicate == expected.duplicate
        if expected.snapshot is not None:
            assert actual.snapshot == pytest.approx(expected.snapshot)


def test_identity_conflicts_and_missing_snapshot_require_recovery():
    redis = fakeredis.FakeRedis()
    engine = RedisFeatureEngine(redis)
    original = event(1, T)
    engine.process(original)
    with pytest.raises(IdentityConflict):
        engine.process(original.model_copy(update={"amount": 999}))
    with pytest.raises(IdentityConflict):
        engine.process(original.model_copy(update={"event_id": "other"}))
    redis.delete(engine._keys("u", "e1")[4])
    with pytest.raises(StateRecoveryRequired):
        engine.process(original)


def test_rebuild_after_cache_loss_excludes_pending():
    redis = fakeredis.FakeRedis()
    engine = RedisFeatureEngine(redis)
    first = event(1, T)
    pending = event(2, T + timedelta(minutes=1))
    engine.process(first)
    before = engine.process(pending)
    redis.flushall()  # Simulates Redis restart; only first has a committed DB decision.
    engine.restore_completed("u", [first])
    after = engine.process(pending)
    assert after.snapshot == pytest.approx(before.snapshot)
    assert engine.process(event(3, T + timedelta(minutes=2))).snapshot["transaction_count_5m"] == 2


def test_microsecond_boundary_same_time_and_out_of_order_second():
    online = RedisFeatureEngine(fakeredis.FakeRedis())
    offline = OfflineFeatureEngine()
    sequence = [
        event(1, T + timedelta(microseconds=777)),
        event(2, T + timedelta(minutes=5, microseconds=776)),
        event(3, T + timedelta(minutes=5, microseconds=777)),
        event(4, T + timedelta(minutes=5, microseconds=777)),
        event(5, T + timedelta(minutes=5, seconds=-1, microseconds=777)),
    ]
    for item in sequence:
        actual = online.process(item)
        expected = offline.process(item)
        assert actual.snapshot == pytest.approx(expected.snapshot)
    assert online.process(event(6, T + timedelta(minutes=6))).snapshot["transaction_count_5m"] == 4


def test_burst_limit_prunes_expired_history_before_rejecting():
    online = RedisFeatureEngine(fakeredis.FakeRedis(), max_history=3)
    for n in range(3):
        online.process(event(n, T + timedelta(seconds=n)))
    with pytest.raises(HistoryLimitExceeded):
        online.process(event(3, T + timedelta(seconds=3)))
    fresh = online.process(event(4, T + timedelta(hours=25)))
    assert fresh.snapshot["transaction_count_1h"] == 0


def test_large_user_burst_and_user_isolation():
    online = RedisFeatureEngine(fakeredis.FakeRedis())
    offline = OfflineFeatureEngine()
    for n in range(200):
        item = event(n, T + timedelta(milliseconds=n))
        assert online.process(item).snapshot == pytest.approx(offline.process(item).snapshot)
    assert online.process(event(1000, T, user="another")).snapshot["transaction_count_5m"] == 0


def test_ttl_expiry_requires_rebuild(monkeypatch):
    monkeypatch.setattr(redis_features, "STATE_TTL_SECONDS", 1)
    redis = fakeredis.FakeRedis()
    online = RedisFeatureEngine(redis)
    first = event(1, T)
    online.process(first)
    assert redis.ttl(online._keys("u", "e1")[3]) > 0
    sleep(1.1)
    assert redis.exists(online._keys("u", "e1")[3]) == 0
    online.restore_completed("u", [first])
    assert online.process(event(2, T + timedelta(seconds=1))).snapshot["transaction_count_5m"] == 1
