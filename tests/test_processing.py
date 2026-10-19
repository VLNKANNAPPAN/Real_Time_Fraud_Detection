import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from random import Random

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from fraud_engine.db import Base, DecisionRow, TransactionRow
from fraud_engine.decision import DecisionPolicy
from fraud_engine.events import TransactionEvent
from fraud_engine.features import FeatureOutcome
from fraud_engine.inference import Prediction
from fraud_engine.kafka_io import process_fraud_record
from fraud_engine.processing import FraudProcessor, SourceMetadata
from fraud_engine.resilience import (
    NonRetryableIdentityConflict,
    RetryableDatabaseError,
    RetryableFeatureError,
    RetryableKafkaError,
    RetryableRedisError,
    RetryPolicy,
)

SNAPSHOT = {
    "transaction_count_5m": 0,
    "transaction_count_1h": 0,
    "average_amount_24h": 0.0,
    "has_amount_history_24h": 0,
    "amount_vs_user_average_24h": 0.0,
    "unique_devices_24h": 0,
    "is_new_device_24h": 1,
    "unique_countries_24h": 0,
    "seconds_since_previous_transaction": -1,
    "has_previous_transaction": 0,
}


def event(*, event_id="e1", transaction_id="t1", user_id="u1", amount=100):
    now = datetime(2025, 1, 1, tzinfo=UTC)
    return TransactionEvent(
        event_id=event_id,
        transaction_id=transaction_id,
        user_id=user_id,
        event_timestamp=now,
        producer_timestamp=now,
        amount=amount,
        merchant_category="fuel",
        country="IN",
        device_id="d1",
        payment_method="card",
    )


class FakeFeatures:
    def __init__(self, *, redis_error=False):
        self.initialized = set()
        self.snapshots = {}
        self.calls = 0
        self.restore_calls = 0
        self.redis_error = redis_error
        self.lock = threading.Lock()

    def is_initialized(self, user_id):
        if self.redis_error:
            raise RedisConnectionError("down")
        return user_id in self.initialized

    def restore_completed(self, user_id, events):
        self.restore_calls += 1
        self.initialized.add(user_id)

    def process(self, item):
        if self.redis_error:
            raise RedisConnectionError("down")
        with self.lock:
            self.calls += 1
            duplicate = item.event_id in self.snapshots
            snapshot = self.snapshots.setdefault(item.event_id, dict(SNAPSHOT))
        return FeatureOutcome("SCORED", snapshot, duplicate)


class FakePredictor:
    def __init__(self, *, error=None, sleep_seconds=0):
        self.error = error
        self.sleep_seconds = sleep_seconds

    def predict(self, item, snapshot, feature_version):
        if self.sleep_seconds:
            time.sleep(self.sleep_seconds)
        if self.error:
            raise self.error
        return Prediction(0.01, "model-test", feature_version, 1.25)


def sessions(tmp_path=None):
    if tmp_path is None:
        engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
    else:
        engine = create_engine(
            f"sqlite:///{tmp_path / 'processor.db'}",
            connect_args={"check_same_thread": False},
        )
    Base.metadata.create_all(engine)
    return sessionmaker(engine, expire_on_commit=False)


def processor(factory, features=None, predictor=None, **kwargs):
    return FraudProcessor(
        factory,
        features or FakeFeatures(),
        predictor or FakePredictor(),
        DecisionPolicy.load(Path("config/decision-policy-v2.json")),
        **kwargs,
    )


SOURCE = SourceMetadata("transactions.raw", 1, 42)


def test_duplicate_delivery_creates_one_decision_and_does_not_repeat_features():
    factory = sessions()
    features = FakeFeatures()
    service = processor(factory, features)
    first = service.process(event(), SOURCE)
    second = service.process(event(), SOURCE)
    assert (first.outcome, second.outcome) == ("DECIDED", "DUPLICATE")
    assert features.calls == 1
    with factory() as session:
        assert session.scalar(select(func.count()).select_from(DecisionRow)) == 1
        row = session.get(TransactionRow, "t1")
        assert row.status == "DECIDED"
        assert row.attempt_count == 2


def test_crash_after_redis_update_reuses_snapshot_on_retry():
    factory = sessions()
    features = FakeFeatures()
    crashed = False

    def fault(stage):
        nonlocal crashed
        if stage == "redis_updated" and not crashed:
            crashed = True
            raise RuntimeError("simulated process death")

    service = processor(factory, features, fault_hook=fault)
    with pytest.raises(RuntimeError, match="simulated"):
        service.process(event(), SOURCE)
    with factory() as session:
        assert session.get(TransactionRow, "t1").status == "PENDING"
        assert session.scalar(select(func.count()).select_from(DecisionRow)) == 0
    result = service.process(event(), SOURCE)
    assert result.outcome == "DECIDED"
    assert features.calls == 2
    assert len(features.snapshots) == 1


@pytest.mark.parametrize("crash_stage", ["claim_committed", "decision_ready", "before_db_commit"])
def test_crash_before_feature_or_after_inference_recovers(crash_stage):
    factory = sessions()
    features = FakeFeatures()
    crashed = False

    def fault(stage):
        nonlocal crashed
        if stage == crash_stage and not crashed:
            crashed = True
            raise RuntimeError(f"crash at {stage}")

    service = processor(factory, features, fault_hook=fault)
    with pytest.raises(RuntimeError, match="crash at"):
        service.process(event(), SOURCE)
    recovered = service.process(event(), SOURCE)
    assert recovered.outcome == "DECIDED"
    assert len(features.snapshots) == 1
    assert features.calls == (1 if crash_stage == "claim_committed" else 2)


def test_crash_after_db_commit_short_circuits_on_retry():
    factory = sessions()
    features = FakeFeatures()
    crashed = False

    def fault(stage):
        nonlocal crashed
        if stage == "db_committed" and not crashed:
            crashed = True
            raise RuntimeError("offset was not committed")

    service = processor(factory, features, fault_hook=fault)
    with pytest.raises(RuntimeError, match="offset"):
        service.process(event(), SOURCE)
    result = service.process(event(), SOURCE)
    assert result.outcome == "DUPLICATE"
    assert features.calls == 1


def test_identity_reuse_with_changed_payload_is_quarantinable():
    factory = sessions()
    service = processor(factory)
    service.process(event(), SOURCE)
    with pytest.raises(NonRetryableIdentityConflict):
        service.process(event(amount=999), SOURCE)


def test_event_and_transaction_identity_conflicts_are_independent():
    first_factory = sessions()
    first = processor(first_factory)
    first.process(event(), SOURCE)
    with pytest.raises(NonRetryableIdentityConflict):
        first.process(event(transaction_id="t2"), SOURCE)

    second_factory = sessions()
    second = processor(second_factory)
    second.process(event(), SOURCE)
    with pytest.raises(NonRetryableIdentityConflict):
        second.process(event(event_id="e2"), SOURCE)


def test_late_event_is_reviewed_without_inference_or_feature_inclusion():
    class LateFeatures(FakeFeatures):
        def process(self, item):
            self.calls += 1
            return FeatureOutcome("LATE_EVENT", None)

    factory = sessions()
    result = processor(factory, LateFeatures()).process(event(), SOURCE)
    assert result.action == "REVIEW"
    assert result.reason_codes == ("LATE_EVENT",)
    with factory() as session:
        decision = session.scalar(select(DecisionRow))
        assert decision.score is None
        assert decision.model_version is None
        assert decision.feature_included is False


def test_two_consumers_racing_same_transaction_create_one_decision(tmp_path):
    factory = sessions(tmp_path)
    features = FakeFeatures()
    service_a = processor(factory, features)
    service_b = processor(factory, features)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(lambda service: service.process(event(), SOURCE), [service_a, service_b])
        )
    assert {result.outcome for result in results} == {"DECIDED", "DUPLICATE"}
    with factory() as session:
        assert session.scalar(select(func.count()).select_from(DecisionRow)) == 1


def test_redis_outage_leaves_pending_and_is_retryable():
    factory = sessions()
    service = processor(factory, FakeFeatures(redis_error=True))
    with pytest.raises(RetryableRedisError):
        service.process(event(), SOURCE)
    with factory() as session:
        assert session.get(TransactionRow, "t1").status == "PENDING"
        assert session.scalar(select(func.count()).select_from(DecisionRow)) == 0


def test_database_unavailable_is_retryable_before_any_offset_commit():
    missing_schema_factory = sessionmaker(create_engine("sqlite://"), expire_on_commit=False)
    with pytest.raises(RetryableDatabaseError, match="PostgreSQL"):
        processor(missing_schema_factory).process(event(), SOURCE)


def test_unknown_feature_failure_is_explicitly_retryable():
    class BrokenFeatures(FakeFeatures):
        def process(self, item):
            raise ValueError("invalid feature payload")

    factory = sessions()
    with pytest.raises(RetryableFeatureError, match="feature calculation"):
        processor(factory, BrokenFeatures()).process(event(), SOURCE)
    with factory() as session:
        assert session.get(TransactionRow, "t1").status == "PENDING"


def test_retry_policy_is_bounded_and_jittered():
    policy = RetryPolicy(initial_seconds=1, maximum_seconds=4, jitter_fraction=0.2)
    delays = [policy.delay(attempt, random=Random(17)) for attempt in range(1, 7)]
    assert 0.8 <= delays[0] <= 1.2
    assert 1.6 <= delays[1] <= 2.4
    assert all(3.2 <= delay <= 4.8 for delay in delays[2:])


@pytest.mark.parametrize(
    ("predictor", "reason"),
    [
        (FakePredictor(error=OSError("missing")), "MODEL_UNAVAILABLE"),
        (FakePredictor(sleep_seconds=0.05), "MODEL_TIMEOUT"),
    ],
)
def test_model_failure_and_timeout_persist_review(predictor, reason):
    factory = sessions()
    result = processor(factory, predictor=predictor, inference_timeout_seconds=0.005).process(
        event(), SOURCE
    )
    assert result.action == "REVIEW"
    with factory() as session:
        row = session.scalar(select(DecisionRow))
        assert row.reason_codes == [reason]
        assert row.score is None
        assert row.model_version is None
        assert row.inference_status == "FAILED"


def test_prediction_contract_error_is_not_mislabeled_as_model_outage():
    class IncompatiblePredictor:
        def predict(self, item, snapshot, feature_version):
            return Prediction(0.01, "bad-model", "incompatible-features", 1.0)

    service = processor(sessions(), predictor=IncompatiblePredictor())
    with pytest.raises(ValueError, match="feature versions differ"):
        service.process(event(), SOURCE)


class Message:
    def __init__(self, value):
        self._value = value

    def value(self):
        return self._value

    def key(self):
        return b"u1"

    def topic(self):
        return "transactions.raw"

    def partition(self):
        return 0

    def offset(self):
        return 7


class Consumer:
    def __init__(self):
        self.commits = 0

    def commit(self, **kwargs):
        self.commits += 1


class Producer:
    def __init__(self):
        self.records = []

    def produce(self, topic, **kwargs):
        self.records.append((topic, kwargs))

    def flush(self, timeout):
        return 0


class FailedProducer(Producer):
    def flush(self, timeout):
        return 1


def test_kafka_commits_only_terminal_or_acknowledged_dlq():
    factory = sessions()
    consumer = Consumer()
    producer = Producer()
    valid = Message(event().canonical_bytes())
    assert process_fraud_record(valid, consumer, producer, processor(factory)) == "DECIDED"
    assert consumer.commits == 1
    malformed = Message(b"not-json")
    assert process_fraud_record(malformed, consumer, producer, processor(factory)) == "QUARANTINED"
    assert consumer.commits == 2
    assert len(producer.records) == 1


def test_kafka_does_not_commit_retryable_failure():
    factory = sessions()
    consumer = Consumer()
    result = process_fraud_record(
        Message(event().canonical_bytes()),
        consumer,
        Producer(),
        processor(factory, FakeFeatures(redis_error=True)),
    )
    assert result == "RETRY"
    assert consumer.commits == 0


def test_kafka_does_not_commit_when_dlq_ack_fails():
    consumer = Consumer()
    with pytest.raises(RetryableKafkaError, match="DLQ"):
        process_fraud_record(
            Message(b"not-json"), consumer, FailedProducer(), processor(sessions())
        )
    assert consumer.commits == 0
