"""Run with FRAUD_INTEGRATION=1 after `docker compose up -d postgres redis kafka`."""

import json
import os
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient
from redis import Redis
from sqlalchemy import delete, inspect, select
from sqlalchemy.orm import Session

from fraud_engine.config import get_settings
from fraud_engine.db import DecisionRow, TransactionRow, make_engine, session_factory
from fraud_engine.decision import DecisionPolicy, decide
from fraud_engine.events import TransactionEvent
from fraud_engine.features import OfflineFeatureEngine
from fraud_engine.inference import Prediction
from fraud_engine.kafka_io import process_probe_record, produce_event
from fraud_engine.persistence import save_decision
from fraud_engine.processing import FraudProcessor, SourceMetadata
from fraud_engine.redis_features import RedisFeatureEngine

pytestmark = pytest.mark.integration
if os.getenv("FRAUD_INTEGRATION") != "1":
    pytest.skip("set FRAUD_INTEGRATION=1 for live containers", allow_module_level=True)


def event(n: int, user: str, at: datetime) -> TransactionEvent:
    return TransactionEvent(
        event_id=f"{user}-e{n}",
        transaction_id=f"{user}-t{n}",
        user_id=user,
        event_timestamp=at,
        producer_timestamp=at,
        amount=100 + n,
        merchant_category="fuel",
        country="IN",
        device_id="d",
        payment_method="card",
    )


def test_postgres_migration_and_redis_feature_parity():
    engine = make_engine()
    tables = set(inspect(engine).get_table_names())
    assert {"alembic_version", "transactions", "decisions", "review_labels"} <= tables
    user = f"integration-{uuid.uuid4().hex}"
    online = RedisFeatureEngine(Redis.from_url(get_settings().redis_url))
    offline = OfflineFeatureEngine()
    start = datetime(2025, 1, 1, tzinfo=UTC)
    events = [event(1, user, start), event(2, user, start + timedelta(minutes=5))]
    try:
        for item in events:
            assert online.process(item).snapshot == pytest.approx(offline.process(item).snapshot)
        assert online.process(events[1]).duplicate
        online.restore_completed(user, events[:1])
        assert online.process(events[1]).snapshot == pytest.approx(
            offline.process(events[1]).snapshot
        )
    finally:
        for item in events:
            online.redis.delete(*online._keys(user, item.event_id))


def test_postgres_persists_explainable_decision_transactionally():
    engine = make_engine()
    item = event(1, f"db-{uuid.uuid4().hex}", datetime.now(UTC))
    snapshot = {
        "is_new_device_24h": 0,
        "transaction_count_5m": 0,
        "has_amount_history_24h": 0,
        "amount_vs_user_average_24h": 0.0,
    }
    policy = DecisionPolicy.load(Path("config/decision-policy-v2.json"))
    decision = decide(
        item,
        snapshot,
        Prediction(policy.review_score, "fraud-lr-balanced-v2+test", "features-v2", 1.0),
        policy,
    )
    with Session(engine) as session:
        save_decision(session, item, decision, snapshot, feature_included=True)
        stored = session.scalar(
            select(DecisionRow).where(DecisionRow.transaction_id == item.transaction_id)
        )
        assert stored.reason_codes == ["MODEL_REVIEW_BAND"]
        assert stored.model_version == "fraud-lr-balanced-v2+test"
        session.rollback()  # Leaves the integration database clean.


def _new_consumer(group: str, topic: str, *, from_end: bool = False) -> Consumer:
    metadata = AdminClient(
        {"bootstrap.servers": get_settings().kafka_bootstrap_servers}
    ).list_topics(topic=topic, timeout=10)
    partition_count = len(metadata.topics[topic].partitions)
    consumer = Consumer(
        {
            "bootstrap.servers": get_settings().kafka_bootstrap_servers,
            "group.id": group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([topic])
    for _ in range(80):
        consumer.poll(0.25)
        if len(consumer.assignment()) == partition_count:
            if from_end:
                positions = []
                for assigned in consumer.assignment():
                    _, high = consumer.get_watermark_offsets(assigned, timeout=5, cached=False)
                    positions.append(TopicPartition(topic, assigned.partition, high))
                # Explicit assignment clears any records fetched while the subscription was
                # joining the group; seek alone can leave those stale records queued locally.
                consumer.assign(positions)
            return consumer
    consumer.close()
    raise AssertionError("consumer was not assigned a Kafka partition")


def _next(consumer: Consumer):
    for _ in range(40):
        message = consumer.poll(0.25)
        if message is not None and not message.error():
            return message
    raise AssertionError("Kafka message not received")


def _next_event(consumer: Consumer, event_id: str):
    for _ in range(120):
        record = _next(consumer)
        try:
            if json.loads(record.value()).get("event_id") == event_id:
                return record
        except (ValueError, AttributeError):
            pass
    raise AssertionError(f"target event {event_id} not received")


def test_kafka_key_order_restart_and_dlq():
    settings = get_settings()
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers, "acks": "all"})
    group = f"integration-{uuid.uuid4().hex}"
    consumer = _new_consumer(group, settings.kafka_raw_topic, from_end=True)
    user = f"integration-{uuid.uuid4().hex}"
    now = datetime.now(UTC)
    first = event(1, user, now)
    second = event(2, user, now + timedelta(seconds=1))
    try:
        produce_event(producer, first)
        produce_event(producer, second)
        first_record = _next_event(consumer, first.event_id)
        second_record = _next_event(consumer, second.event_id)
        assert json.loads(first_record.value())["event_id"] == first.event_id
        assert json.loads(second_record.value())["event_id"] == second.event_id
        assert first_record.key() == second_record.key() == user.encode()
        assert first_record.partition() == second_record.partition()
        assert first_record.offset() < second_record.offset()
        process_probe_record(first_record, consumer, producer, lambda _: None)
        process_probe_record(second_record, consumer, producer, lambda _: None)
    finally:
        consumer.close()

    # This test topic is shared across repeated local runs. Seek all assignments to
    # their current end so uncommitted history on unrelated partitions cannot hide
    # the new target record; committed() below still reads the group's durable offsets.
    restarted = _new_consumer(group, settings.kafka_raw_topic, from_end=True)
    committed = restarted.committed(restarted.assignment(), timeout=5)
    assert any(
        item.partition == second_record.partition() and item.offset >= second_record.offset() + 1
        for item in committed
    )
    dlq_consumer = _new_consumer(
        f"integration-dlq-{uuid.uuid4().hex}", settings.kafka_dlq_topic, from_end=True
    )
    third = event(3, user, now + timedelta(seconds=2))
    try:
        produce_event(producer, third)
        record = _next_event(restarted, third.event_id)
        assert json.loads(record.value())["event_id"] == third.event_id
        process_probe_record(record, restarted, producer, lambda _: None)
        producer.produce(settings.kafka_raw_topic, key=user.encode(), value=b"broken-json")
        assert producer.flush(timeout=10) == 0
        malformed = _next(restarted)
        while malformed.value() != b"broken-json":
            malformed = _next(restarted)
        assert process_probe_record(malformed, restarted, producer, lambda _: None) == "QUARANTINED"
        quarantine = json.loads(_next(dlq_consumer).value())
        while quarantine["source_offset"] != malformed.offset():
            quarantine = json.loads(_next(dlq_consumer).value())
        assert quarantine["source_offset"] == malformed.offset()
        assert quarantine["source_partition"] == malformed.partition()
    finally:
        restarted.close()
        dlq_consumer.close()


def test_multiple_users_distribute_across_partitions():
    settings = get_settings()
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers, "acks": "all"})
    consumer = _new_consumer(
        f"integration-users-{uuid.uuid4().hex}", settings.kafka_raw_topic, from_end=True
    )
    now = datetime.now(UTC)
    prefix = uuid.uuid4().hex
    events = [event(i, f"{prefix}-user-{i}", now) for i in range(15)]
    try:
        for item in events:
            producer.produce(
                settings.kafka_raw_topic, key=item.user_id.encode(), value=item.canonical_bytes()
            )
        assert producer.flush(timeout=15) == 0
        targets = {item.event_id for item in events}
        records = []
        for _ in range(200):
            record = _next(consumer)
            try:
                event_id = json.loads(record.value()).get("event_id")
            except (ValueError, AttributeError):
                continue
            if event_id in targets:
                records.append(record)
                targets.remove(event_id)
            if not targets:
                break
        assert not targets
        assert len({record.partition() for record in records}) >= 2
    finally:
        consumer.close()


def test_live_processor_recovers_after_redis_update_before_db_decision():
    class Predictor:
        def predict(self, item, snapshot, feature_version):
            return Prediction(0.01, "integration-model-v1", feature_version, 0.5)

    user = f"processor-{uuid.uuid4().hex}"
    item = event(1, user, datetime.now(UTC))
    redis_client = Redis.from_url(get_settings().redis_url)
    features = RedisFeatureEngine(redis_client)
    policy = DecisionPolicy.load(Path("config/decision-policy-v2.json"))
    crashed = False

    def fault(stage):
        nonlocal crashed
        if stage == "redis_updated" and not crashed:
            crashed = True
            raise RuntimeError("simulated crash after Redis")

    processor = FraudProcessor(session_factory(), features, Predictor(), policy, fault_hook=fault)
    source = SourceMetadata("transactions.raw", 0, 123)
    try:
        with pytest.raises(RuntimeError, match="simulated crash"):
            processor.process(item, source)
        with Session(make_engine()) as session:
            assert session.get(TransactionRow, item.transaction_id).status == "PENDING"
        recovered = FraudProcessor(session_factory(), features, Predictor(), policy).process(
            item, source
        )
        assert recovered.outcome == "DECIDED"
        duplicate = FraudProcessor(session_factory(), features, Predictor(), policy).process(
            item, source
        )
        assert duplicate.outcome == "DUPLICATE"
        with Session(make_engine()) as session:
            stored = session.scalar(
                select(DecisionRow).where(DecisionRow.transaction_id == item.transaction_id)
            )
            assert stored.model_version == "integration-model-v1"
            assert stored.feature_snapshot is not None
    finally:
        redis_client.delete(*features._keys(user, item.event_id))
        with Session(make_engine()) as session:
            session.execute(
                delete(DecisionRow).where(DecisionRow.transaction_id == item.transaction_id)
            )
            session.execute(
                delete(TransactionRow).where(TransactionRow.transaction_id == item.transaction_id)
            )
            session.commit()
