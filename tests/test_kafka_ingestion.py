from datetime import UTC, datetime
from unittest.mock import Mock

import pytest

from fraud_engine.events import TransactionEvent
from fraud_engine.kafka_io import init_topics_main, process_probe_record, produce_events


def sample() -> TransactionEvent:
    now = datetime.now(UTC)
    return TransactionEvent(
        event_id="e1",
        transaction_id="t1",
        user_id="u1",
        event_timestamp=now,
        producer_timestamp=now,
        amount=100,
        merchant_category="fuel",
        country="IN",
        device_id="d1",
        payment_method="card",
    )


def message(event: TransactionEvent):
    result = Mock()
    result.value.return_value = event.canonical_bytes()
    result.key.return_value = event.user_id.encode()
    result.topic.return_value = "transactions.raw"
    result.partition.return_value = 1
    result.offset.return_value = 2
    return result


def test_valid_commit_after_handler():
    event = sample()
    record = message(event)
    consumer = Mock()
    handler = Mock()
    assert process_probe_record(record, consumer, Mock(), handler) == "VALID"
    handler.assert_called_once_with(event)
    consumer.commit.assert_called_once_with(message=record, asynchronous=False)


def test_handler_failure_does_not_commit():
    record = message(sample())
    consumer = Mock()
    with pytest.raises(RuntimeError):
        process_probe_record(record, consumer, Mock(), Mock(side_effect=RuntimeError("down")))
    consumer.commit.assert_not_called()


def test_malformed_to_dlq_before_commit():
    record = message(sample())
    record.value.return_value = b"not-json"
    producer = Mock()
    producer.flush.return_value = 0
    consumer = Mock()
    assert process_probe_record(record, consumer, producer, Mock()) == "QUARANTINED"
    producer.produce.assert_called_once()
    consumer.commit.assert_called_once()


def test_dlq_failure_keeps_offset_uncommitted():
    record = message(sample())
    record.value.return_value = b"not-json"
    producer = Mock()
    producer.flush.return_value = 1
    consumer = Mock()
    with pytest.raises(RuntimeError):
        process_probe_record(record, consumer, producer, Mock())
    consumer.commit.assert_not_called()


def test_topic_initialization_is_repeatable(monkeypatch):
    admin = Mock()
    admin.list_topics.return_value.topics = {
        "transactions.raw": Mock(),
        "fraud.dead-letter": Mock(),
    }
    monkeypatch.setattr("fraud_engine.kafka_io.AdminClient", Mock(return_value=admin))
    init_topics_main()
    admin.create_topics.assert_not_called()


def test_batch_producer_waits_for_all_acknowledgements():
    producer = Mock()

    def acknowledge(topic, *, key, value, callback):
        callback(None, Mock())

    producer.produce.side_effect = acknowledge
    producer.flush.return_value = 0
    report = produce_events(producer, [sample(), sample().model_copy(update={"event_id": "e2"})])
    assert report["events"] == 2
    assert producer.produce.call_count == 2
    producer.flush.assert_called_once()
