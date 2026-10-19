"""Kafka transport and a validation-only ingestion probe.

The probe has its own consumer group. It is not the fraud processor and must not be
used to claim a transaction was decided.
"""

import argparse
import json
import logging
import time
from collections import deque
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from pydantic import ValidationError
from redis import Redis

from fraud_engine.config import get_settings
from fraud_engine.db import session_factory
from fraud_engine.decision import DecisionPolicy
from fraud_engine.events import TransactionEvent, parse_event
from fraud_engine.inference import InferenceService
from fraud_engine.observability import RUNTIME_METRICS, structured_log
from fraud_engine.processing import FraudProcessor, SourceMetadata, UnavailablePredictor
from fraud_engine.redis_features import RedisFeatureEngine
from fraud_engine.resilience import (
    NonRetryableIdentityConflict,
    RetryableKafkaError,
    RetryableProcessingError,
    RetryPolicy,
)

logger = logging.getLogger(__name__)


def init_topics_main() -> None:
    admin = AdminClient({"bootstrap.servers": get_settings().kafka_bootstrap_servers})
    existing = set(admin.list_topics(timeout=10).topics)
    wanted = (get_settings().kafka_raw_topic, get_settings().kafka_dlq_topic)
    to_create = [
        NewTopic(name, num_partitions=3, replication_factor=1)
        for name in wanted
        if name not in existing
    ]
    if not to_create:
        logger.info("Kafka topics already exist")
        return
    for name, future in admin.create_topics(to_create).items():
        future.result(timeout=15)
        print(f"created {name}")


def make_producer() -> Producer:
    settings = get_settings()
    return Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
            "message.timeout.ms": 15000,
        }
    )


def produce_event(producer: Producer, event: TransactionEvent, topic: str | None = None) -> None:
    topic = topic or get_settings().kafka_raw_topic
    failures = []

    def on_delivery(error, message):
        if error is not None:
            failures.append(str(error))

    producer.produce(
        topic, key=event.user_id.encode(), value=event.canonical_bytes(), callback=on_delivery
    )
    remaining = producer.flush(timeout=20)
    if remaining or failures:
        raise RuntimeError(f"Kafka delivery not acknowledged: {failures or remaining}")


def produce_events(producer: Producer, events: list[TransactionEvent]) -> dict[str, float | int]:
    """Publish a bounded batch and return only after every broker acknowledgement."""
    from time import perf_counter

    failures: list[str] = []
    delivered = 0

    def on_delivery(error, message):
        nonlocal delivered
        if error is not None:
            failures.append(str(error))
        else:
            delivered += 1

    started = perf_counter()
    for event in events:
        while True:
            try:
                producer.produce(
                    get_settings().kafka_raw_topic,
                    key=event.user_id.encode(),
                    value=event.canonical_bytes(),
                    callback=on_delivery,
                )
                break
            except BufferError:
                producer.poll(0.1)
        producer.poll(0)
    remaining = producer.flush(timeout=60)
    elapsed = perf_counter() - started
    if remaining or failures or delivered != len(events):
        raise RuntimeError(
            f"Kafka batch delivery failed: delivered={delivered}, remaining={remaining}, "
            f"failures={failures}"
        )
    return {
        "events": delivered,
        "elapsed_seconds": elapsed,
        "events_per_second": delivered / elapsed if elapsed else 0.0,
    }


def make_probe_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": get_settings().kafka_bootstrap_servers,
            "group.id": "fraud-ingestion-probe-v1",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )


def make_processor_consumer() -> Consumer:
    settings = get_settings()
    return Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": "fraud-processor-v1",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "max.poll.interval.ms": 300_000,
        }
    )


def build_processor() -> FraudProcessor:
    settings = get_settings()
    try:
        predictor = InferenceService(Path(settings.model_artifact_dir))
    except Exception as exc:
        logger.exception("model artifact unavailable; processor will persist REVIEW fallbacks")
        predictor = UnavailablePredictor(str(exc))
    return FraudProcessor(
        session_factory(),
        RedisFeatureEngine(Redis.from_url(settings.redis_url)),
        predictor,
        DecisionPolicy.load(Path("config/decision-policy-v2.json")),
        inference_timeout_seconds=settings.inference_timeout_seconds,
    )


def validate_record(message) -> TransactionEvent:
    event = parse_event(message.value())
    if message.key() != event.user_id.encode():
        raise ValueError("Kafka key must match event.user_id")
    now = datetime.now(UTC)
    if event.event_timestamp > now + timedelta(minutes=5):
        raise ValueError("event timestamp beyond future-skew allowance")
    return event


def publish_dlq(producer: Producer, message, reason: str, topic: str | None = None) -> None:
    topic = topic or get_settings().kafka_dlq_topic
    body = {
        "source_topic": message.topic(),
        "source_partition": message.partition(),
        "source_offset": message.offset(),
        "key": message.key().decode(errors="replace") if message.key() else None,
        "value_hex": message.value().hex() if message.value() else None,
        "reason": reason,
        "quarantined_at": datetime.now(UTC).isoformat(),
    }
    failures = []
    producer.produce(
        topic,
        key=f"{message.topic()}:{message.partition()}:{message.offset()}".encode(),
        value=json.dumps(body).encode(),
        callback=lambda error, _: failures.append(str(error)) if error else None,
    )
    if producer.flush(timeout=20) or failures:
        raise RuntimeError(f"DLQ delivery not acknowledged: {failures}")


def process_probe_record(message, consumer, dlq_producer, handler) -> str:
    try:
        event = validate_record(message)
    except (ValidationError, ValueError) as exc:
        publish_dlq(dlq_producer, message, str(exc))
        consumer.commit(message=message, asynchronous=False)
        logger.warning(
            json.dumps(
                {
                    "event": "quarantined",
                    "partition": message.partition(),
                    "offset": message.offset(),
                }
            )
        )
        return "QUARANTINED"
    handler(event)
    consumer.commit(message=message, asynchronous=False)
    logger.info(
        json.dumps(
            {
                "event": "validated",
                "event_id": event.event_id,
                "transaction_id": event.transaction_id,
                "user_id": event.user_id,
                "partition": message.partition(),
                "offset": message.offset(),
            }
        )
    )
    return "VALID"


def process_fraud_record(message, consumer, dlq_producer, processor: FraudProcessor) -> str:
    """Process one offset synchronously and commit only after a durable terminal outcome."""
    try:
        event = validate_record(message)
    except (ValidationError, ValueError) as exc:
        try:
            publish_dlq(dlq_producer, message, f"INVALID_EVENT: {exc}")
        except Exception as dlq_error:
            raise RetryableKafkaError("DLQ publish was not acknowledged") from dlq_error
        consumer.commit(message=message, asynchronous=False)
        RUNTIME_METRICS.increment("dlq")
        structured_log(
            logger,
            logging.WARNING,
            "record_quarantined",
            reason="INVALID_EVENT",
            topic=message.topic(),
            partition=message.partition(),
            offset=message.offset(),
        )
        return "QUARANTINED"

    try:
        result = processor.process(
            event,
            SourceMetadata(message.topic(), message.partition(), message.offset()),
        )
    except NonRetryableIdentityConflict as exc:
        try:
            publish_dlq(dlq_producer, message, f"IDENTITY_CONFLICT: {exc}")
        except Exception as dlq_error:
            raise RetryableKafkaError("DLQ publish was not acknowledged") from dlq_error
        consumer.commit(message=message, asynchronous=False)
        RUNTIME_METRICS.increment("dlq")
        structured_log(
            logger,
            logging.WARNING,
            "record_quarantined",
            reason="IDENTITY_CONFLICT",
            event_id=event.event_id,
            transaction_id=event.transaction_id,
            user_id=event.user_id,
            partition=message.partition(),
            offset=message.offset(),
        )
        return "QUARANTINED"
    except RetryableProcessingError as exc:
        RUNTIME_METRICS.increment("consumer_failures")
        name = type(exc).__name__.lower()
        if "redis" in name:
            RUNTIME_METRICS.increment("redis_errors")
        if "database" in name:
            RUNTIME_METRICS.increment("postgresql_errors")
        structured_log(
            logger,
            logging.ERROR,
            "record_retryable_failure",
            error_type=type(exc).__name__,
            event_id=event.event_id,
            transaction_id=event.transaction_id,
            user_id=event.user_id,
            partition=message.partition(),
            offset=message.offset(),
        )
        return "RETRY"

    consumer.commit(message=message, asynchronous=False)
    RUNTIME_METRICS.increment("transactions_processed")
    RUNTIME_METRICS.increment(f"decisions_{result.action.lower()}")
    if result.outcome == "DUPLICATE":
        RUNTIME_METRICS.increment("duplicate_events")
    if "LATE_EVENT" in result.reason_codes:
        RUNTIME_METRICS.increment("late_events")
    RUNTIME_METRICS.observe("processing_latency", result.processing_latency_ms)
    RUNTIME_METRICS.observe("feature_latency", result.feature_latency_ms)
    RUNTIME_METRICS.observe("persistence_latency", result.persistence_latency_ms)
    if result.inference_latency_ms is not None:
        RUNTIME_METRICS.observe("inference_latency", result.inference_latency_ms)
    structured_log(
        logger,
        logging.INFO,
        "fraud_decision_terminal",
        outcome=result.outcome,
        event_id=event.event_id,
        transaction_id=result.transaction_id,
        user_id=event.user_id,
        decision_id=result.decision_id,
        action=result.action,
        reason_codes=result.reason_codes,
        model_version=result.model_version,
        feature_version=result.feature_version,
        processing_latency_ms=round(result.processing_latency_ms, 3),
        partition=message.partition(),
        offset=message.offset(),
    )
    return result.outcome


def produce_main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=Path, default=Path("data/generated/events.csv"))
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()
    producer = make_producer()
    events = [
        TransactionEvent.model_validate(record)
        for record in pd.read_csv(args.events, nrows=args.limit).to_dict("records")
    ]
    print(json.dumps(produce_events(producer, events)))


def probe_main() -> None:
    logging.basicConfig(level=logging.INFO)
    consumer = make_probe_consumer()
    producer = make_producer()
    consumer.subscribe([get_settings().kafka_raw_topic])
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            process_probe_record(message, consumer, producer, lambda _: None)
    finally:
        consumer.close()


def processor_main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = get_settings()
    consumer = make_processor_consumer()
    dlq_producer = make_producer()
    processor = build_processor()
    retry = RetryPolicy(settings.retry_initial_seconds, settings.retry_max_seconds)
    pending_messages = deque()
    poll_failure_attempt = 1
    consumer.subscribe([settings.kafka_raw_topic])
    try:
        while True:
            message = pending_messages.popleft() if pending_messages else consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                delay = retry.delay(poll_failure_attempt)
                structured_log(
                    logger,
                    logging.ERROR,
                    "kafka_poll_failure",
                    error=str(message.error()),
                    attempt=poll_failure_attempt,
                    delay_seconds=round(delay, 3),
                )
                RUNTIME_METRICS.increment("kafka_errors")
                poll_failure_attempt += 1
                time.sleep(delay)
                continue
            poll_failure_attempt = 1
            attempt = 1
            paused = False
            source_partition = TopicPartition(message.topic(), message.partition())
            paused_partitions: list[TopicPartition] = []
            while True:
                try:
                    disposition = process_fraud_record(message, consumer, dlq_producer, processor)
                except Exception:
                    logger.exception("unexpected processing failure; source offset retained")
                    disposition = "RETRY"
                if disposition != "RETRY":
                    if paused:
                        consumer.resume(paused_partitions)
                    break
                if not paused:
                    # A dependency outage can last indefinitely. Pause every assigned
                    # partition so the local pending deque stays bounded while poll(0)
                    # continues to heartbeat the consumer group.
                    paused_partitions = consumer.assignment() or [source_partition]
                    consumer.pause(paused_partitions)
                    paused = True
                delay = retry.delay(attempt)
                logger.warning(
                    json.dumps(
                        {
                            "event": "processing_retry",
                            "partition": message.partition(),
                            "offset": message.offset(),
                            "attempt": attempt,
                            "delay_seconds": round(delay, 3),
                        }
                    )
                )
                time.sleep(delay)
                heartbeat_record = consumer.poll(0)
                if heartbeat_record is not None:
                    if heartbeat_record.error():
                        structured_log(
                            logger,
                            logging.ERROR,
                            "kafka_heartbeat_failure",
                            error=str(heartbeat_record.error()),
                        )
                        RUNTIME_METRICS.increment("kafka_errors")
                    else:
                        pending_messages.append(heartbeat_record)
                attempt += 1
    finally:
        consumer.close()


if __name__ == "__main__":
    probe_main()
