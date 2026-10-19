# Phase 04 — Kafka ingestion transport

## Concepts and core logic

`TransactionEvent` validates schema v1 with timezone-aware UTC timestamps, positive integer paise, immutable IDs, and strict fields. `canonical_bytes()` sorts JSON keys before hashing so a reserialized identical event has a stable digest. The producer sends the canonical bytes with `user_id` as the Kafka key, `acks=all`, and idempotent producer mode. Producer idempotence limits producer retries; it does not make Redis/DB processing exactly once.

The `fraud-ingestion-probe-v1` consumer group is intentionally separate from the later fraud processor group. It validates messages, calls a supplied handler, and commits each source offset synchronously **after** that handler returns. Malformed records or key/payload mismatches go to `fraud.dead-letter`, including original bytes as hex and source coordinates; only an acknowledged DLQ publication permits source offset commit. This probe never scores or writes fraud decisions. Its commit represents completed **validation-only** work for the probe group.

```python
try:
    event = validate_record(message)
except (ValidationError, ValueError) as exc:
    publish_dlq(producer, message, str(exc))  # must receive broker acknowledgment
    consumer.commit(message=message, asynchronous=False)
else:
    handler(event)  # an exception leaves the source offset uncommitted
    consumer.commit(message=message, asynchronous=False)
```

Kafka preserves offset order inside a partition; it does not preserve global or event-time order. Three partitions let different users be distributed, while all events for one user go to one partition. Changing partition count later needs explicit migration considerations. The actual processor group in Checkpoint 8 will commit only after its PostgreSQL terminal outcome.

## Verification and limits

Unit tests cover valid records, handler failure, malformed records, failed DLQ publication, and repeatable topic initialization. Live Kafka tests verified same-user partition/offset order, distribution of 15 users across at least two partitions, a restarted consumer group's committed offset, and malformed-event DLQ acknowledgment. After `docker compose restart kafka`, the live tests passed again, showing local broker recovery for this tested path. The first probe run failed with `UNKNOWN_TOPIC_OR_PART` because the topics had not yet been created. The first repeat run of `fraud-init-topics` failed because the Admin API rejects an empty create list; the command now exits successfully when both topics exist. The live test initially used a fresh group with `latest` reset and intermittently saw records outside its intended run. It now seeks all assigned partitions to their high watermarks before publishing and filters by unique event IDs. Duplicate delivery is expected to call the validation handler again in this probe; identity deduplication belongs to the later durable processor, not this transport shell. A consumer restart can therefore repeat validation logs without creating a fraud decision.
