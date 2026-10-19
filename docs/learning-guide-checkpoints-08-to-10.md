# Learning guide — checkpoints 8 to 10

This guide links the implementation records for the complete transaction path.

1. [Checkpoint 8: idempotent processing](phases/phase-08-idempotent-processing.md) explains identity claims, advisory locks, Redis snapshot reuse, terminal database writes, offset ordering, and crash recovery.
2. [Checkpoint 9: resilience](phases/phase-09-resilience.md) explains failure classification, retry/backoff, DLQ acknowledgment, model fallbacks, timeouts, and health.
3. [Checkpoint 10: API and review](phases/phase-10-api-review.md) explains thin controllers, query services, review-label idempotency, metrics, and API tests.

## The core invariant in one sentence

For a valid record, the consumer advances its Kafka offset only after PostgreSQL has one durable terminal decision; for an invalid record, it advances only after Kafka acknowledges the DLQ record.

## Suggested code reading order

1. `events.py` — immutable input and canonical digest.
2. `processing.py` — claim/rebuild/feature/infer/decide/persist orchestration.
3. `redis_features.py` — atomic feature calculation and event deduplication.
4. `resilience.py` — named retry classes, timeout, and backoff.
5. `kafka_io.py` — validation, DLQ ordering, consumer commit boundary.
6. `db.py` and migration `0002` — durable identity, decisions, and labels.
7. `services.py` and `api.py` — read/review interfaces without recalculation.
8. `test_processing.py` and `test_api.py` — executable examples of crash and idempotency behavior.

## Mental model

Kafka supplies replay, PostgreSQL supplies truth, Redis supplies speed, the model supplies a score, and policy supplies an action. Keeping those roles separate is what makes each failure visible and recoverable.
