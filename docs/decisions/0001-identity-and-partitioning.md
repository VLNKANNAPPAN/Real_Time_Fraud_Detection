# ADR 0001: immutable identity and per-user Kafka key

Status: proposed for checkpoint 0; implementation checkpoints must test it.

## Problem

Duplicate, conflicting, and out-of-order events must not duplicate decisions or corrupt user features. Kafka has only partition-local offset order.

## Naive solution and why it fails

Treating `transaction_id` as the event ID misses altered/replayed envelopes. Keying by transaction ID distributes one user's activity across partitions, losing useful per-user arrival order. Assuming Kafka exactly-once or global order leaves Redis and PostgreSQL unprotected.

## Chosen design

Use immutable `event_id` plus canonical digest for delivery identity, and unique `transaction_id` for v1 business identity. Key raw events by `user_id`; validate key/payload agreement. PostgreSQL uniqueness is authoritative. A per-user processing lock covers rebalance overlap. Same event/digest replay is idempotent; conflicting reuse is quarantined. Event time, not Kafka offset, defines feature windows.

## Tests and adversarial cases for later checkpoints

Exact duplicate, same event ID with changed payload, same transaction ID with different event ID, wrong Kafka key, rebalance overlap, equal event timestamps, one-second out-of-order event, two-minute lateness boundary, and a hot user burst. A global feature or partition-count change requires a new ADR.

## Limitation

A single hot user can dominate a partition. This design prioritizes understandable per-user correctness on one local broker.
