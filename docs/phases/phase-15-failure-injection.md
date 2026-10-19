# Phase 15 — Failure injection

## Objective

Exercise duplicates, invalid input, event-time edge cases, dependency outages, crash windows, identity conflicts, model failures, review idempotency, and burst behavior against the system invariants.

## Implementation

- Added crash hooks at claim commit, Redis update, decision ready, before DB commit, and after DB commit.
- Added independent event-ID and transaction-ID conflict tests.
- Added Kafka poll recovery with bounded exponential backoff and jitter.
- Bounded retry buffering by pausing all assigned Kafka partitions during an unresolved valid event.
- Ran real Kafka, Redis, and PostgreSQL restart experiments with traceable event IDs.

## Result

All 22 planned cases have automated or live evidence. See [failure experiments](../failure-experiments.md) for exact injections, expected results, observed outcomes, and limitations.
