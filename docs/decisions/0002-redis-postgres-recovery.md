# ADR 0002: PostgreSQL ledger and rebuildable Redis state

Status: proposed for checkpoint 0; prove the protocol in checkpoints 5 and 8.

## Problem

Redis feature mutation, PostgreSQL decision commit, and Kafka offset commit cannot be a single atomic operation. A crash can occur between any two. Redis may lose its entire cache.

## Naive solution and why it fails

Updating Redis and immediately committing the Kafka offset can lose a decision. Writing the DB first and then blindly incrementing Redis can double-count on replay. Treating Redis as the only historical ledger makes recovery impossible after a restart.

## Chosen design

Claim immutable identity as PENDING in PostgreSQL; compute and insert once in an atomic Redis operation that saves the pre-event snapshot; commit the terminal decision and feature snapshot in PostgreSQL; then commit the Kafka offset. Replays inspect the DB terminal state first. A missing Redis cache is rebuilt from **completed feature-included** DB events; pending events are never used as prior history. Per-user serialization prevents later events from observing an unfinished one. Rebuild must complete atomically from the scorer's perspective.

## Tests and adversarial cases for later checkpoints

Crash after claim, after Redis mutation, after inference, after DB commit, and before offset commit; Redis flush/restart; concurrent duplicate delivery; incomplete rebuild; duplicate digest conflict; DB unavailable during final commit. Compare the eventual snapshot and one-decision invariant in each case.

## Limitation

This gives replay-safe outcomes under the stated assumptions, not exactly-once execution. Database durability, event retention, and a stable immutable schema remain required. Redis warm-state performance and per-user lock contention need measurement.
