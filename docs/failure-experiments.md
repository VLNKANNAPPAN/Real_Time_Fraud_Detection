# Failure injection experiments

This record covers checkpoint 15. The experiments validate outcomes and recovery boundaries rather than claiming that Kafka, Redis, and PostgreSQL participate in one transaction. Automated cases use deterministic fakes or real Redis semantics through fakeredis. Restart cases use the local Compose stack and the real processor.

## Results

| # | Fault | Injection and timing | Expected and observed result | Invariant exercised | Evidence |
|---:|---|---|---|---|---|
| 1 | Duplicate event | Deliver the same immutable event twice, after the first decision | One decision; replay returns `DUPLICATE`; features run once. Passed. | Deduplicate before state mutation | `test_duplicate_delivery_creates_one_decision_and_does_not_repeat_features` |
| 2 | Malformed JSON | Invalid bytes before validation | DLQ acknowledgment precedes source commit. Passed. | Invalid input cannot poison the partition | `test_malformed_to_dlq_before_commit` |
| 3 | Late event | Event time older than watermark minus two minutes | Durable REVIEW, `LATE_EVENT`, null score, excluded from feature history. Passed. | Late data is explicit | `test_late_event_is_reviewed_without_inference_or_feature_inclusion` |
| 4 | In-bound out-of-order event | Event inside the lateness boundary arrives after newer data | Snapshot uses only previously arrived, strictly earlier event-time records; prior decisions stay immutable. Passed. | Event-time semantics | feature engine offline/Redis parity tests |
| 5 | Kafka restart | Restart broker with processor connected, then publish one event | Client reconnects, processor stays alive, offset 117 produces decision 732. Passed. | At-least-once recovery | live transaction `kafka-restart-transaction-b30907fabc08` |
| 6 | Redis restart | Stop Redis after Kafka is available, publish an event, restart Redis | Row remained PENDING, offset stayed uncommitted, attempts reached 9, then one scored decision. Passed. | Redis is disposable; no fabricated features | live transaction `redis-restart-transaction-2415e4250e90` |
| 7 | PostgreSQL restart | Stop DB, publish, restart DB | Repeated `RetryableDatabaseError`, no partial claim, then one decision after recovery. Passed. | DB owns durable identity | live transaction `postgres-restart-transaction-d423fd5fb6fa` |
| 8 | Crash after claim | Raise immediately after PENDING claim commit | Replay completes; no duplicate decision. Passed. | PENDING is recoverable | parametrized processing crash test |
| 9 | Crash after Redis update | Raise after atomic feature script | Replay reuses event snapshot; one feature insert and one decision. Passed. | Redis mutation is idempotent | `test_crash_after_redis_update_reuses_snapshot_on_retry` |
| 10 | Crash after inference | Raise after decision is calculated, before persistence | Replay may infer again but persists one decision and reuses feature snapshot. Passed. | Durable outcome is the DB commit | parametrized processing crash test |
| 11 | Crash around DB commit | Raise before commit and after committed decision in separate tests | Before commit rolls back and replays; after commit short-circuits as duplicate. Passed. | Offset follows terminal persistence | before-commit and `test_crash_after_db_commit_short_circuits_on_retry` |
| 12 | Missing model | Predictor cannot load artifact | Durable REVIEW with `MODEL_UNAVAILABLE`, null score/model version. Passed. | Score and action remain separate | model fallback tests |
| 13 | Corrupt model | Invalid or incompatible artifact metadata | Loader rejects artifact; processor uses explicit fallback. Passed. | Never silently score with incompatible artifacts | `test_missing_invalid_or_incompatible_feature_fails` |
| 14 | Redis timeout/outage | Redis connection raises before feature outcome | PENDING row and retryable error; no offset commit. Passed. | No zero-filled feature fallback | `test_redis_outage_leaves_pending_and_is_retryable` plus live restart |
| 15 | DB timeout/outage | Database connection unavailable at claim | Retryable error and no offset commit. Passed. | No action without durable identity | `test_database_unavailable_is_retryable_before_any_offset_commit` plus live restart |
| 16 | Poison event and DLQ outage | Invalid record while DLQ publish also fails | Source offset remains uncommitted. Passed. | Quarantine must itself be durable | `test_dlq_failure_keeps_offset_uncommitted` |
| 17 | Burst traffic | 50 event burst and 100-event per-user feature burst | No duplicate state; measured 19.55 events/s in the local direct processor run. Passed within prototype scope. | Bounded state and measured capacity | phase 14 artifact and `test_large_user_burst_and_user_isolation` |
| 18 | Reused event ID | Same `event_id`, changed immutable content | Non-retryable conflict; original decision remains. Passed. | Event identity is immutable | identity conflict tests |
| 19 | Reused transaction ID | New event under an existing `transaction_id` | Non-retryable conflict; original decision remains. Passed. | Business identity is unique | `test_event_and_transaction_identity_conflicts_are_independent` |
| 20 | Duplicate human label | Repeat same submission key, then conflicting value | Identical retry is idempotent; conflict returns 409. Passed. | Review writes are append-only and idempotent | API review test |
| 21 | Stale or mismatched model | Artifact feature version or digest is wrong | Load fails closed and generates explicit REVIEW fallback when processing. Passed. | Persist exact compatible version | inference compatibility test |
| 22 | Incompatible feature input | Required feature is absent or unknown version supplied | Inference refuses to score. Passed. | Offline/online preprocessing contract | inference compatibility test |

The machine-readable restart evidence is in `artifacts/failure-experiments/live-restarts.json`. Performance results are in `artifacts/performance/`.

## Bugs exposed and fixes

### Kafka poll errors could terminate the processor

The broker restart review found that the outer consumer loop raised on `message.error()`. The processor now records a `kafka_poll_failure`, backs off with jitter, and continues polling. Heartbeat poll errors are recorded instead of being inserted into the pending-message queue.

### Retry buffering could grow during a long outage

The retry loop originally paused only the source partition and continued polling other assigned partitions into an in-memory deque. It now pauses every assigned partition while one valid event is retrying. `poll(0)` continues heartbeats, and all partitions resume after the event reaches a durable outcome. This is conservative head-of-line blocking, but it bounds local buffering and preserves commit order.

### Inference timing measured only part of the call

The first slow-model run timed the inner scikit-learn operation rather than the full bounded predictor call, hiding injected delay. Timing now surrounds the complete predictor call. The performance suite was rerun and the corrected artifacts show the added latency.

### Evaluation initially mixed action and model metrics

Treating REVIEW and BLOCK as one positive model prediction would have made policy changes look like model changes. Model metrics now use the versioned score threshold, while action outcomes are reported separately.

## Remaining limits

- The deployment has one Kafka broker, so broker disk loss is outside the demonstrated restart guarantee.
- Pausing all assigned partitions favors bounded memory and simple ordering over availability across unrelated users.
- In-process timeout returns ignore a late inference result; Python cannot forcibly stop the underlying worker thread.
- The live restart experiments establish eventual recovery on this local stack. They are not a high availability or disaster recovery benchmark.
