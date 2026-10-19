# Final architecture review

Review date: 2026-09-23
Scope: checkpoints 0–18, including source, migrations, tests, live Compose services, documentation, and measured artifacts.

## Conclusion

The repository is complete as a local production-style learning system. Its core correctness claims are supported by deterministic tests and live Kafka, Redis, and PostgreSQL integration tests. It is suitable for demonstrating streaming semantics, replay-safe processing, versioned ML, explainable policy, delayed labels, and operational failure behavior.

It is not a high availability payment authorization service. One broker, one synchronous processor, local credentials, caller-supplied reviewer identity, and a weak synthetic baseline remain explicit deployment limits.

## Actual guarantees

- Kafka events are consumed at least once and keyed by `user_id`.
- An identical replay creates at most one PostgreSQL decision and does not insert a second Redis history item.
- `event_id` and `transaction_id` uniqueness are enforced independently with an immutable payload digest.
- Valid unresolved work stays PENDING or unclaimed and keeps its Kafka offset uncommitted.
- Invalid or conflicting work advances only after an acknowledged DLQ publish.
- Normal features use event time, exclude the current event, and include only already arrived eligible history.
- Events outside the lateness bound become explicit REVIEW decisions and do not enter feature state.
- Redis can be rebuilt from completed, feature-included PostgreSQL events.
- The artifact-bound model returns a score; the separately versioned policy returns an action and reasons.
- A model failure can produce an explicit persisted REVIEW with no fabricated score. Redis or PostgreSQL failure cannot silently produce ALLOW.
- Human and simulated labels attach to immutable decisions and expose their availability time and source.
- Monitoring export and evaluation preserve feature, model, policy, and schema versions.

## Guarantees deliberately absent

- exactly-once execution across Kafka, Redis, and PostgreSQL;
- global Kafka order or event-time arrival order;
- synchronous authorization immediately after producer acknowledgment;
- automatic revision of decisions after late events;
- survival of single-broker disk loss;
- calibrated probability or proven production fraud effectiveness;
- authenticated analyst identity in the local API;
- automatic label correction, model retraining, or deployment rollback.

## Invariant traceability

| Invariant | Implementation | Verification |
|---|---|---|
| Separate immutable event and transaction identities | unique columns, digest comparison, DLQ conflict path | processing identity conflict tests; migration constraints |
| Replay produces one terminal decision | terminal short-circuit, unique decision transaction ID, Redis snapshot key | duplicate, concurrency, crash-after-commit tests |
| PENDING is not an action | short claim transaction, terminal decision table, query status | Redis/DB outage tests and live restart evidence |
| Current and future events excluded from features | strict `[t-W,t)` Lua/offline selection | offline/Redis boundary and parity tests |
| Late data is explicit | watermark check and `LATE_EVENT` fallback | late and out-of-order tests |
| Redis is rebuildable | completed-history query, generation marker, atomic restore | cache-loss and live Redis restart tests |
| Audit context is complete | raw event, snapshot, versions, score/status, reasons, times | persistence and API tests |
| Model compatibility fails closed | manifest hash, sklearn version, ordered input and feature version checks | inference artifact tests |
| Invalid records cannot poison a partition | acknowledged DLQ before source commit | malformed and DLQ-failure tests |
| Offset follows durable terminal outcome | synchronous commit after DB/DLQ acknowledgment | Kafka processing tests and live crash integration |
| Labels are immutable and idempotent | decision/submission unique constraints, conflict response | review API and delayed-label tests |
| Dependency failures never become ALLOW | retryable Redis/DB errors; model-only REVIEW fallback | resilience and model timeout tests |

## Final hardening changes

### Database domains

Migration `0004_schema_constraints` makes invalid durable states harder to create outside the API. It enforces:

- transaction status and positive attempt count;
- action and inference-status enumerations;
- score range when a score is present;
- review label and source enumerations;
- a non-null label availability timestamp;
- a foreign key from the label's transaction ID to the transaction ledger.

### Kafka recovery and bounded buffering

Broker error records now produce structured retry telemetry and backoff rather than terminating the process. During a retryable valid event, all assigned partitions pause, `poll(0)` continues group heartbeats, and partitions resume after a durable terminal outcome. This bounds application buffering and preserves simple offset order at the cost of cross-partition head-of-line blocking.

### Failure classification

The predictor call alone is inside the model-availability fallback. A policy or model-contract defect is no longer mislabeled as `MODEL_UNAVAILABLE`; it remains visible as a processing failure instead of generating an apparently valid fallback action.

### Documentation corrections

The review removed an unsupported authentication claim, replaced deferred Redis/lock wording with the implemented design, updated retry documentation from one-partition pause to all-assignment pause, and made the PostgreSQL host port and performance scope explicit.

## Evidence from this completion run

- Ruff formatting: passed.
- Ruff lint: passed.
- Python compilation: passed.
- Deterministic suite: 59 passed, 1 skipped; two third-party deprecation warnings.
- Live integration suite: 5 passed with migration `0004_schema_constraints` at head.
- Redis restart: PENDING event recovered to one scored decision after nine attempts.
- PostgreSQL restart: no partial claim during outage; one decision after recovery.
- Kafka restart: processor survived reconnect and decided the next event.
- Dashboard: rendered from live API data with 330 transactions and zero lag at capture time.

The raw live restart record is `artifacts/failure-experiments/live-restarts.json`; performance reports are under `artifacts/performance/`.

## Open risks by severity

### High for a real payment deployment

- Single-broker Kafka and single-instance PostgreSQL have no high availability or disaster recovery guarantee.
- The API trusts a caller-supplied reviewer ID and has no production authentication, authorization, or transport-security setup.
- The measured synthetic baseline is weak and uncalibrated; automatic BLOCK is not justified by current evidence.

These are accepted local-scope limits rather than hidden defects.

### Medium

- Pausing all assigned partitions bounds memory but one dependency-blocked event delays unrelated partitions owned by the same consumer.
- Python's thread timeout ignores a late result but cannot forcibly stop arbitrary inference code.
- There is no automated stale-PENDING sweeper or paging integration; operators must use status age, lag, and logs.
- Runtime retry counters reset on restart unless scraped by Prometheus.
- Label correction is documented but has no correction-record schema or workflow.
- There is no PostgreSQL outbox for reliable downstream decision publication.

### Low

- The dashboard is a lightweight operational console and has no pagination beyond bounded API limits.
- Local demo credentials and ports are fixed defaults intended for loopback development.
- Performance artifacts describe one small Windows machine and exclude Kafka transport latency.

## Recommended next increments

1. Add authenticated identities and authorization to review writes.
2. Add an explicit label correction table with supersession and audit semantics.
3. Add a stale-PENDING monitor and alert routing.
4. Add a PostgreSQL outbox before publishing decisions to another Kafka topic.
5. Run representative end-to-end Kafka load tests and choose consumer scaling only from measured lag and hot-user behavior.
6. Validate and calibrate a stronger model on representative, governed data before considering automatic BLOCK.
7. Add replicated infrastructure and recovery objectives if the service becomes an authorization dependency.
