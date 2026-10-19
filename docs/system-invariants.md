# System invariants

These are design obligations for later implementation and tests. "Accepted" means a producer got a Kafka acknowledgment; it does not mean a fraud action was issued.

1. Each transaction has one stable `transaction_id`; each immutable envelope has one stable `event_id`. They identify different things. PostgreSQL uniqueness and canonical payload digest make conflicting reuse explicit.
2. Kafka is at least once. A repeated identical event can be processed again but produces at most one terminal decision and cannot insert the transaction into Redis history twice.
3. A `PENDING` claim is not a final action. A valid accepted event reaches a durable terminal decision or remains visibly pending for retry; malformed/conflicting events reach a durable quarantine/DLQ path before offset acknowledgment.
4. Normal feature snapshots contain only previously **arrived and completed**, eligible events with event time strictly before the current event's time. The current transaction and any future event-time information are excluded.
5. An event beyond the lateness bound yields REVIEW with reason `LATE_EVENT`, no model score, and `feature_included=false`; it does not rewrite old snapshots or enter feature history. In-bound out-of-order events use their own event-time windows; old decisions stay immutable.
6. Redis is a bounded derived cache. Its loss can be recovered from completed PostgreSQL events; a partial rebuild cannot be used for scoring. A Redis outage cannot silently produce stale features.
7. A normal decision records raw event, feature snapshot, feature-definition version, model artifact version, score, policy version, reasons, and timestamps. A fallback records the same audit context plus explicit inference status and **no** model version/score when no model ran.
8. Model input order, preprocessing, categorical vocabulary policy, and feature semantics are artifact-bound and checked at load/inference. A mismatch fails explicitly.
9. A malformed record cannot trap a partition forever: after classified as non-retryable and acknowledged by the DLQ broker, its source offset may advance. Transient failures keep the offset uncommitted.
10. Producer success requires a Kafka acknowledgment. DB success before offset commit is safe to replay. No offset is committed before the event's terminal DB outcome or acknowledged DLQ outcome.
11. Human labels reference the immutable decision ID and model/feature versions actually used. Repeated identical submissions are idempotent; conflicting submissions require explicit correction workflow.
12. An unavailable Redis/PostgreSQL dependency leaves processing pending; an unavailable/slow model yields a persisted REVIEW fallback when dependencies permit. No dependency failure silently becomes ALLOW.
