# Checkpoint 8 — Idempotent end-to-end processing

## Outcome

Checkpoint 8 connects the real processing path:

```text
Kafka record
  -> validate envelope and key
  -> acquire per-user PostgreSQL session lock
  -> claim event and transaction identity as PENDING
  -> rebuild Redis from completed PostgreSQL history when needed
  -> atomically calculate and freeze the pre-event feature snapshot
  -> run inference
  -> apply the versioned decision policy
  -> commit the terminal decision in PostgreSQL
  -> commit the Kafka offset
```

The implementation is in `src/fraud_engine/processing.py`. Kafka transport and offset handling are in `src/fraud_engine/kafka_io.py`. Migration `0002_processing_and_reviews` adds source coordinates, attempt metadata, terminal timestamps, inference latency, and the review-label table.

## Concepts

### At-least-once delivery needs idempotency

Kafka may redeliver an offset after a consumer crash, rebalance, or failed offset commit. The processor therefore cannot treat delivery as proof that work is new. PostgreSQL is the identity authority:

- `event_id` identifies one immutable event envelope.
- `transaction_id` identifies one business transaction.
- `payload_digest` proves that a repeated ID has the same canonical content.
- unique constraints provide the final race-safe enforcement.

An identical terminal event returns `DUPLICATE`. A reused ID with different content raises `NonRetryableIdentityConflict` and is sent to the DLQ. It never changes the original transaction or decision.

### A PENDING claim is a recovery marker

The first short database transaction inserts a `PENDING` row before Redis or the model runs. It makes an interrupted attempt visible and reserves both identities. `PENDING` is not an ALLOW, REVIEW, or BLOCK result.

```python
row = TransactionRow(
    transaction_id=event.transaction_id,
    event_id=event.event_id,
    payload_digest=event.digest(),
    status="PENDING",
    source_topic=source.topic,
    source_partition=source.partition,
    source_offset=source.offset,
)
session.add(row)
session.commit()
```

If insertion loses a database uniqueness race, the session rolls back and reads the winning row. Exact content continues or short-circuits. Different content becomes an identity conflict.

### Why the per-user lock is session-level

Normal Kafka partitioning already sends one user's records to one partition, but a rebalance or a separate replay process can temporarily overlap consumers. The processor also needs the lock to span two database transactions and external Redis/model calls. A transaction-level lock would be released immediately after the PENDING claim.

Production uses PostgreSQL `pg_advisory_lock(hashtextextended(user_id, 0))` on a pinned connection. The connection remains pinned until the terminal commit or failure cleanup. SQLite tests use a process-local reentrant lock.

```python
connection.execute(
    text("SELECT pg_advisory_lock(hashtextextended(:user_id, 0))"),
    {"user_id": user_id},
)
connection.commit()  # advisory lock remains owned by this session
session = Session(bind=connection, expire_on_commit=False)
```

The unlock is in `finally`, so exceptions do not leave a pooled PostgreSQL connection holding a lock.

### Redis snapshots close the first crash gap

The Redis Lua operation calculates features from prior history, inserts the current event once, and stores the exact pre-event snapshot under `event_id`. These actions are atomic inside Redis.

If the process crashes after Redis changes but before PostgreSQL stores a decision, the database still says `PENDING`. Redelivery repeats the Redis call. Redis recognizes the event and returns the saved snapshot with `duplicate=true`, so the transaction is not counted twice and inference uses the same inputs.

If Redis was lost, `is_initialized(user_id)` is false. The processor rebuilds the user's cache from PostgreSQL rows that are both terminal and `feature_included=true`. It excludes the current PENDING event, then processes that event once.

### PostgreSQL closes the second crash gap

The final database transaction inserts one `DecisionRow` and moves the transaction from `PENDING` to `DECIDED`. It stores:

- raw immutable event payload and digest;
- Kafka topic, partition, and offset;
- feature snapshot and feature-definition version;
- score, model version, and inference status;
- action, reason codes, and policy version;
- feature inclusion flag;
- processing, decision, and terminal timestamps;
- attempt count and fallback error when applicable.

Only after that commit succeeds does `process_fraud_record` synchronously commit the Kafka offset.

```python
result = processor.process(event, SourceMetadata(...))
consumer.commit(message=message, asynchronous=False)
```

If PostgreSQL commits and the process dies before the offset commit, redelivery sees `DECIDED`, returns `DUPLICATE`, skips Redis and inference, and safely commits the offset.

## Crash and replay analysis

| Crash point | Durable state | Replay behavior |
|---|---|---|
| Before claim commit | no row | create PENDING and process normally |
| After claim, before Redis | PENDING | continue the same identity |
| After Redis mutation | PENDING plus Redis snapshot | Redis returns frozen snapshot; no double count |
| After inference | PENDING plus Redis snapshot | inference may repeat against identical inputs |
| After DB terminal commit | DECIDED plus one decision | short-circuit before Redis/model |
| After DB commit, before offset commit | DECIDED plus uncommitted offset | Kafka redelivers; terminal short-circuit then offset commit |

The design provides one durable terminal decision per transaction when PostgreSQL remains durable and dependencies eventually recover. It does not claim one physical execution across Kafka, Redis, and PostgreSQL.

## Tests added

`tests/test_processing.py` deterministically verifies:

- the same Kafka event twice creates one decision and one feature insertion;
- a simulated crash after Redis mutation leaves PENDING and safely reuses the snapshot;
- a simulated crash after PostgreSQL commit skips feature work on replay;
- changed payload under an existing identity is rejected;
- two concurrent consumers racing the same transaction create one decision;
- Redis outage leaves the offset uncommitted and the claim PENDING;
- the Kafka wrapper commits only terminal outcomes or acknowledged DLQ outcomes.

## Errors encountered and resolutions

### Workspace commands failed before process start

An accidental `git init` created repository metadata with ownership that the sandbox helper rejected. Every command failed during environment setup. The empty `.git` directory was removed, restoring access. No source history was lost because this workspace did not previously contain a Git repository.

### Holding an advisory lock across commits

A normal SQLAlchemy session can return its connection to the pool after `commit()`. That would make a session-level PostgreSQL lock unsafe. The solution was to explicitly open and pin one engine connection, bind a Session to it, and release the advisory lock before closing the connection.

### Cache rebuild must exclude PENDING

Replaying all transaction rows would insert a PENDING event before its original snapshot was recomputed. The rebuild query now joins decisions and selects only `status=DECIDED` with `feature_included=true`.

### Consumer heartbeat polling could discard a record

An early retry loop called `consumer.poll(0)` merely to serve heartbeats and ignored its return value. A poll can return another record, so that version could lose a record in application memory. The loop was first corrected to retain such records. Checkpoint 15 then paused every assigned partition during an unresolved event so the pending queue remains bounded. A process crash still leaves uncommitted offsets for redelivery.

### Repeated live tests accumulated unrelated Kafka history

The restart integration test originally relied on `auto.offset.reset=earliest` for partitions that had no committed group offset. On a reused local topic, the consumer could spend its bounded search reading old records from unrelated partitions and miss the newly published target. The restarted test consumer now seeks all assignments to their current end before publishing the target record. The durable group offset is still queried and asserted separately.

## Limits

- PostgreSQL advisory lock hash collisions can serialize unrelated users, but cannot violate correctness.
- A single Kafka broker is not durable against broker disk loss.
- Cross-user features need a separate ordering and aggregation design.
- A permanently failing valid event intentionally blocks later offsets in its partition until recovery or an explicit operator quarantine action.
