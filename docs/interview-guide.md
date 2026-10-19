# Interview guide

## Explain the architecture in one minute

Kafka accepts immutable transaction events keyed by user. A processor manually commits offsets only after PostgreSQL contains either a terminal fraud decision or Kafka has acknowledged an invalid record in the DLQ. Redis keeps bounded per-user event-time history and can be rebuilt from completed PostgreSQL records. A versioned scikit-learn pipeline estimates risk; a separate policy maps the score and rule signals to ALLOW, REVIEW, or BLOCK. FastAPI and Streamlit read persisted outcomes and never rescore a transaction.

## Why use at-least-once delivery?

Kafka offset commit and the PostgreSQL decision commit cannot be one local transaction. The processor accepts redelivery and makes it harmless with immutable identities, unique constraints, cached pre-event snapshots, and terminal short-circuiting. This provides one durable terminal decision per transaction under retry, conditional on PostgreSQL durability and eventual dependency recovery. It does not provide exactly-once execution across services.

## Why key Kafka by `user_id`?

Per-user features need sequential arrival processing. A Kafka key keeps one user's normal traffic on one partition. PostgreSQL advisory locks still protect replay and rebalance overlap. Kafka does not provide ordering across users or by event timestamp.

## How are event time and late data handled?

The user watermark is the maximum event time among completed, feature-included events. An event older than watermark minus two minutes gets a durable REVIEW with `LATE_EVENT`; it is not inserted into history. An in-bound out-of-order event uses only earlier events already known at decision time. Past decisions are not rewritten.

## Why is Redis safe to lose?

Redis owns no durable business fact. PostgreSQL stores raw event payloads, terminal status, whether an event was included in features, and decision order. On a cache miss, the processor rebuilds completed eligible history, excluding PENDING and late events. The pending event is then processed exactly once by the Redis script.

## How are model and policy responsibilities separated?

The model returns a risk score plus its version. The policy uses that score, strong behavioral signals, hard rules, and fallback state to choose an action and reason codes. A high score alone does not automatically BLOCK in the current policy. Model metrics therefore use a score threshold, while action metrics report the policy outcome separately.

## What happens when the model fails?

With PostgreSQL and features available, a missing, corrupt, or timed-out model produces a persisted REVIEW with an explicit inference status and no fabricated score or model version. Redis or PostgreSQL failures leave the event pending instead because the system cannot calculate trustworthy features or persist an auditable action.

## How are delayed labels evaluated?

Labels are separate append-only records with source and availability time. Evaluation includes only matured FRAUD or NOT_FRAUD labels, excludes UNSURE, and reports results overall, by model version, and by event day. Chronological splits and availability checks avoid training or evaluation leakage.

## What did failure injection reveal?

It exposed three practical issues: Kafka poll error events could terminate the process, retry polling could grow an in-memory queue, and the first inference timer excluded wrapper delay. The processor now retries broker errors, pauses all assigned partitions during an unresolved event, and times the full predictor call.

## What are the largest limitations?

- One local Kafka broker has no high availability or broker disk-loss tolerance.
- One synchronous processor favors clarity over throughput.
- The baseline model has weak measured PR-AUC and should be treated as a pipeline artifact, not production fraud intelligence.
- Review identity is caller supplied in the local demo; production authentication and authorization are outside the current deployment.
- The inference deadline ignores late thread results but cannot forcibly terminate Python worker code.

## What would you build next?

Use a transactional outbox for downstream decision events, introduce authenticated analyst identities, move the processor to a supervised deployment, add a correction/audit workflow for labels, calibrate and validate a stronger model on representative data, add multi-broker Kafka and replicated PostgreSQL, and scale processing with explicit partition and hot-user capacity tests.
