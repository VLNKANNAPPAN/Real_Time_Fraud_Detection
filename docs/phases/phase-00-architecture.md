# Phase 00 — architecture and system design

Status: complete as a **design proposal**. No application or infrastructure has been implemented or tested yet. Read this phase record before checkpoint 1.

## Deliverables

The project map, architecture, event model, invariants, Kafka/offset semantics, exact v1 feature definitions, ML methodology, failure model, and ADRs 0001/0002 now specify the intended system. Six Mermaid views cover architecture, transaction lifecycle, Kafka processing, feature computation, decision path, and retry flow.

## Key decisions

- `event_id` + immutable digest identify a delivery; `transaction_id` identifies the business object; PostgreSQL enforces uniqueness.
- `user_id` is the Kafka partition key; ordering relied on is offset order within one partition, not event-time or global order.
- PostgreSQL is durable identity/decision history. Redis is derived bounded feature state with rebuild from completed events.
- Manual offset commit follows terminal PostgreSQL commit or acknowledged DLQ publication.
- Feature windows use prior completed events in `[t-W,t)`, in original arrival order. Two-minute lateness tolerance; beyond that REVIEW with no score and no feature insertion.
- Model score and business action are versioned separately. Redis/DB outages leave processing pending; model failure/timeout becomes explicit REVIEW fallback.

## Independent second-pass architecture review

The first pass proposed an atomic Redis snapshot/insert and DB terminal record. The second pass examined crash windows, ordering, state recovery, and ML leakage independently:

| Perspective | Finding and design response |
|---|---|
| Streaming | DB commit followed by failed offset commit redelivers. DB terminal check must precede Redis access; do not advance offset earlier. Acknowledged DLQ may duplicate. |
| Redis/state | Redis mutation followed by DB failure leaves a pending entry. Store the pre-event snapshot for replay; rebuild only completed DB events after cache loss. No partial rebuild may serve a score. |
| Database | Unique IDs alone do not serialize different transactions of one user during rebalance. Require a per-user processing lock through final decision commit. |
| ML | Sorting offline events by event time would allow knowledge unavailable online. Offline features must replay arrival order and frozen snapshots. Train-only preprocessing and chronological validation avoid future leakage. |
| Product | A queued or pending event cannot be presented as ALLOW. Model failure yields REVIEW with an explicit missing score; BLOCK needs corroboration/policy review. |
| Failure | Kafka retention could delete unprocessed accepted events during prolonged outage. Add lag/oldest-age alarms and an operator runbook; one local broker remains a durability limit. |

## Implementation gates and remaining risks

Checkpoint 1 must prove the local stack fits 8 GB RAM and document actual startup/migration checks. Checkpoints 5 and 8 must test the atomic Redis operation, per-user lock, rebuild, and crash windows against real Redis/PostgreSQL where practical. Benchmark hot-user history scans before relying on Lua at scale. Define canonical digest serialization, DB state transitions, retry/DLQ format, and exact timeout/config versions in code. The two-minute tolerance and fallback policy are prototype choices to validate against a realistic arrival-delay distribution and false-positive cost. No measured throughput, latency, or model quality is claimed.
