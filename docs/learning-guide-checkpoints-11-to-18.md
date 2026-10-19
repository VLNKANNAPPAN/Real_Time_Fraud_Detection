# Learning guide: checkpoints 11–18

This guide explains the concepts, core logic, significant code, failures encountered, and fixes used to complete the system after the end-to-end processor and API.

## Checkpoint 11: delayed labels and evaluation

### Concept

A production prediction exists before its outcome is known. Evaluation must join a frozen decision to a label only after the label is available. The label must not alter the original event, feature snapshot, score, or action. `UNSURE` is valid review data but cannot be treated as a binary target.

`ReviewLabelRow` therefore stores the decision identity, model and feature versions from that decision, label source, and `available_at`. The importer checks maturity before insertion.

### Core logic

```python
available_at = datetime.fromisoformat(item["label_available_at"]).astimezone(UTC)
if available_at > as_of:
    counts["not_mature"] += 1
    continue
decision = session.scalar(
    select(DecisionRow).where(DecisionRow.transaction_id == item["transaction_id"])
)
```

Model metrics use the versioned score threshold. Policy outcomes are separate:

```python
predictions = [int(score >= score_threshold) for score in scores]
model_metrics = _binary_metrics(labels, predictions, scores)
# ALLOW false negatives, BLOCK false positives, and REVIEW conversion
# are calculated in a separate decision_outcomes structure.
```

This distinction matters because changing a review threshold changes operations without changing the model's ranking quality.

### Error and fix

The first evaluator draft treated REVIEW and BLOCK as a single positive model prediction. That mixed policy behavior with model behavior. The implementation was changed to calculate binary model predictions from the score threshold and report actions independently.

Key files: `src/fraud_engine/evaluation.py`, `migrations/versions/0003_delayed_labels.py`, `tests/test_evaluation.py`.

## Checkpoint 12: Streamlit analyst dashboard

### Concept

The UI is a read and review client. It does not import the model, access Redis, or recalculate risk. Every displayed value comes from FastAPI, which in turn reads persisted facts.

The dashboard has four views:

- live overview with health, lag, counts, latency, and recent transactions;
- review queue with idempotent label submission;
- transaction inspector for raw payload, frozen features, versions, score, and reasons;
- matured-label performance.

### Core logic

```python
transaction = api.get(f"/transactions/{transaction_id}")
decision = api.get(f"/decisions/{transaction_id}")
st.json(transaction["payload"])
st.json(decision["feature_snapshot"])
st.json(decision)
```

The `fraud-dashboard` command delegates to Streamlit's own runner. Calling the render function as a normal Python console script would not establish Streamlit's runtime context.

### Error and fix

The first headless screenshot captured only Streamlit's loading skeleton. A persistent Edge DevTools session was used so the page could keep its WebSocket connection, settle, and then call `Page.captureScreenshot`. A second issue selected Edge's automatic sync confirmation tab; the capture script now selects the HTTP page target. The final screenshot is `demo/dashboard.png`.

Key files: `src/fraud_engine/dashboard.py`, `scripts/capture_dashboard.py`, `tests/test_dashboard.py`.

## Checkpoint 13: observability

### Concept

Observability has three layers:

1. structured event logs for individual attempts and terminal decisions;
2. bounded in-process counters and latency samples for immediate runtime inspection;
3. database-derived metrics for durable totals and latency percentiles.

Kafka lag is calculated from the consumer group's committed offsets and each partition's high watermark.

### Core logic

```python
result = processor.process(event, source)
consumer.commit(message=message, asynchronous=False)
RUNTIME_METRICS.increment("transactions_processed")
RUNTIME_METRICS.observe("processing_latency", result.processing_latency_ms)
structured_log(
    logger,
    logging.INFO,
    "fraud_decision_terminal",
    transaction_id=result.transaction_id,
    action=result.action,
    model_version=result.model_version,
)
```

The offset commit stays before the success metric and terminal log so telemetry cannot imply a committed Kafka outcome when the commit itself failed.

### Design limit

The in-process registry resets on process restart. Durable counts come from PostgreSQL, and Prometheus should scrape frequently if runtime retry/error counters need continuity.

Key files: `src/fraud_engine/observability.py`, `src/fraud_engine/kafka_io.py`, `config/prometheus.yml`.

## Checkpoint 14: load and performance

### Concept

The local harness isolates processor cost from Kafka transport. Every sample covers PostgreSQL claim, feature processing, bounded inference, policy, and terminal DB commit. Kafka lag is observed separately so the scope is explicit.

Five scenarios run the same processor path: normal, event-time burst, slow model, slow DB, and slow Redis.

### Core logic

```python
started = perf_counter()
result = processor.process(event, SourceMetadata("load-test.direct", 0, offset))
elapsed = perf_counter() - started
samples.append(
    LoadSample(
        result.processing_latency_ms,
        result.feature_latency_ms,
        result.inference_latency_ms,
        result.persistence_latency_ms,
    )
)
```

### Error and fix

The first slow-model result showed little inference change because the timer was inside `InferenceService` and excluded the delayed wrapper. The processor now times the full predictor call around the deadline wrapper. All scenarios were rerun and the stored reports reflect corrected measurement.

Measured normal throughput was 19.47 events/s with 62.25 ms p95 end-to-end latency on this machine. These are development measurements, not a service-level objective.

Key files: `src/fraud_engine/load_test.py`, `scripts/run-performance.ps1`, `artifacts/performance/`.

## Checkpoint 15: failure injection

### Concept

Failures are injected at state boundaries, because an error before Redis mutation has a different recovery path from a crash after DB commit. The important question is which durable facts exist and whether the Kafka offset advanced.

The processor exposes deterministic hooks:

```python
self.fault_hook("claim_committed")
self.fault_hook("redis_updated")
self.fault_hook("decision_ready")
self.fault_hook("before_db_commit")
self.fault_hook("db_committed")
```

### Live recovery evidence

- Redis stopped: the event remained PENDING, its offset stayed uncommitted, and attempt count reached nine. It completed once Redis returned.
- PostgreSQL stopped: retries created no partial row. The event completed after PostgreSQL returned.
- Kafka restarted: librdkafka reconnected, the processor stayed alive, and the next published event completed.

### Errors and fixes

Kafka poll error events previously raised out of the outer loop and could terminate the processor. They now increment `kafka_errors`, log the condition, apply jittered backoff, and continue.

During a long dependency failure, pausing only the source partition allowed heartbeat polls to collect records from other partitions in a local deque. The processor now pauses all assigned partitions until the current valid event reaches a terminal outcome. This introduces head-of-line blocking but bounds memory and keeps the recovery rule simple.

The complete 22-case matrix is in `docs/failure-experiments.md`.

## Checkpoint 16: drift integration

### Concept

Data drift, action drift, and performance drift are different signals:

- feature and score PSI describe distribution change;
- ALLOW/REVIEW/BLOCK shares describe policy-output change;
- precision and recall require matured labels.

The export keeps these concepts separate and includes versions so incompatible populations are not silently mixed.

### Core logic

```python
record = {
    "schema_version": "fraud-monitoring-export-v1",
    "feature_version": decision.feature_version,
    "model_version": decision.model_version,
    "policy_version": decision.policy_version,
    "features": decision.feature_snapshot,
    "score": decision.score,
    "action": decision.action,
}
if label is not None and label.available_at <= as_of:
    record["label"] = label.label
```

The full raw event is omitted from the default export to reduce unnecessary data exposure.

Key files: `src/fraud_engine/drift.py`, `docs/integration-with-drift-monitoring.md`, `tests/test_drift.py`.

## Checkpoint 17: polish and repeatability

The README now records the exact guarantees, host ports, component commands, routes, measurements, and limits. PowerShell scripts start and stop only project processes whose executable matches the virtual environment's Python. CI checks formatting, lint, compilation, and deterministic tests. The interview guide explains actual project decisions rather than generic streaming answers.

### Environment errors and fixes

After Docker's disk image moved to D:, stale Docker Desktop processes left the backend unavailable. Docker Desktop was restarted cleanly. The project PostgreSQL container then conflicted with a Windows PostgreSQL process on host port 5432. The project mapping and default database URL were changed to 5433, and migrations and live tests were rerun.

A process launch mistake used `python -m fraud_engine.kafka_io`, whose module guard starts the validation probe. The processor must use `fraud-process` or `python -m fraud_engine.processor_cli`. The demo script now uses the dedicated processor entry point.

## Checkpoint 18: final architecture audit

The audit traced every system invariant to code and tests, searched documentation for stale guarantees, checked migration/model parity, and reran unit and live integration gates. It produced two additional hardening changes:

- PostgreSQL check constraints now enforce transaction status, attempt count, action, inference status, score range, label, and label source domains.
- The review label's denormalized transaction ID now has a foreign key, and `available_at` is non-null.

The audit also corrected stale documentation that said the local review endpoint was authenticated and older retry text that described pausing only one partition.

See `docs/final-architecture-review.md` for guarantees, traceability, open risks, and deployment boundaries.

## How to study this project

1. Read `docs/system-invariants.md` and follow each invariant into its named test in the final review.
2. Trace one event through `validate_record`, `FraudProcessor.process`, `RedisFeatureEngine.process`, `InferenceService.predict`, `decide`, and `_finalize`.
3. Compare the crash hooks with the durable state that exists at each line.
4. Run one outage experiment and inspect the transaction status, attempt count, processor log, committed offset, and final decision.
5. Compare the score-based evaluator to decision-outcome metrics to understand why ML quality and business policy must be reported separately.
