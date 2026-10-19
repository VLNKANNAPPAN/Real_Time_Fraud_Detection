# Checkpoint 9 — Resilience and explicit failure policy

## Outcome

Checkpoint 9 turns dependency and data failures into named outcomes. The processor never silently converts an unavailable dependency into ALLOW. Retryable failures retain the Kafka offset; safe terminal fallbacks are persisted as REVIEW; invalid records advance only after the DLQ broker acknowledges them.

Core types and timeout/backoff helpers are in `src/fraud_engine/resilience.py`. Runtime classification is split between `processing.py` and `kafka_io.py`.

## Failure policy

| Failure | Classification | Durable result | Offset behavior |
|---|---|---|---|
| PostgreSQL unavailable | retryable | no new terminal decision; PENDING may already exist | do not commit |
| Redis unavailable | retryable | PENDING remains | do not commit |
| Redis snapshot/rebuild failure | retryable | PENDING remains | do not commit |
| Unexpected feature calculation failure | retryable feature error | PENDING remains | do not commit |
| Per-user history safety limit | deterministic fallback | REVIEW / `FEATURE_HISTORY_LIMIT`, no score, excluded from history | commit after DB commit |
| Model load or prediction failure | terminal fallback | REVIEW / `MODEL_UNAVAILABLE`, no fabricated score/version | commit after DB commit |
| Inference deadline exceeded | terminal fallback | REVIEW / `MODEL_TIMEOUT`, late result ignored | commit after DB commit |
| Beyond lateness bound | terminal fallback | REVIEW / `LATE_EVENT`, no score, excluded from history | commit after DB commit |
| Malformed or unsupported event | non-retryable | acknowledged DLQ record | commit after DLQ ack |
| Identity conflict | non-retryable | acknowledged DLQ record; original DB row unchanged | commit after DLQ ack |
| DLQ producer unavailable | retryable Kafka failure | no acknowledged quarantine | do not commit |
| Offset commit failure | Kafka redelivery | terminal DB row already exists | retry and short-circuit |
| Unknown processor exception | retryable/alert | no assumed outcome | do not commit |

## Bounded exponential backoff

`RetryPolicy` grows delays exponentially, applies ±20% jitter, and caps the delay:

```python
base = min(maximum_seconds, initial_seconds * (2 ** (attempt - 1)))
delay = base * (1 + random.uniform(-jitter_fraction, jitter_fraction))
```

Jitter prevents several consumers or services from retrying in lockstep after a shared outage. There is no finite retry count that discards a valid event. Operational alerts should fire from attempt count, elapsed PENDING age, consumer lag, and dependency health.

The phase 9 runtime paused the failing source partition during retry and retained records returned by heartbeat polls. Checkpoint 15 tightened this after observing that a long outage could grow the pending queue from other partitions. The current runtime pauses all assigned partitions, calls `poll(0)` to serve the consumer protocol, and resumes after the current record reaches a terminal outcome.

## Model timeout and fallback

Inference runs in a one-worker executor and waits for a configured deadline. On timeout, cancellation is requested and the future's eventual result is ignored. The decision record has:

```json
{
  "action": "REVIEW",
  "reason_codes": ["MODEL_TIMEOUT"],
  "score": null,
  "model_version": null,
  "inference_status": "FAILED"
}
```

This distinction matters: zero risk would be a fabricated prediction, while `null` explicitly says the model did not provide a usable answer. The feature snapshot is retained and remains feature-included because Redis completed successfully.

If the artifact cannot load during process startup, `UnavailablePredictor` keeps the stream operational in the same REVIEW fallback mode. The processor does not load an unverified artifact or substitute a different model version.

Python cannot forcibly stop arbitrary running code in a thread. The current deadline guarantees that the stream decision ignores the late result and moves on; a native call that hangs forever could leave one executor thread alive. Process isolation is the next hardening step for untrusted or potentially hanging model runtimes.

## Malformed records and DLQ ordering

`process_fraud_record` validates JSON, the Pydantic event contract, key/payload user consistency, and future-clock tolerance. For invalid records it publishes a DLQ envelope containing source topic, partition, offset, key, original value in hexadecimal, reason, and quarantine time.

The source offset is committed only after `producer.flush()` confirms the DLQ write. A failed DLQ write raises `RetryableKafkaError`, keeping the source offset available for retry. DLQ records themselves may be duplicated, which is acceptable because their source coordinates provide a stable inspection key.

## Health reporting

`GET /system/health` performs bounded checks against PostgreSQL, Redis, and Kafka and returns `ok` only when all three respond. Any failure returns `degraded` with component status. Health reporting is observational; it does not change transaction decisions or reset state.

## Tests added

The resilience tests cover:

- Redis connection failure produces a retryable error, a PENDING claim, and no decision;
- model exception produces REVIEW with `MODEL_UNAVAILABLE` and null score/version;
- slow inference produces REVIEW with `MODEL_TIMEOUT`;
- malformed input commits only after the fake DLQ producer acknowledges;
- a retryable processor failure performs no consumer commit;
- retry delay boundaries are deterministic when a seeded random generator is supplied;
- health can truthfully report a degraded dependency.

## Errors encountered and resolutions

### Docker daemon unavailable after storage relocation

Docker CLI initially reported no `docker_engine` named pipe while Docker Desktop applied the disk-location change. Work continued through deterministic SQLite/fakeredis and transport-fake tests rather than weakening checks. Live integration was deferred until the daemon became available, then migrations and container-backed tests were retried.

### Old health test assumed unconditional success

The scaffold test expected `{"status": "ok"}` even with all containers stopped. That assertion contradicted checkpoint 9. It was replaced with a contract check accepting `ok` or `degraded` while requiring explicit PostgreSQL, Redis, and Kafka component entries.

### Feature exceptions needed a named retry class

Redis transport exceptions were already retryable, but unexpected feature decoding/calculation errors initially escaped as generic exceptions. They are now wrapped as `RetryableFeatureError`, making the offset policy explicit while preserving the original exception as the cause.

### Existing editable environment could not refresh the console script offline

After adding `fraud-process`, pip's isolated editable build tried to download `setuptools`, but network access is intentionally restricted and this virtual environment did not contain setuptools. The project keeps the console entry point for fresh installations and also provides `python -m fraud_engine.processor_cli`, which runs directly from the existing editable source path without downloading build tools.

## Operational signals

Production monitoring should alert on:

- oldest PENDING age;
- attempt count and retry delay;
- Kafka consumer lag and partition assignment changes;
- Redis rebuild count and duration;
- model fallback rate and inference latency percentiles;
- DLQ rate by reason;
- REVIEW capacity and unlabeled REVIEW age.
