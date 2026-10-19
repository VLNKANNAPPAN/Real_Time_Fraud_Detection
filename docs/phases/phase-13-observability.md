# Checkpoint 13 — Observability

## Structured logs

The Kafka boundary emits JSON logs containing event ID, transaction ID, user ID, source partition/offset, outcome, action, reasons, model version, feature version, and processing latency. Retry and quarantine logs carry the error class and source coordinates.

No raw card-like payload is placed in normal logs. The DLQ contains the original event bytes as hexadecimal for this synthetic project; production systems should encrypt or store a controlled reference according to data policy.

## Runtime metrics

`observability.py` maintains bounded process-local counters and latency samples:

- transactions processed;
- ALLOW/REVIEW/BLOCK;
- duplicate and late events;
- consumer, Redis, and PostgreSQL errors;
- DLQ count;
- processing, feature, inference, persistence, and HTTP latency P50/P95/P99.

The sample deque is bounded to avoid an observability memory leak. `/metrics/runtime` exposes JSON and `/metrics` exposes Prometheus text.

## Durable operational metrics

`/metrics/summary` derives restart-safe values from PostgreSQL:

- status and action counts;
- labels and fallback reasons;
- duplicate delivery estimate from attempt counts;
- processing latency from claim/terminal timestamps;
- stored model inference latency;
- throughput in the latest one-minute activity window.

Kafka lag is queried from committed offsets and partition high watermarks for `fraud-processor-v1`. When Kafka is down, lag returns `available=false`; it does not invent zero lag.

## Optional Prometheus

The `observability` Compose profile starts one small Prometheus instance with 24-hour retention:

```powershell
docker compose --profile observability up -d prometheus
```

It scrapes the host FastAPI `/metrics` endpoint. Grafana is omitted to preserve memory; Streamlit already provides the local human-readable view.

## Limits

Runtime metrics are process-local and reset on restart. Durable counts come from PostgreSQL. A production deployment would use OpenTelemetry or a Prometheus client with centralized collection and trace correlation. Adding that must not change the decision or offset boundary.
