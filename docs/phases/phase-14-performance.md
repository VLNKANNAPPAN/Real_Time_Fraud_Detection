# Checkpoint 14 — Controlled performance testing

## Harness

`load_test.py` runs the real claim, Redis feature, model inference, decision, and PostgreSQL terminal-commit path with bounded synthetic traffic. Kafka transport is deliberately excluded from its processor latency measurement and is reported as a scope limitation. Kafka consumer lag is sampled separately after each run.

The harness records:

- events per second;
- end-to-end processor P50/P95/P99;
- Redis feature P50/P95/P99;
- model inference P50/P95/P99;
- PostgreSQL final-persistence P50/P95/P99.

It removes its database and Redis records after the report, uses at most 2,000 events, and supports:

- `normal`;
- `burst`;
- `slow_model` with 20 ms injected delay;
- `slow_db` with 20 ms injected before commit;
- `slow_redis` with 20 ms injected before the atomic Redis operation.

## Commands

```powershell
.\.venv\Scripts\python.exe -m fraud_engine.load_test --scenario normal --events 100
.\.venv\Scripts\python.exe -m fraud_engine.load_test --scenario burst --events 100
.\.venv\Scripts\python.exe -m fraud_engine.load_test --scenario slow_model --events 50
.\.venv\Scripts\python.exe -m fraud_engine.load_test --scenario slow_db --events 50
.\.venv\Scripts\python.exe -m fraud_engine.load_test --scenario slow_redis --events 50
```

## Measured results

Measured on the local Windows 11 / 8 GB machine on 2026-09-23 with live PostgreSQL on port 5433 and live Redis. Runs were sequential and intentionally small.

| Scenario | Events | Events/s | E2E P50 ms | E2E P95 ms | Redis P50 ms | Model P50 ms | DB P50 ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| normal | 50 | 19.47 | 38.51 | 62.25 | 3.69 | 9.18 | 12.06 |
| burst | 50 | 19.55 | 39.65 | 56.77 | 4.05 | 9.81 | 12.19 |
| slow model (+20 ms) | 30 | 11.57 | 70.92 | 90.34 | 7.41 | 33.07 | 16.92 |
| slow DB (+20 ms) | 30 | 10.37 | 78.06 | 101.33 | 8.18 | 12.46 | 40.58 |
| slow Redis (+20 ms) | 30 | 10.77 | 79.71 | 96.25 | 29.66 | 12.66 | 17.43 |

Full P50/P95/P99/mean reports are in `artifacts/performance/`. These are machine-specific evidence, not throughput promises.

The initial run revealed that inference instrumentation covered only the inner scikit-learn method and missed injected wrapper delay. Measurement was moved around the complete predictor call, and all scenarios were rerun. Each injected dependency then showed the expected approximately 20 ms increase in its own component metric.

The direct harness observed Kafka group lag of 271 existing records because it does not consume Kafka and the `fraud-processor-v1` group had no committed offset for those partitions. This is a useful scope check: the direct throughput number cannot claim Kafka end-to-end throughput.

At baseline, final database persistence and model execution are the largest measured components. No batching or concurrency change is justified for this learning workload. Slow dependencies reduce throughput predictably, demonstrating backpressure. Connection pooling, prepared/batched writes, multiple partitions/consumers, and a separate model worker are production options, but each needs a new correctness and load analysis.

## Interpretation rules

Measure before optimizing. A slow external dependency should increase its component latency and reduce event rate predictably. Consumer lag indicates backpressure when arrival exceeds processing. Batching DB writes or parallelizing one user's events is not justified until measurements show a need because either change complicates the current correctness boundary.
