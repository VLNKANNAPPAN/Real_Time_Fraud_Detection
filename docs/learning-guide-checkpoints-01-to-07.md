# Learning guide: Checkpoints 1–7

This guide explains the implemented code, what the experiments proved, and what still waits for checkpoint 8. Read the individual [phase records](phases/phase-01-scaffolding.md) for commands and full test details. The [architecture](architecture.md) and [invariants](system-invariants.md) remain the intended end-to-end contract.

## 1. What each component owns

| Component | Code | Current responsibility |
|---|---|---|
| Event contract | `src/fraud_engine/events.py` | Validate immutable v1 envelopes and calculate a canonical digest. |
| Simulator | `src/fraud_engine/simulator.py` | Generate deterministic users, transactions, and separate delayed labels. |
| Offline reference | `src/fraud_engine/features.py` | Define event-time feature behavior independently of Redis storage. |
| Baseline trainer | `src/fraud_engine/training.py` | Replay features, enforce mature chronological splits, fit preprocessing + logistic regression, report metrics, version artifact. |
| Kafka probe | `src/fraud_engine/kafka_io.py` | Publish raw events, validate deliveries, acknowledge a DLQ, and manually commit probe offsets. |
| Redis feature engine | `src/fraud_engine/redis_features.py` | Atomically read prior history, freeze a snapshot, deduplicate, insert, and prune. |
| Inference | `src/fraud_engine/inference.py` | Validate artifact/schema/runtime and score an online vector. |
| Decision policy | `src/fraud_engine/decision.py` | Turn score plus explicit signals into ALLOW/REVIEW/BLOCK and reason codes. |
| Persistence | `src/fraud_engine/persistence.py` | Save an already-made decision with its frozen snapshot in one caller-owned DB transaction. |

The Kafka probe is **not** the final fraud processor. It owns a separate group. There is no deployed path yet that atomically coordinates Kafka offset, Redis state, and PostgreSQL decision. The recovery design exists in ADR 0002; the implementation belongs to checkpoint 8.

## 2. Identities, ordering, and offset commits

`event_id` names the immutable envelope; `transaction_id` names the business transaction. `TransactionEvent.canonical_bytes()` serializes normalized fields with sorted keys, and `digest()` hashes those bytes. Exact replay therefore has the same digest; changed content under the same ID is a conflict. PostgreSQL has unique constraints for both IDs, but the full claim/replay protocol has not been implemented.

Kafka events are keyed by `user_id`. That places a user's records in one partition and gives offset order **within that partition**. Event time can still be out of order. The probe's central rule is:

```python
event = validate_record(message)
handler(event)
consumer.commit(message=message, asynchronous=False)
```

If validation fails, it publishes to `fraud.dead-letter` and waits for acknowledgment **before** committing the source offset. Handler failure or DLQ failure leaves the offset uncommitted. Kafka can redeliver after a crash; the probe intentionally does not claim fraud-processing deduplication. Live tests verified per-user partition order, distribution of multiple users, restart from committed offset, DLQ content, and broker restart.

## 3. Event time and frozen features

Every online window is `[event_timestamp - W, event_timestamp)`. The lower edge is included, and the current/equal-time event is excluded. A late but still eligible event sees only already-arrived completed events whose event times are earlier than its own. Decisions already made are not rewritten. Beyond the two-minute watermark tolerance, the normal feature engine returns `LATE_EVENT` without mutating history.

The offline reference computes the 24-hour prior set before appending the event:

```python
prior = [x for x in history if t - HORIZON <= x.event_timestamp < t]
snapshot = calculate_snapshot(event, history)
history.append(event)
```

Redis stores a per-user sorted set of transaction IDs by integer UTC microseconds, an immutable entry hash, an event-ID index, watermark/generation metadata, and a saved pre-event snapshot. One Lua script performs identity check, lateness check, pruning, feature calculation, snapshot save, and insertion. This avoids a process crash between separate Redis commands leaving half-mutated state. Exact duplicate delivery returns the saved snapshot. `restore_completed` can atomically rebuild the cache from completed events supplied by a future PostgreSQL loader. The loader and lock remain checkpoint 8 work.

The most important Redis defect found was timestamp precision loss: Lua converted a ~16-digit microsecond number to a shortened string for a sorted-set bound. A same-time event was then counted as prior history. The fix passes the exact integer as a decimal string and formats computed bounds with no decimal places. Another defect checked the history cap before removing expired events; pruning now happens first. Regression tests cover both.

## 4. Synthetic data and the label-time leak

The generator samples labels from several noisy risk factors and interactions. Labels live in a separate table with `label_available_at` one or two days after the event. Neither the hidden label probability nor the label timestamp enters `MODEL_COLUMNS`. The deterministic seed makes failures reproducible, but the generated labels are not evidence of production fraud behavior.

Features replay in original producer arrival order. The first trainer split was chronological by rows, yet it still used labels that would not have arrived when the model was supposedly trained. The corrected splitter uses a two-day label maturation gap between train and validation and another between validation and test, then asserts:

```python
max(train.label_available_at) <= validation_start
max(validation.label_available_at) <= test_start
```

Transactions in the gaps still enter streaming feature history; their labels are simply unavailable for model fitting or threshold selection. The old metrics were discarded, the artifact retrained, and a regression test now injects a future label to ensure the splitter rejects it.

## 5. Training-serving consistency and artifact identity

The trainer fits a single scikit-learn `Pipeline`: numeric median imputation and scaling, categorical one-hot encoding with unknown-category handling, then logistic regression. `model_row` and `MODEL_COLUMNS` define the same feature order offline and online. The manifest records the feature version, ordered columns, training-frame hash, dataset version, scikit-learn version, and joblib SHA-256. The model version includes the artifact hash prefix, so a retrain cannot silently reuse the same version label.

`InferenceService` rejects a missing/tampered artifact, wrong feature version/order, incompatible scikit-learn version, missing feature, and non-finite numeric input. It never substitutes a default score. A known offline vector gets the same result through direct pipeline use and the online wrapper. This proves local parity for that vector, not calibration or fraud quality.

The corrected default run had 2,716 train, 1,556 validation, and 941 test examples. Test PR-AUC was **0.0225** at 1.59% prevalence; ROC-AUC was **0.610**. The validation-selected F1 threshold caught **0 of 15** test fraud labels. The small, noisy dataset and sparse user history make this an intentionally modest baseline. See [phase 03](phases/phase-03-baseline-model.md) for the complete confusion matrix and limitations.

## 6. Why a model score is not an action

The policy uses score bands and four explainable signals: new device in 24h, high five-minute velocity, unusually high amount relative to nonempty 24h history, and foreign country. With the current config, REVIEW requires score at least 0.075 or three signals. BLOCK requires score at least 0.40 **and** three signals. Score alone cannot BLOCK. The policy version and reason codes are persisted alongside model and feature versions.

Validation-only policy analysis targeted roughly 5% review capacity. On the corrected validation split, 58/1,556 transactions went to REVIEW, including 5/31 fraud labels; none BLOCK. On untouched test, 40/941 went to REVIEW, including 1/15 fraud labels; none BLOCK. The default policy is therefore demonstrative and unsuitable for a live authorization gate. Model failure/timeout returns REVIEW with no fabricated score or model version; late-event fallback also returns REVIEW without a normal snapshot.

The live PostgreSQL persistence test found that adding both ORM rows without an explicit relationship let SQLAlchemy flush `decisions` before its parent `transactions`. SQLite's default foreign-key behavior had missed the issue. `save_decision` now flushes the parent first, then the child, within the same caller-owned transaction. Concurrency-safe duplicate persistence remains checkpoint 8 work.

## 7. Error log and resolution

| Error or finding | Evidence | Resolution |
|---|---|---|
| Python dependency install denied outbound socket | pip failed before downloading setuptools | Repeated with approved network access into D: `.venv` and D: cache/temp. |
| Docker daemon unavailable/API denied in default shell | `docker info` failed | Started Docker Desktop and used approved daemon access; monitored C: space and stopped it after tests. |
| Intermittent local DB connection stall | `localhost` SQLAlchemy connection hung while direct IPv4 worked | Used `127.0.0.1` defaults and a five-second connect timeout. |
| Kafka topic missing | Probe received `UNKNOWN_TOPIC_OR_PART` | Added Admin API topic initialization before probe startup. |
| Repeated topic init failed | Admin API rejected an empty `create_topics` list | Made an existing-topic run a successful no-op and added a unit test. |
| Kafka integration test saw unrelated records | Fresh group/`latest` behavior raced with assignment and prefetch | Wait for all assignments, seek to high watermarks, filter unique target IDs, assert committed restart offsets. |
| Redis microsecond boundary mismatch | Online counted an equal-time event excluded offline | Exact decimal score strings and a microsecond regression sequence. |
| False Redis history-cap rejection | Full but expired history was checked before prune | Prune first, then enforce cap; regression test after a 25-hour gap. |
| Delayed-label leakage | Initial train split used not-yet-available labels | Two-day maturation gaps, assertions, retraining, and regression test. |
| Ambiguous model version after retraining | Static baseline family label | Include artifact hash prefix in version and validate file hash. |
| PostgreSQL FK violation | Live decision INSERT preceded transaction INSERT | Flush parent before child in one DB transaction; live test passes. |

## 8. What the tests do and do not prove

`ruff check`, `ruff format --check`, pytest unit tests, live PostgreSQL migration, live Redis Lua parity, live Kafka offset/key/DLQ tests, and broker restart were run on this machine. They prove the tested component contracts and local infrastructure startup. They do **not** prove one end-to-end durable decision per Kafka event, cross-service crash recovery, hot-user throughput, calibrated probability, or useful fraud detection. Those require checkpoints 8 onward. All local services were stopped after verification.
