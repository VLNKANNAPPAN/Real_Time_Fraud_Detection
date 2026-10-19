# Phase 07 — explainable decision engine

## Score and action are different

`InferenceService` returns a numeric estimate. `decide` applies `config/decision-policy-v1.json` to the estimate and explicit event-time signals, returning ALLOW, REVIEW, or BLOCK plus ordered reason codes, `policy_version`, score/model/feature versions, and inference status. `save_decision` writes those fields and the frozen feature snapshot to the SQLAlchemy decision table within a caller-owned DB transaction. This is standalone persistence; Kafka/Redis/DB orchestration and concurrency-safe idempotency are checkpoint 8 work.

The current demo policy is:

```python
if score >= block_score and len(signals) >= minimum_block_signals:
    return BLOCK
if score >= review_score or len(signals) >= minimum_review_signals:
    return REVIEW
return ALLOW
```

The actual implementation also returns precise reason codes. Signals are `NEW_DEVICE_24H`, `HIGH_TX_VELOCITY` (at least three prior transactions in five minutes), `UNUSUAL_AMOUNT` (at least triple a nonempty 24h mean), and `FOREIGN_COUNTRY` (not IN). `review_score=0.075`, `block_score=0.40`, `minimum_review_signals=3`, and `minimum_block_signals=3` are **prototype** values, not production authorization advice. Score alone cannot BLOCK. A model outage or timeout yields REVIEW with `MODEL_UNAVAILABLE` or `MODEL_TIMEOUT`, null score/model version, and the available feature snapshot. A too-late event yields REVIEW with `LATE_EVENT`, no score/snapshot, and `feature_included=false`.

## Threshold selection, costs, and capacity

The model's corrected validation F1 threshold of 0.08427 is not reused as a business rule. On the default deterministic 80-user dataset, the baseline has weak discrimination and only 31/15 fraud labels in validation/test. We explored **validation only** for an approximate 5% review capacity. With the chosen policy, 58 of 1,556 validation transactions go to REVIEW (3.73%), including 5 of 31 simulated fraud labels; no validation transactions BLOCK. The conservative block condition reflects the cost of false blocks. On the corrected validation split, a candidate `block_score=0.20` with two signals would have blocked 2 transactions, 1 legitimate and 1 fraudulent. That tiny, noisy sample is inadequate evidence for automatic blocking, so the policy uses 0.40 plus three signals. The untouched test split is reported separately after freezing the policy. Low capture means this policy is not suitable for a real payment gate.

After the policy was frozen, the corrected 941-row test split sent 40 transactions to REVIEW (4.25%), including 1 of 15 simulated fraud labels; it issued no BLOCK decisions. Thus 39 legitimate transactions consumed review capacity and 14 fraud labels remained in ALLOW. False positives may inconvenience customers or reject sales; false negatives allow losses; manual review consumes limited staff time. REVIEW is the uncertainty path. A real deployment would need validated cost estimates, review SLA, appeal/correction handling, calibration checks, and separate policy approval before any automatic BLOCK. Fallback REVIEW may itself overwhelm capacity during an outage, so operational monitoring is required.

## Verification and limitations

Boundary tests cover values just below/at review and block thresholds, score-only high risk, multiple corroborating signals, rule-driven REVIEW, and fallbacks without fabricated score. An SQLite persistence test verifies reason codes, versions, score, and model-failure snapshot; a real PostgreSQL test checks the stored decision against the migrated schema. That live test found a genuine FK-order bug: adding parent and child ORM rows without a declared relationship let SQLAlchemy flush `decisions` before `transactions`. SQLite's default disabled foreign-key enforcement had hidden it. `save_decision` now flushes the transaction row before adding the decision row; both remain in the caller's single DB transaction. `save_decision` does not yet protect concurrent duplicate writes or decide when Kafka offsets advance. Those are specifically deferred to checkpoint 8.
