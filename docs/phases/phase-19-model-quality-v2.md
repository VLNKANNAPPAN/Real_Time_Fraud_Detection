# Phase 19 — model quality, security, and live burst audit

## Data and diagnostics

Synthetic v2 contains 200,000 transactions and 3,545 fraud labels (1.7725%). It generates noisy
account-takeover, card-testing, and amount-anomaly campaigns plus rare background fraud. The report
contains fraud/legitimate distributions and single-feature ROC-AUC for every engineered feature.
A hidden-variable ceiling experiment is isolated from the serving contract.

## Untouched chronological test results

| Model | PR-AUC | Recall in exact 5% review budget |
|---|---:|---:|
| Prior / random | 0.015 | 0.050 |
| Rules only | 0.167 | 0.391 |
| Logistic regression | 0.647 | 0.822 |
| LR, balanced class weights | 0.647 | 0.834 |
| HistGradientBoosting | 0.738 | 0.886 |
| Serving model + rules | 0.647 | 0.834 |

The serving balanced-LR threshold `0.6020968715` was selected on validation for a 5% review
budget. Applied unchanged to test, it reviewed 4.61% and recalled 81.72% of fraud. Test PR-AUC 95%
bootstrap CI is `[0.604, 0.697]`; recall CI is `[0.782, 0.853]`. Test Brier score is 0.0469 and ECE
is 0.130, so the score is not presented as a calibrated probability.

## Explainability and security

Online logistic inference stores the five largest absolute `coefficient × transformed value`
contributions with every decision. FastAPI returns them and Streamlit shows them in the review
queue. Review reads and writes require `X-Review-API-Key`; the service fails closed when
`FRAUD_REVIEW_API_KEY` is absent. CI runs formatting, lint, compilation, tests, and `pip-audit`.
The local audit completed with no known third-party vulnerabilities after upgrading pip to 26.2.1;
the project package itself is skipped because it is local and not published on PyPI.

## Reliability and load evidence

The existing crash-after-Redis and replay tests remain. A 500-event direct live run measured 25.19
decisions/s, 33.61 ms p50, 44.92 ms p95, and zero pre-existing Kafka lag. Kafka accepted a separate
1,000-event burst at 4,157.86 events/s; the single synchronous processor immediately accumulated
947 lag and later drained it to zero. Therefore the prototype accepts a 500–1,000 events/s burst
but does **not** sustain that decision rate. Scaling the processor is required before making such a
capacity claim.

A live Redis interruption left the injected event durably `PENDING` with Kafka lag 1 and no offset
commit. After Redis restarted, retry attempt 7 produced exactly one `DECIDED` row and lag returned
to zero. No fallback ALLOW was issued during the outage.
