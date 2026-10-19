# ADR 0004: behaviorally learnable synthetic data and features-v2

Status: implemented.

## Decision

Synthetic v2 generates noisy account-takeover, card-testing, and amount-anomaly campaigns whose
patterns are observable at decision time. Hidden campaign variables are written only to
`generator_truth.csv` for a ceiling experiment and never enter the serving feature contract.

`features-v2` adds amount z-score, recent small-payment count, merchant diversity, device age, and
hour deviation while retaining the v1 event-time, deduplication, lateness, and recovery semantics.
Offline Python and online Redis/Lua implementations remain separate and must pass parity tests.

The serving artifact remains balanced logistic regression because it supports per-decision
coefficient contributions. HistGradientBoosting is retained as a measured challenger and currently
has higher ranking quality. Promotion requires an explanation design and a separately versioned
artifact/policy decision.

## Evidence

On the deterministic 200,000-row dataset, test prevalence is 1.53%. Balanced logistic regression
has PR-AUC 0.647 and recall 0.834 in an exact top-5% review budget. HistGradientBoosting has PR-AUC
0.738 and recall 0.886. The hidden-variable ceiling has PR-AUC 0.792.

## Limits

This remains synthetic evidence. Class weighting makes the logistic score poorly calibrated
(test ECE 0.130), so policy thresholds are validation-selected operating points rather than claims
of true fraud probability.
