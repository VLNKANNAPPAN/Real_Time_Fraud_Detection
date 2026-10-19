# Checkpoint 11 — Delayed labels and production evaluation

## Implementation

Predictions and decisions remain immutable. Labels arrive through a separate table keyed to the immutable decision ID and copy the model and feature versions used at decision time. Migration `0003_delayed_labels` adds `label_source` and `available_at` so human and simulated labels have explicit provenance and availability time.

`evaluation.py` provides:

- matured simulated-label import from `data/generated/labels.csv`;
- exclusion of labels whose `label_available_at` is still in the future;
- precision, recall, F1, PR-AUC, ROC-AUC, FPR, FNR, and confusion matrix;
- metrics by event day and model version;
- ALLOW false negatives, BLOCK false positives, and REVIEW fraud conversion;
- explicit exclusion count for `UNSURE` labels.

Model metrics use the configured REVIEW score threshold and scored decisions only. Decision outcome metrics are reported separately. This prevents decision rules from being mistaken for model predictions and keeps score quality separate from intervention outcomes.

## Delayed-label invariant

The original feature snapshot, score, and action are never recomputed when a label arrives. A label is new evidence for evaluation and future training. It cannot influence its own historical prediction.

Human labels enter through the REVIEW endpoint and use the server receipt time. Simulated labels preserve their generated availability timestamp. Historical backfills may be processed after their labels are already mature; the dataset still records that label availability followed producer time.

## Core evaluation flow

```python
decision = find_decision(transaction_id)
if label_available_at <= evaluation_as_of:
    attach_label(
        decision_id=decision.id,
        model_version=decision.model_version,
        feature_version=decision.feature_version,
    )
```

Evaluation ignores unlabeled and `UNSURE` rows for binary metrics. This avoids treating missing outcomes as legitimate transactions or fraud.

## Selection bias

Human REVIEW labels alone are not representative of all traffic because policy selected those cases. The simulated delayed-label importer is useful for demonstrating ALLOW false negatives and BLOCK false positives across the complete synthetic population. Real deployments need outcome sources covering non-reviewed transactions, such as disputes, chargebacks, confirmed account takeover, or sampled legitimate outcomes.

## Interfaces and tests

- `GET /metrics/model-performance` returns current matured-label evaluation.
- `python -m fraud_engine.evaluation` imports available simulated labels and writes a JSON report.
- Tests verify time-gated import, model/day splits, action outcomes, `UNSURE` exclusion, and version retention.

## Errors found

The first evaluation draft used REVIEW/BLOCK actions as the binary prediction for precision and recall. That mixed policy performance with model performance. It was corrected so model threshold metrics use scores, while action-level outcomes remain in their own section.
