# Phase 03 — offline features and baseline model

## Feature semantics

`OfflineFeatureEngine` processes transactions in producer/arrival order. Before inserting the current event, it chooses previously completed events with `t-24h <= event_timestamp < t`. This excludes the current event, equal-time events, and future event-time information. A per-user completed-event watermark rejects events more than two minutes late. Exact duplicate event ID plus digest returns the frozen snapshot; conflicting identity raises. The same feature names and ordering are declared in `FEATURE_NAMES` and the model manifest. See [feature definitions](../feature-definitions.md).

Critical reference code:

```python
prior = [item for item in history if t - HORIZON <= item.event_timestamp < t]
average = sum(item.amount for item in prior) / len(prior) if prior else 0.0
previous = max((item.event_timestamp for item in prior), default=None)
```

No-history values have explicit indicator columns. A ratio of zero on cold start is distinguishable from a true ratio by `has_amount_history_24h`. Offline replay uses original arrival order; sorting by event time would fabricate history that had not arrived when a decision was made.

## Pipeline and evaluation

`make_training_frame` joins delayed labels only after feature replay and carries `label_available_at` as split metadata, never a model column. `chronological_splits` uses the first 45% of elapsed producer time for training, a two-day maturation gap, validation through 78% of elapsed time, another two-day gap, and the remaining time for untouched test. It asserts every training label was available by validation start and every validation label by test start. Gap transactions still contribute to feature history because they really arrived; only their labels are excluded from training and threshold selection. Numeric imputation/scaling and categorical one-hot encoding live inside a scikit-learn `Pipeline`; they fit on train only. The model is regularized logistic regression. Validation selects a threshold by F1, and the test set reports precision, recall, F1, PR-AUC, ROC-AUC, confusion matrix, false-positive rate, and false-negative rate. Accuracy is deliberately absent from the summary. A locally generated `pipeline.joblib`, SHA-256 manifest, and report go under ignored `artifacts/`.

The corrected default 6,000-row/80-user run had 2,716/1,556/941 train/validation/test rows after maturation gaps. Validation prevalence was 1.99%; test prevalence was 1.59%. The validation-chosen F1 threshold was 0.08427. Validation PR-AUC was 0.0681 and test PR-AUC was 0.0225; test ROC-AUC was 0.610. Test precision/recall/F1 at that threshold were all zero; the test confusion matrix was `[[900, 26], [15, 0]]`, so 15 fraud labels were missed and 26 legitimate transactions were flagged. These are poor results and make the threshold unsuitable as an automatic BLOCK policy. They are real local results, not claims of production quality. Stochastic labels, sparse 24-hour histories, hidden profile risk, and only 15 fraud labels in the test split limit inference; future modeling work should examine more seeds and cohorts without tuning on this test set.

**Second-pass errors found:** The initial 60/20/20 chronological row split still used training labels that would only arrive after the simulated training date. Chronological ordering alone did not prevent this delayed-label leakage. We shortened the synthetic delay to a documented one-to-two-day range, inserted two-day gaps, asserted label maturation, and added a regression test that injects an unavailable label. The earlier metrics were discarded and the model retrained. The gap periods are real data for streaming feature replay, never a source of future labels. A fixed family name also could have labeled different retrained artifacts identically; the stored model version now includes the artifact hash prefix and the manifest records the training-frame hash.

## Score versus action

The F1 threshold is a model-analysis output, not a business action threshold. A fraud operation has review capacity and different costs for false positives and false negatives. Checkpoint 7 uses a separate conservative versioned policy and requires corroborating signals for BLOCK. The model output is treated as an estimate whose calibration remains unproven.
