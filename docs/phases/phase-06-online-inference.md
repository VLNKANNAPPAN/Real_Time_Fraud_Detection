# Phase 06 — online ML inference

## Concept and contract

Training saves one scikit-learn `Pipeline`, not separate preprocessing and model objects. Its `ColumnTransformer` is fitted on chronological training data only. `model_row(event, snapshot)` is shared by offline frame construction and online inference, with `MODEL_COLUMNS` as one ordered contract. The artifact manifest lists every column, numeric/categorical grouping, `features-v1`, a model version suffixed by the artifact hash, training-frame hash, dataset version, exact scikit-learn version, and SHA-256 of the joblib file. `InferenceService` refuses to load when the file is missing, hash differs, feature version differs, column order differs, scikit-learn version differs, or the loaded pipeline's `feature_names_in_` differs from the manifest. Only locally generated trusted joblib files should be loaded because pickle-family formats can execute code.

Important load guard:

```python
if self.manifest["artifact_sha256"] != sha256(pipeline_path.read_bytes()).hexdigest():
    raise ArtifactMismatch("pipeline hash differs from manifest")
if tuple(self.manifest["columns"]) != MODEL_COLUMNS:
    raise ArtifactMismatch("feature columns or order incompatible")
```

At prediction time, missing features raise `InvalidFeatures`; non-finite numeric values, invalid categories, and feature-version mismatch fail explicitly. The service measures only the local `predict_proba` call and DataFrame preparation with `perf_counter`, returning `fraud_probability`, model version, feature version, and inference latency. The score is a model output, not proof of fraud or a calibrated probability guarantee. A timeout/failure fallback is a decision-engine concern and must not be represented as a made-up score.

## Verification, errors, and limits

Tests train a fresh deterministic artifact, compare online inference to direct use of the saved pipeline on a known offline vector, and exercise missing, NaN, version-mismatched, and hash-tampered inputs. The locally generated default artifact is under ignored `artifacts/baseline-v1`; it can be reproduced with `fraud-generate` and `fraud-train` (or the module commands). Strict library-version checking requires retraining or a reviewed migration when scikit-learn changes. The generated test run's weak PR-AUC is described in [phase 03](phase-03-baseline-model.md); no production model-performance claim is made.
