import json

import pandas as pd
import pytest

from fraud_engine.events import TransactionEvent
from fraud_engine.features import OfflineFeatureEngine
from fraud_engine.inference import ArtifactMismatch, InferenceService, InvalidFeatures
from fraud_engine.simulator import generate_dataset
from fraud_engine.training import MODEL_COLUMNS, make_training_frame, model_row, train_and_save


@pytest.fixture(scope="module")
def trained(tmp_path_factory):
    data = generate_dataset(seed=21, n_users=80, n_transactions=2500)
    path = tmp_path_factory.mktemp("model")
    train_and_save(make_training_frame(data.events, data.labels), path)
    return data, path


def test_online_prediction_matches_offline_pipeline(trained):
    data, path = trained
    service = InferenceService(path)
    event = TransactionEvent.model_validate(data.events.iloc[0].to_dict())
    snapshot = OfflineFeatureEngine().process(event).snapshot
    result = service.predict(event, snapshot)
    direct = service.pipeline.predict_proba(
        pd.DataFrame([model_row(event, snapshot)], columns=list(MODEL_COLUMNS))
    )[0, 1]
    assert result.fraud_probability == pytest.approx(float(direct))
    assert 0 <= result.fraud_probability <= 1
    assert result.model_version.startswith("fraud-lr-balanced-v2+")
    assert result.explanations
    assert result.inference_latency_ms >= 0


def test_missing_invalid_or_incompatible_feature_fails(trained):
    data, path = trained
    service = InferenceService(path)
    event = TransactionEvent.model_validate(data.events.iloc[0].to_dict())
    snapshot = OfflineFeatureEngine().process(event).snapshot
    with pytest.raises(InvalidFeatures):
        service.predict(
            event, {key: value for key, value in snapshot.items() if key != "transaction_count_5m"}
        )
    with pytest.raises(InvalidFeatures):
        service.predict(event, {**snapshot, "transaction_count_5m": float("nan")})
    with pytest.raises(ArtifactMismatch):
        service.predict(event, snapshot, "features-v0")
    manifest_path = path / "manifest.json"
    original = manifest_path.read_text(encoding="utf-8")
    manifest = json.loads(original)
    manifest["feature_version"] = "features-v0"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    try:
        with pytest.raises(ArtifactMismatch):
            InferenceService(path)
    finally:
        manifest_path.write_text(original, encoding="utf-8")
    pipeline_path = path / "pipeline.joblib"
    original_bytes = pipeline_path.read_bytes()
    pipeline_path.write_bytes(original_bytes + b"tampered")
    try:
        with pytest.raises(ArtifactMismatch):
            InferenceService(path)
    finally:
        pipeline_path.write_bytes(original_bytes)
