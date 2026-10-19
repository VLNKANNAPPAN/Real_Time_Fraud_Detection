import json
from datetime import timedelta

import pytest

from fraud_engine.simulator import generate_dataset
from fraud_engine.training import (
    MODEL_COLUMNS,
    chronological_splits,
    make_training_frame,
    train_and_save,
)


def test_chronological_feature_frame_and_artifact(tmp_path):
    data = generate_dataset(seed=11, n_users=100, n_transactions=2000)
    frame = make_training_frame(data.events, data.labels)
    train, validation, test = chronological_splits(frame)
    assert train["producer_timestamp"].max() <= validation["producer_timestamp"].min()
    assert validation["producer_timestamp"].max() <= test["producer_timestamp"].min()
    assert "label_available_at" not in MODEL_COLUMNS
    assert train["label_available_at"].max() <= validation["producer_timestamp"].min()
    assert validation["label_available_at"].max() <= test["producer_timestamp"].min()
    report = train_and_save(frame, tmp_path)
    assert 0 <= report["test_metrics"]["pr_auc"] <= 1
    assert (tmp_path / "pipeline.joblib").is_file()
    assert (tmp_path / "manifest.json").is_file()
    manifest = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["model_version"].endswith(manifest["artifact_sha256"][:12])
    assert len(manifest["training_data_sha256"]) == 64
    assert "feature_diagnostics" in report
    assert "hist_gradient_boosting" in report["model_comparison"]
    assert "recall_at_5pct_review" in report["model_comparison"]["logistic"]["test"]


def test_split_rejects_unmatured_training_label():
    data = generate_dataset(seed=15, n_users=50, n_transactions=800)
    frame = make_training_frame(data.events, data.labels)
    frame.loc[frame.index[0], "label_available_at"] = frame["producer_timestamp"].max() + timedelta(
        days=1
    )
    with pytest.raises(ValueError, match="training label not mature"):
        chronological_splits(frame)
