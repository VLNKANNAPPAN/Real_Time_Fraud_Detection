"""Strict model artifact loading and online prediction."""

import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

import joblib
import pandas as pd
import sklearn

from fraud_engine.events import TransactionEvent
from fraud_engine.features import FEATURE_VERSION
from fraud_engine.training import MODEL_COLUMNS, model_row


class ArtifactMismatch(ValueError):
    pass


class InvalidFeatures(ValueError):
    pass


@dataclass(frozen=True)
class Prediction:
    fraud_probability: float
    model_version: str
    feature_version: str
    inference_latency_ms: float
    explanations: tuple[dict[str, float | str], ...] = ()


class InferenceService:
    def __init__(self, artifact_directory: Path) -> None:
        manifest_path = artifact_directory / "manifest.json"
        pipeline_path = artifact_directory / "pipeline.joblib"
        if not manifest_path.is_file() or not pipeline_path.is_file():
            raise FileNotFoundError("model artifact and manifest must both exist")
        self.manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        expected_hash = self.manifest.get("artifact_sha256")
        actual_hash = hashlib.sha256(pipeline_path.read_bytes()).hexdigest()
        if expected_hash != actual_hash:
            raise ArtifactMismatch("pipeline hash differs from manifest")
        if self.manifest.get("feature_version") != FEATURE_VERSION:
            raise ArtifactMismatch("feature version incompatible with service")
        if tuple(self.manifest.get("columns", ())) != MODEL_COLUMNS:
            raise ArtifactMismatch("feature columns or order incompatible")
        if self.manifest.get("sklearn_version") != sklearn.__version__:
            raise ArtifactMismatch("scikit-learn runtime version differs from artifact")
        # Only locally produced, trusted joblib artifacts may be loaded.
        self.pipeline = joblib.load(pipeline_path)
        if tuple(self.pipeline.feature_names_in_) != MODEL_COLUMNS:
            raise ArtifactMismatch("pipeline preprocessing input order differs from manifest")

    def predict(
        self,
        event: TransactionEvent,
        snapshot: dict[str, float | int],
        feature_version: str = FEATURE_VERSION,
    ) -> Prediction:
        if feature_version != self.manifest["feature_version"]:
            raise ArtifactMismatch("snapshot feature version does not match model")
        try:
            row = model_row(event, snapshot)
        except KeyError as exc:
            raise InvalidFeatures(f"missing feature {exc}") from exc
        for name, value in row.items():
            if name in self.manifest["numeric_columns"]:
                if not isinstance(value, (int, float)) or not math.isfinite(value):
                    raise InvalidFeatures(f"invalid numeric feature {name}")
            elif not isinstance(value, str) or not value:
                raise InvalidFeatures(f"invalid categorical feature {name}")
        started = perf_counter()
        frame = pd.DataFrame([row], columns=list(MODEL_COLUMNS))
        transformed = self.pipeline.named_steps["preprocess"].transform(frame)
        score = float(self.pipeline.predict_proba(frame)[0, 1])
        elapsed_ms = (perf_counter() - started) * 1000
        if not math.isfinite(score) or not 0 <= score <= 1:
            raise InvalidFeatures("model produced invalid probability")
        explanations: tuple[dict[str, float | str], ...] = ()
        estimator = self.pipeline.named_steps["model"]
        if hasattr(estimator, "coef_"):
            names = self.pipeline.named_steps["preprocess"].get_feature_names_out()
            values = transformed.toarray()[0] if hasattr(transformed, "toarray") else transformed[0]
            contributions = estimator.coef_[0] * values
            ranked = sorted(
                zip(names, values, estimator.coef_[0], contributions, strict=True),
                key=lambda item: abs(float(item[3])),
                reverse=True,
            )[:5]
            explanations = tuple(
                {
                    "feature": str(name).split("__", 1)[-1],
                    "transformed_value": float(value),
                    "coefficient": float(coefficient),
                    "contribution": float(contribution),
                }
                for name, value, coefficient, contribution in ranked
            )
        return Prediction(
            score, self.manifest["model_version"], feature_version, elapsed_ms, explanations
        )
