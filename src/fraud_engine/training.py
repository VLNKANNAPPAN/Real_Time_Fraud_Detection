"""Chronological baseline training with frozen online-compatible feature snapshots."""

import argparse
import hashlib
import json
from datetime import timedelta
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import sklearn
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from fraud_engine.events import TransactionEvent
from fraud_engine.features import FEATURE_NAMES, FEATURE_VERSION, OfflineFeatureEngine

NUMERIC_COLUMNS = (*FEATURE_NAMES, "amount", "account_age_days", "event_hour")
CATEGORICAL_COLUMNS = ("merchant_category", "country", "payment_method")
MODEL_COLUMNS = (*NUMERIC_COLUMNS, *CATEGORICAL_COLUMNS)
MODEL_VERSION = "fraud-lr-balanced-v2"
REVIEW_RATE = 0.05
MISSED_FRAUD_COST = 100.0
FALSE_POSITIVE_COST = 5.0


def model_row(event: TransactionEvent, snapshot: dict[str, float | int]) -> dict:
    return {
        **{name: snapshot[name] for name in FEATURE_NAMES},
        "amount": event.amount,
        "account_age_days": event.account_age_days,
        "event_hour": event.event_timestamp.hour,
        "merchant_category": event.merchant_category,
        "country": event.country,
        "payment_method": event.payment_method,
    }


def make_training_frame(events: pd.DataFrame, labels: pd.DataFrame) -> pd.DataFrame:
    label_map = labels.set_index("transaction_id")
    # Generated training input has already passed unique-ID validation; retaining every frozen
    # dedup snapshot would turn a bounded event-time replay into an unnecessary O(dataset) cache.
    engine = OfflineFeatureEngine(track_identities=False)
    rows = []
    ordered = events.sort_values(["producer_timestamp", "transaction_id"], kind="stable")
    for record in ordered.itertuples(index=False):
        event = TransactionEvent.model_validate(record._asdict())
        outcome = engine.process(event)
        if outcome.snapshot is None:
            continue
        rows.append(
            {
                **model_row(event, outcome.snapshot),
                "transaction_id": event.transaction_id,
                "producer_timestamp": event.producer_timestamp,
                "fraud_label": int(label_map.loc[event.transaction_id, "fraud_label"]),
                "label_available_at": pd.Timestamp(
                    label_map.loc[event.transaction_id, "label_available_at"]
                ),
            }
        )
    return pd.DataFrame(rows)


def chronological_splits(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    ordered = frame.sort_values(["producer_timestamp", "transaction_id"], kind="stable")
    when = pd.to_datetime(ordered["producer_timestamp"], utc=True)
    duration = when.max() - when.min()
    train_end = when.min() + duration * 0.45
    validation_start = train_end + timedelta(days=2)
    validation_end = when.min() + duration * 0.78
    test_start = validation_end + timedelta(days=2)
    train = ordered.loc[when <= train_end]
    validation = ordered.loc[(when >= validation_start) & (when <= validation_end)]
    test = ordered.loc[when >= test_start]
    if any(part.empty for part in (train, validation, test)):
        raise ValueError("chronological splits are empty after label maturation gaps")
    if pd.to_datetime(train["label_available_at"], utc=True).max() > validation_start:
        raise ValueError("training label not mature by validation start")
    if pd.to_datetime(validation["label_available_at"], utc=True).max() > test_start:
        raise ValueError("validation label not mature by test start")
    return train, validation, test


def make_pipeline(model_name: str = "logistic_balanced") -> Pipeline:
    numeric = Pipeline([("impute", SimpleImputer(strategy="median")), ("scale", StandardScaler())])
    dense = model_name == "hist_gradient_boosting"
    categorical = OneHotEncoder(handle_unknown="ignore", sparse_output=not dense)
    preprocess = ColumnTransformer(
        [
            ("numeric", numeric, list(NUMERIC_COLUMNS)),
            ("categorical", categorical, list(CATEGORICAL_COLUMNS)),
        ]
    )
    if model_name == "logistic":
        estimator = LogisticRegression(max_iter=500)
    elif model_name == "logistic_balanced":
        estimator = LogisticRegression(max_iter=500, class_weight="balanced")
    elif model_name == "hist_gradient_boosting":
        estimator = HistGradientBoostingClassifier(
            learning_rate=0.08, max_iter=180, max_leaf_nodes=31, random_state=17
        )
    else:
        raise ValueError(f"unknown model {model_name}")
    return Pipeline([("preprocess", preprocess), ("model", estimator)])


def threshold_for_review_rate(scores: np.ndarray, review_rate: float = REVIEW_RATE) -> float:
    return float(np.quantile(scores, 1 - review_rate, method="higher"))


def recall_at_threshold(y: np.ndarray, scores: np.ndarray, threshold: float) -> float:
    positives = int(np.sum(y))
    return float(np.sum((scores >= threshold) & (y == 1)) / positives) if positives else 0.0


def recall_at_fixed_budget(
    y: np.ndarray, scores: np.ndarray, review_rate: float = REVIEW_RATE
) -> float:
    positives = int(np.sum(y))
    if not positives:
        return 0.0
    budget = max(1, int(np.ceil(len(scores) * review_rate)))
    # Stable ordering makes ties deterministic for coarse rule scores.
    selected = np.argsort(-scores, kind="stable")[:budget]
    return float(np.sum(y[selected] == 1) / positives)


def calibration_report(y: np.ndarray, scores: np.ndarray, bins: int = 10) -> dict:
    edges = np.linspace(0, 1, bins + 1)
    assignments = np.minimum(np.digitize(scores, edges[1:-1]), bins - 1)
    rows = []
    expected_calibration_error = 0.0
    for index in range(bins):
        mask = assignments == index
        if not mask.any():
            continue
        mean_score = float(np.mean(scores[mask]))
        observed = float(np.mean(y[mask]))
        fraction = float(np.mean(mask))
        expected_calibration_error += fraction * abs(mean_score - observed)
        rows.append(
            {
                "lower": float(edges[index]),
                "upper": float(edges[index + 1]),
                "count": int(mask.sum()),
                "mean_score": mean_score,
                "observed_fraud_rate": observed,
            }
        )
    return {
        "brier_score": float(brier_score_loss(y, scores)),
        "expected_calibration_error": float(expected_calibration_error),
        "bins": rows,
    }


def bootstrap_intervals(
    y: np.ndarray, scores: np.ndarray, threshold: float, *, repeats: int = 250, seed: int = 17
) -> dict:
    rng = np.random.default_rng(seed)
    pr_auc_values: list[float] = []
    recall_values: list[float] = []
    for _ in range(repeats):
        indices = rng.integers(0, len(y), len(y))
        sampled_y, sampled_scores = y[indices], scores[indices]
        if len(np.unique(sampled_y)) < 2:
            continue
        pr_auc_values.append(float(average_precision_score(sampled_y, sampled_scores)))
        recall_values.append(recall_at_threshold(sampled_y, sampled_scores, threshold))

    def interval(values: list[float]) -> list[float] | None:
        return [float(value) for value in np.quantile(values, [0.025, 0.975])] if values else None

    return {
        "repeats": len(pr_auc_values),
        "pr_auc_95ci": interval(pr_auc_values),
        "recall_at_5pct_review_95ci": interval(recall_values),
    }


def feature_diagnostics(frame: pd.DataFrame) -> dict:
    y = frame["fraud_label"].to_numpy()
    report = {}
    for name in FEATURE_NAMES:
        values = frame[name].astype(float).to_numpy()
        fraud = values[y == 1]
        legitimate = values[y == 0]
        auc = float(roc_auc_score(y, values)) if len(np.unique(values)) > 1 else 0.5
        report[name] = {
            "fraud": {
                "mean": float(np.mean(fraud)),
                "median": float(np.median(fraud)),
                "q25": float(np.quantile(fraud, 0.25)),
                "q75": float(np.quantile(fraud, 0.75)),
            },
            "legitimate": {
                "mean": float(np.mean(legitimate)),
                "median": float(np.median(legitimate)),
                "q25": float(np.quantile(legitimate, 0.25)),
                "q75": float(np.quantile(legitimate, 0.75)),
            },
            "single_feature_roc_auc": auc,
            "direction_agnostic_roc_auc": max(auc, 1 - auc),
        }
    return report


def rule_scores(frame: pd.DataFrame) -> np.ndarray:
    signals = np.column_stack(
        [
            frame["is_new_device_24h"].to_numpy() >= 1,
            frame["transaction_count_5m"].to_numpy() >= 3,
            frame["country"].to_numpy() != "IN",
            frame["amount_vs_user_average_24h"].to_numpy() >= 3,
            (frame["small_transaction_count_5m"].to_numpy() >= 3)
            & (frame["amount_vs_user_average_24h"].to_numpy() >= 3),
        ]
    )
    return signals.mean(axis=1)


def ranking_metrics(y: np.ndarray, scores: np.ndarray, threshold: float) -> dict:
    return {
        "pr_auc": float(average_precision_score(y, scores)),
        "roc_auc": float(roc_auc_score(y, scores)),
        "recall_at_5pct_review": recall_at_fixed_budget(y, scores),
        "operating_recall_from_validation_threshold": recall_at_threshold(y, scores, threshold),
        "operating_review_rate_from_validation_threshold": float(np.mean(scores >= threshold)),
        "threshold_selected_on_validation": float(threshold),
    }


def cost_threshold(y: np.ndarray, scores: np.ndarray) -> dict:
    candidates = np.unique(np.quantile(scores, np.linspace(0, 1, 501)))
    best = None
    for threshold in candidates:
        predicted = scores >= threshold
        false_positives = int(np.sum(predicted & (y == 0)))
        false_negatives = int(np.sum(~predicted & (y == 1)))
        cost = false_positives * FALSE_POSITIVE_COST + false_negatives * MISSED_FRAUD_COST
        candidate = (cost, -float(threshold), false_positives, false_negatives)
        if best is None or candidate < best:
            best = candidate
    assert best is not None
    return {
        "threshold": -best[1],
        "validation_cost": float(best[0]),
        "false_positives": best[2],
        "false_negatives": best[3],
        "missed_fraud_cost": MISSED_FRAUD_COST,
        "false_positive_cost": FALSE_POSITIVE_COST,
    }


def metrics(y: np.ndarray, scores: np.ndarray, threshold: float) -> dict:
    predicted = (scores >= threshold).astype(int)
    tn, fp, fn, tp = confusion_matrix(y, predicted, labels=[0, 1]).ravel()
    return {
        "precision": float(precision_score(y, predicted, zero_division=0)),
        "recall": float(recall_score(y, predicted, zero_division=0)),
        "f1": float(f1_score(y, predicted, zero_division=0)),
        "pr_auc": float(average_precision_score(y, scores)) if len(set(y)) == 2 else None,
        "roc_auc": float(roc_auc_score(y, scores)) if len(set(y)) == 2 else None,
        "confusion_matrix": [[int(tn), int(fp)], [int(fn), int(tp)]],
        "false_positive_rate": float(fp / (fp + tn)) if fp + tn else None,
        "false_negative_rate": float(fn / (fn + tp)) if fn + tp else None,
        "prevalence": float(np.mean(y)),
        "n": int(len(y)),
        "threshold": threshold,
    }


def hidden_ceiling_report(frame: pd.DataFrame, generator_truth: pd.DataFrame | None) -> dict | None:
    if generator_truth is None:
        return None
    joined = frame.merge(generator_truth, on="transaction_id", how="inner", validate="one_to_one")
    train, validation, test = chronological_splits(joined)
    columns = [
        "attack_intent",
        "campaign_position",
        "hidden_profile_amount_paise",
        "generated_new_device",
        "generated_foreign_country",
        "latent_fraud_probability",
        "scenario",
    ]
    numeric = [name for name in columns if name != "scenario"]
    pipeline = Pipeline(
        [
            (
                "preprocess",
                ColumnTransformer(
                    [
                        ("numeric", StandardScaler(), numeric),
                        ("categorical", OneHotEncoder(handle_unknown="ignore"), ["scenario"]),
                    ]
                ),
            ),
            ("model", LogisticRegression(max_iter=500, class_weight="balanced")),
        ]
    )
    pipeline.fit(train[columns], train["fraud_label"])
    val_scores = pipeline.predict_proba(validation[columns])[:, 1]
    threshold = threshold_for_review_rate(val_scores)
    test_scores = pipeline.predict_proba(test[columns])[:, 1]
    return {
        "purpose": "generator ceiling only; hidden columns are never serving features",
        "validation": ranking_metrics(validation["fraud_label"].to_numpy(), val_scores, threshold),
        "test": ranking_metrics(test["fraud_label"].to_numpy(), test_scores, threshold),
    }


def train_and_save(
    frame: pd.DataFrame, output: Path, generator_truth: pd.DataFrame | None = None
) -> dict:
    train, validation, test = chronological_splits(frame)
    if any(part["fraud_label"].nunique() < 2 for part in (train, validation, test)):
        raise ValueError("each chronological split must contain both labels")
    model_scores: dict[str, tuple[Pipeline, np.ndarray, np.ndarray]] = {}
    for name in ("logistic", "logistic_balanced", "hist_gradient_boosting"):
        candidate = make_pipeline(name)
        candidate.fit(train[list(MODEL_COLUMNS)], train["fraud_label"])
        model_scores[name] = (
            candidate,
            candidate.predict_proba(validation[list(MODEL_COLUMNS)])[:, 1],
            candidate.predict_proba(test[list(MODEL_COLUMNS)])[:, 1],
        )

    # Keep the explainable balanced logistic model as the serving artifact; the benchmark makes
    # any performance gap to the nonlinear challenger visible instead of silently changing it.
    model, val_scores, test_scores = model_scores["logistic_balanced"]
    threshold = threshold_for_review_rate(val_scores)
    validation_y = validation["fraud_label"].to_numpy()
    test_y = test["fraud_label"].to_numpy()
    rules_validation = rule_scores(validation)
    rules_test = rule_scores(test)
    rules_threshold = threshold_for_review_rate(rules_validation)
    combined_validation = np.maximum(val_scores, rules_validation)
    combined_test = np.maximum(test_scores, rules_test)
    combined_threshold = threshold_for_review_rate(combined_validation)
    comparison = {
        "prior_random": {
            "validation": {
                "pr_auc": float(validation_y.mean()),
                "recall_at_5pct_review": REVIEW_RATE,
            },
            "test": {"pr_auc": float(test_y.mean()), "recall_at_5pct_review": REVIEW_RATE},
        },
        "rules_only": {
            "validation": ranking_metrics(validation_y, rules_validation, rules_threshold),
            "test": ranking_metrics(test_y, rules_test, rules_threshold),
        },
    }
    for name, (_, candidate_validation, candidate_test) in model_scores.items():
        candidate_threshold = threshold_for_review_rate(candidate_validation)
        comparison[name] = {
            "validation": ranking_metrics(validation_y, candidate_validation, candidate_threshold),
            "test": ranking_metrics(test_y, candidate_test, candidate_threshold),
        }
    comparison["model_plus_rules"] = {
        "validation": ranking_metrics(validation_y, combined_validation, combined_threshold),
        "test": ranking_metrics(test_y, combined_test, combined_threshold),
    }
    cost = cost_threshold(validation_y, val_scores)
    test_cost_metrics = metrics(test_y, test_scores, cost["threshold"])
    cost["test_cost"] = (
        test_cost_metrics["confusion_matrix"][1][0] * MISSED_FRAUD_COST
        + test_cost_metrics["confusion_matrix"][0][1] * FALSE_POSITIVE_COST
    )
    report = {
        "model_version": MODEL_VERSION,
        "feature_version": FEATURE_VERSION,
        "dataset_version": "synthetic-v2",
        "split": {"train": len(train), "validation": len(validation), "test": len(test)},
        "train_end": str(train["producer_timestamp"].max()),
        "validation_end": str(validation["producer_timestamp"].max()),
        "validation_metrics": metrics(validation_y, val_scores, threshold),
        "test_metrics": metrics(test_y, test_scores, threshold),
        "model_comparison": comparison,
        "feature_diagnostics": feature_diagnostics(train),
        "hidden_generator_ceiling": hidden_ceiling_report(frame, generator_truth),
        "bootstrap_test": bootstrap_intervals(test_y, test_scores, threshold),
        "calibration": {
            "validation": calibration_report(validation_y, val_scores),
            "test": calibration_report(test_y, test_scores),
        },
        "cost_threshold": cost,
        "threshold_analysis": [
            metrics(validation_y, val_scores, float(candidate))
            for candidate in (0.02, 0.05, 0.1, 0.2, 0.5)
        ],
    }
    output.mkdir(parents=True, exist_ok=True)
    artifact = output / "pipeline.joblib"
    joblib.dump(model, artifact)
    artifact_hash = hashlib.sha256(artifact.read_bytes()).hexdigest()
    artifact_version = f"{MODEL_VERSION}+{artifact_hash[:12]}"
    report["model_version"] = artifact_version
    manifest = {
        "model_version": artifact_version,
        "feature_version": FEATURE_VERSION,
        "dataset_version": "synthetic-v2",
        "training_data_sha256": hashlib.sha256(frame.to_csv(index=False).encode()).hexdigest(),
        "sklearn_version": sklearn.__version__,
        "split_policy": "producer-time-45%-78%-two-day-label-gaps",
        "columns": list(MODEL_COLUMNS),
        "numeric_columns": list(NUMERIC_COLUMNS),
        "categorical_columns": list(CATEGORICAL_COLUMNS),
        "artifact_sha256": artifact_hash,
        "validation_threshold_review_rate_5pct": threshold,
        "serving_model": "logistic_balanced",
    }
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    (output / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, default=Path("data/generated"))
    parser.add_argument("--output", type=Path, default=Path("artifacts/fraud-v2"))
    args = parser.parse_args()
    frame = make_training_frame(
        pd.read_csv(args.data / "events.csv"), pd.read_csv(args.data / "labels.csv")
    )
    truth_path = args.data / "generator_truth.csv"
    truth = (
        pd.read_csv(truth_path, dtype={"campaign_id": "string", "scenario": "string"})
        if truth_path.is_file()
        else None
    )
    report = train_and_save(frame, args.output, truth)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
