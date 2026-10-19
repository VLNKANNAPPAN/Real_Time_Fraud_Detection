"""Delayed-label ingestion and production decision evaluation."""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sqlalchemy import select
from sqlalchemy.orm import Session

from fraud_engine.db import DecisionRow, ReviewLabelRow, TransactionRow, session_factory
from fraud_engine.decision import DecisionPolicy


def _binary_metrics(labels: list[int], predictions: list[int], scores: list[float]) -> dict:
    if not labels:
        return {
            "n": 0,
            "precision": None,
            "recall": None,
            "f1": None,
            "pr_auc": None,
            "roc_auc": None,
            "false_positive_rate": None,
            "false_negative_rate": None,
            "confusion_matrix": [[0, 0], [0, 0]],
        }
    y = np.asarray(labels)
    predicted = np.asarray(predictions)
    tn, fp, fn, tp = confusion_matrix(y, predicted, labels=[0, 1]).ravel()
    two_classes = len(set(labels)) == 2
    return {
        "n": len(labels),
        "precision": float(precision_score(y, predicted, zero_division=0)),
        "recall": float(recall_score(y, predicted, zero_division=0)),
        "f1": float(f1_score(y, predicted, zero_division=0)),
        "pr_auc": float(average_precision_score(y, scores)) if two_classes else None,
        "roc_auc": float(roc_auc_score(y, scores)) if two_classes else None,
        "false_positive_rate": float(fp / (fp + tn)) if fp + tn else None,
        "false_negative_rate": float(fn / (fn + tp)) if fn + tp else None,
        "confusion_matrix": [[int(tn), int(fp)], [int(fn), int(tp)]],
    }


def evaluate_records(
    records: list[dict[str, Any]], *, score_threshold: float = 0.5
) -> dict[str, Any]:
    """Evaluate matured binary labels; UNSURE remains visible but is excluded."""
    matured = [record for record in records if record["label"] in {"FRAUD", "NOT_FRAUD"}]

    def metrics_for(rows: list[dict[str, Any]]) -> dict[str, Any]:
        scored = [row for row in rows if row.get("score") is not None]
        labels = [int(row["label"] == "FRAUD") for row in scored]
        scores = [float(row["score"]) for row in scored]
        predictions = [int(score >= score_threshold) for score in scores]
        result = _binary_metrics(labels, predictions, scores)
        result["labeled_total"] = len(rows)
        result["scored_total"] = len(scored)
        result["score_threshold"] = score_threshold
        return result

    by_model: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in matured:
        by_model[row.get("model_version") or "NOT_RUN"].append(row)
        by_day[row["event_timestamp"][:10]].append(row)

    action_counts: dict[str, dict[str, int]] = {
        action: {"total": 0, "fraud": 0, "not_fraud": 0} for action in ("ALLOW", "REVIEW", "BLOCK")
    }
    for row in matured:
        bucket = action_counts[row["action"]]
        bucket["total"] += 1
        bucket["fraud" if row["label"] == "FRAUD" else "not_fraud"] += 1
    fraud_total = sum(row["label"] == "FRAUD" for row in matured)
    not_fraud_total = len(matured) - fraud_total
    return {
        "overall": metrics_for(matured),
        "by_model_version": {key: metrics_for(rows) for key, rows in sorted(by_model.items())},
        "by_event_day": {key: metrics_for(rows) for key, rows in sorted(by_day.items())},
        "decision_outcomes": action_counts,
        "allow_false_negative_rate": action_counts["ALLOW"]["fraud"] / fraud_total
        if fraud_total
        else None,
        "block_false_positive_rate": action_counts["BLOCK"]["not_fraud"] / not_fraud_total
        if not_fraud_total
        else None,
        "review_fraud_conversion": action_counts["REVIEW"]["fraud"]
        / action_counts["REVIEW"]["total"]
        if action_counts["REVIEW"]["total"]
        else None,
        "unsure_labels_excluded": sum(row["label"] == "UNSURE" for row in records),
    }


def evaluation_records(session: Session) -> list[dict[str, Any]]:
    rows = session.execute(
        select(TransactionRow, DecisionRow, ReviewLabelRow)
        .join(DecisionRow, DecisionRow.transaction_id == TransactionRow.transaction_id)
        .join(ReviewLabelRow, ReviewLabelRow.decision_id == DecisionRow.id)
        .order_by(TransactionRow.event_timestamp)
    )
    return [
        {
            "transaction_id": transaction.transaction_id,
            "event_timestamp": transaction.event_timestamp.isoformat(),
            "action": decision.action,
            "score": decision.score,
            "model_version": decision.model_version,
            "feature_version": decision.feature_version,
            "label": label.label,
            "label_source": label.label_source,
            "label_available_at": (label.available_at or label.created_at).isoformat(),
        }
        for transaction, decision, label in rows
    ]


def evaluate_session(session: Session) -> dict[str, Any]:
    threshold = DecisionPolicy.load(Path("config/decision-policy-v2.json")).review_score
    return evaluate_records(evaluation_records(session), score_threshold=threshold)


def import_simulated_labels(
    session: Session, path: Path, *, as_of: datetime | None = None
) -> dict[str, int]:
    as_of = as_of or datetime.now(UTC)
    counts = {"inserted": 0, "not_mature": 0, "no_decision": 0, "already_labeled": 0}
    with path.open(newline="", encoding="utf-8") as handle:
        for item in csv.DictReader(handle):
            available_at = datetime.fromisoformat(item["label_available_at"].replace("Z", "+00:00"))
            if available_at > as_of:
                counts["not_mature"] += 1
                continue
            decision = session.scalar(
                select(DecisionRow).where(DecisionRow.transaction_id == item["transaction_id"])
            )
            if decision is None:
                counts["no_decision"] += 1
                continue
            existing = session.scalar(
                select(ReviewLabelRow).where(ReviewLabelRow.decision_id == decision.id)
            )
            if existing is not None:
                counts["already_labeled"] += 1
                continue
            session.add(
                ReviewLabelRow(
                    decision_id=decision.id,
                    transaction_id=decision.transaction_id,
                    label="FRAUD" if int(item["fraud_label"]) else "NOT_FRAUD",
                    reviewer_id="synthetic-label-source",
                    submission_key=f"synthetic-v1:{decision.transaction_id}",
                    label_source="SIMULATED",
                    model_version=decision.model_version,
                    feature_version=decision.feature_version,
                    available_at=available_at,
                )
            )
            counts["inserted"] += 1
    session.commit()
    return counts


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--labels", type=Path, default=Path("data/generated/labels.csv"))
    parser.add_argument("--output", type=Path, default=Path("artifacts/evaluation/latest.json"))
    args = parser.parse_args()
    with session_factory()() as session:
        imported = import_simulated_labels(session, args.labels)
        report = {"import": imported, "evaluation": evaluate_session(session)}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
