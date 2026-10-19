"""Versioned monitoring export and lightweight distribution comparison."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
from sqlalchemy import select
from sqlalchemy.orm import Session

from fraud_engine.db import DecisionRow, ReviewLabelRow, TransactionRow, session_factory

EXPORT_SCHEMA_VERSION = "fraud-monitoring-export-v1"


def monitoring_records(
    session: Session, *, as_of: datetime | None = None, limit: int = 10_000
) -> list[dict[str, Any]]:
    as_of = as_of or datetime.now(UTC)
    rows = session.execute(
        select(TransactionRow, DecisionRow, ReviewLabelRow)
        .join(DecisionRow, DecisionRow.transaction_id == TransactionRow.transaction_id)
        .outerjoin(ReviewLabelRow, ReviewLabelRow.decision_id == DecisionRow.id)
        .order_by(DecisionRow.id.desc())
        .limit(limit)
    )
    result = []
    for transaction, decision, label in rows:
        available_at = (label.available_at or label.created_at) if label is not None else None
        label_is_mature = available_at is not None and _as_utc(available_at) <= as_of
        result.append(
            {
                "schema_version": EXPORT_SCHEMA_VERSION,
                "transaction_id": transaction.transaction_id,
                "event_timestamp": transaction.event_timestamp.isoformat(),
                "decision_timestamp": decision.decided_at.isoformat()
                if decision.decided_at
                else None,
                "model_version": decision.model_version,
                "feature_version": decision.feature_version,
                "policy_version": decision.policy_version,
                "score": decision.score,
                "action": decision.action,
                "reason_codes": decision.reason_codes,
                "features": decision.feature_snapshot,
                "label": label.label if label_is_mature else None,
                "label_source": label.label_source if label_is_mature else None,
                "label_available_at": available_at.isoformat() if label_is_mature else None,
            }
        )
    return result


def _as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def population_stability_index(
    reference: list[float], current: list[float], bins: int = 10
) -> float | None:
    if len(reference) < bins or len(current) < bins:
        return None
    edges = np.unique(np.quantile(reference, np.linspace(0, 1, bins + 1)))
    if len(edges) < 3:
        return 0.0
    edges[0], edges[-1] = -np.inf, np.inf
    reference_hist = np.histogram(reference, bins=edges)[0] / len(reference)
    current_hist = np.histogram(current, bins=edges)[0] / len(current)
    reference_hist = np.clip(reference_hist, 1e-6, None)
    current_hist = np.clip(current_hist, 1e-6, None)
    return float(np.sum((current_hist - reference_hist) * np.log(current_hist / reference_hist)))


def distribution_report(
    reference: list[dict[str, Any]], current: list[dict[str, Any]]
) -> dict[str, Any]:
    feature_names = sorted(
        {
            name
            for row in [*reference, *current]
            for name in (row.get("features") or {})
            if isinstance((row.get("features") or {}).get(name), (int, float))
        }
    )
    feature_psi = {}
    for name in feature_names:
        old = [
            float(row["features"][name]) for row in reference if name in (row.get("features") or {})
        ]
        new = [
            float(row["features"][name]) for row in current if name in (row.get("features") or {})
        ]
        feature_psi[name] = population_stability_index(old, new)
    old_scores = [float(row["score"]) for row in reference if row.get("score") is not None]
    new_scores = [float(row["score"]) for row in current if row.get("score") is not None]

    def decisions(rows):
        total = len(rows) or 1
        return {
            action: sum(row.get("action") == action for row in rows) / total
            for action in ("ALLOW", "REVIEW", "BLOCK")
        }

    return {
        "reference_rows": len(reference),
        "current_rows": len(current),
        "feature_psi": feature_psi,
        "score_psi": population_stability_index(old_scores, new_scores),
        "decision_distribution": {"reference": decisions(reference), "current": decisions(current)},
        "interpretation": (
            "PSI is a distribution signal, not proof of model degradation; "
            "join matured labels for performance."
        ),
    }


def recent_psi_report(session: Session, *, window: int = 200) -> dict[str, Any]:
    """Compare the newest scored window with the immediately preceding compatible window."""
    rows = monitoring_records(session, limit=window * 4)
    if not rows:
        return distribution_report([], [])
    newest = rows[0]
    compatible = [
        row
        for row in rows
        if row.get("feature_version") == newest.get("feature_version")
        and row.get("model_version") == newest.get("model_version")
    ]
    current = compatible[:window]
    reference = compatible[window : window * 2]
    report = distribution_report(reference, current)
    report["feature_version"] = newest.get("feature_version")
    report["model_version"] = newest.get("model_version")
    report["window"] = window
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("artifacts/monitoring/export.jsonl"))
    parser.add_argument("--limit", type=int, default=10_000)
    args = parser.parse_args()
    with session_factory()() as session:
        rows = monitoring_records(session, limit=args.limit)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
    print(
        json.dumps(
            {"schema_version": EXPORT_SCHEMA_VERSION, "rows": len(rows), "output": str(args.output)}
        )
    )


if __name__ == "__main__":
    main()
