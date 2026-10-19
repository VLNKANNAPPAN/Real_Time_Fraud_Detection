import csv
from datetime import UTC, datetime, timedelta

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from fraud_engine.db import Base, DecisionRow, ReviewLabelRow, TransactionRow
from fraud_engine.evaluation import evaluate_records, import_simulated_labels


def test_delayed_label_metrics_separate_actions_and_model_versions():
    rows = [
        {
            "transaction_id": "t1",
            "event_timestamp": "2025-01-01T00:00:00+00:00",
            "action": "ALLOW",
            "score": 0.8,
            "model_version": "m1",
            "label": "FRAUD",
        },
        {
            "transaction_id": "t2",
            "event_timestamp": "2025-01-01T01:00:00+00:00",
            "action": "BLOCK",
            "score": 0.9,
            "model_version": "m1",
            "label": "NOT_FRAUD",
        },
        {
            "transaction_id": "t3",
            "event_timestamp": "2025-01-02T00:00:00+00:00",
            "action": "REVIEW",
            "score": 0.7,
            "model_version": "m1",
            "label": "FRAUD",
        },
        {
            "transaction_id": "t4",
            "event_timestamp": "2025-01-02T01:00:00+00:00",
            "action": "ALLOW",
            "score": 0.1,
            "model_version": "m2",
            "label": "NOT_FRAUD",
        },
        {
            "transaction_id": "t5",
            "event_timestamp": "2025-01-02T02:00:00+00:00",
            "action": "REVIEW",
            "score": 0.5,
            "model_version": "m2",
            "label": "UNSURE",
        },
    ]
    report = evaluate_records(rows, score_threshold=0.5)
    assert report["overall"]["n"] == 4
    assert report["overall"]["precision"] == 2 / 3
    assert report["overall"]["recall"] == 1.0
    assert report["overall"]["score_threshold"] == 0.5
    assert report["allow_false_negative_rate"] == 0.5
    assert report["block_false_positive_rate"] == 0.5
    assert report["review_fraud_conversion"] == 1.0
    assert set(report["by_model_version"]) == {"m1", "m2"}
    assert set(report["by_event_day"]) == {"2025-01-01", "2025-01-02"}
    assert report["unsure_labels_excluded"] == 1


def test_simulated_labels_import_only_after_availability(tmp_path):
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    factory = sessionmaker(engine, expire_on_commit=False)
    now = datetime(2025, 1, 5, tzinfo=UTC)
    with factory() as session:
        for number in (1, 2):
            transaction_id = f"t{number}"
            session.add(
                TransactionRow(
                    transaction_id=transaction_id,
                    event_id=f"e{number}",
                    user_id="u1",
                    payload_digest=str(number) * 64,
                    payload={"transaction_id": transaction_id},
                    status="DECIDED",
                    event_timestamp=now - timedelta(days=3),
                )
            )
            session.flush()
            session.add(
                DecisionRow(
                    transaction_id=transaction_id,
                    action="ALLOW",
                    reason_codes=["LOW_RISK"],
                    feature_snapshot={},
                    feature_version="features-v2",
                    model_version="m1",
                    policy_version="p1",
                    score=0.1,
                    inference_status="SCORED",
                    feature_included=True,
                )
            )
        session.commit()
    path = tmp_path / "labels.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=["transaction_id", "fraud_label", "label_available_at"]
        )
        writer.writeheader()
        writer.writerow(
            {
                "transaction_id": "t1",
                "fraud_label": 1,
                "label_available_at": (now - timedelta(days=1)).isoformat(),
            }
        )
        writer.writerow(
            {
                "transaction_id": "t2",
                "fraud_label": 0,
                "label_available_at": (now + timedelta(days=1)).isoformat(),
            }
        )
    with factory() as session:
        result = import_simulated_labels(session, path, as_of=now)
        labels = list(session.scalars(select(ReviewLabelRow)))
    assert result == {"inserted": 1, "not_mature": 1, "no_decision": 0, "already_labeled": 0}
    assert labels[0].label == "FRAUD"
    assert labels[0].label_source == "SIMULATED"
