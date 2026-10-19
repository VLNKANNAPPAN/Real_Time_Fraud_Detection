from datetime import UTC, datetime

import pytest
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import fraud_engine.api as api_module
from fraud_engine.api import ReviewSubmission
from fraud_engine.config import Settings
from fraud_engine.db import Base, DecisionRow, TransactionRow
from fraud_engine.services import HealthReport


def seeded_factory():
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    Base.metadata.create_all(engine)
    factory = sessionmaker(engine, expire_on_commit=False)
    now = datetime(2025, 1, 1, tzinfo=UTC)
    with factory() as session:
        session.add(
            TransactionRow(
                transaction_id="t-review",
                event_id="e-review",
                user_id="u1",
                payload_digest="a" * 64,
                payload={"transaction_id": "t-review"},
                status="DECIDED",
                event_timestamp=now,
                source_topic="transactions.raw",
                source_partition=0,
                source_offset=10,
                attempt_count=1,
                terminal_at=now,
            )
        )
        session.flush()
        session.add(
            DecisionRow(
                transaction_id="t-review",
                action="REVIEW",
                reason_codes=["MODEL_REVIEW_BAND"],
                explanations=[{"feature": "amount", "contribution": 1.2}],
                feature_snapshot={"transaction_count_5m": 0},
                feature_version="features-v2",
                model_version="model-v2",
                policy_version="policy-v2",
                score=0.7,
                inference_status="SCORED",
                inference_latency_ms=1.0,
                feature_included=True,
                decided_at=now,
            )
        )
        session.commit()
    return factory


def test_query_controllers_and_metrics(monkeypatch):
    factory = seeded_factory()
    monkeypatch.setattr(api_module, "kafka_consumer_lag", lambda: {"available": True, "total": 0})
    with factory() as session:
        assert api_module.get_transaction("t-review", session)["source"]["offset"] == 10
        decision = api_module.get_decision("t-review", session)
        assert decision["model_version"] == "model-v2"
        assert decision["explanations"][0]["feature"] == "amount"
        assert (
            api_module.get_risk_history("u1", session, 100)["items"][0]["transaction_id"]
            == "t-review"
        )
        assert api_module.get_reviews(session, None, 100)["items"][0]["review_label"] is None
        metrics = api_module.get_metrics_summary(session)
        assert metrics["actions"] == {"ALLOW": 0, "REVIEW": 1, "BLOCK": 0}
        assert metrics["consumer_lag"]["total"] == 0
        assert api_module.get_monitoring_export(session, 100)["items"][0]["model_version"] == (
            "model-v2"
        )
        assert "fraud_engine_decisions 1" in api_module.get_prometheus_metrics(session)
        with pytest.raises(HTTPException) as missing:
            api_module.get_transaction("missing", session)
        assert missing.value.status_code == 404


def test_review_label_is_versioned_idempotent_and_conflict_safe():
    factory = seeded_factory()
    body = ReviewSubmission(label="FRAUD", reviewer_id="analyst-1", submission_key="submission-1")
    with factory() as session:
        first = api_module.submit_review_label("t-review", body, session, None)
        assert first["model_version"] == "model-v2"
        assert first["feature_version"] == "features-v2"
        replay = api_module.submit_review_label("t-review", body, session, None)
        assert replay["idempotent_replay"] is True
        with pytest.raises(HTTPException) as conflict:
            api_module.submit_review_label(
                "t-review", body.model_copy(update={"label": "NOT_FRAUD"}), session, None
            )
        assert conflict.value.status_code == 409


def test_review_validation_health_and_fail_closed_auth(monkeypatch):
    with pytest.raises(ValidationError):
        ReviewSubmission(label="MAYBE", reviewer_id="a", submission_key="b")
    factory = seeded_factory()
    monkeypatch.setattr(
        api_module,
        "health_report",
        lambda session: HealthReport(
            "degraded", {"postgresql": "ok", "redis": "unavailable", "kafka": "ok"}
        ),
    )
    with factory() as session:
        assert api_module.get_system_health(session)["status"] == "degraded"

    monkeypatch.setattr(api_module, "get_settings", lambda: Settings(_env_file=None))
    with pytest.raises(HTTPException) as unavailable:
        api_module.require_review_auth(None)
    assert unavailable.value.status_code == 503

    configured = Settings(_env_file=None, fraud_review_api_key="secret-value")
    monkeypatch.setattr(api_module, "get_settings", lambda: configured)
    with pytest.raises(HTTPException) as unauthorized:
        api_module.require_review_auth("wrong")
    assert unauthorized.value.status_code == 401
    assert api_module.require_review_auth("secret-value") is None
