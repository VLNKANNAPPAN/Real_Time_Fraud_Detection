from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from fraud_engine.db import Base, DecisionRow
from fraud_engine.decision import DecisionPolicy, decide
from fraud_engine.events import TransactionEvent
from fraud_engine.inference import Prediction
from fraud_engine.persistence import save_decision

POLICY = DecisionPolicy.load(Path("config/decision-policy-v2.json"))
NOW = datetime(2025, 1, 1, tzinfo=UTC)
EVENT = TransactionEvent(
    event_id="e",
    transaction_id="t",
    user_id="u",
    event_timestamp=NOW,
    producer_timestamp=NOW,
    amount=100,
    merchant_category="fuel",
    country="IN",
    device_id="d",
    payment_method="card",
)
BASE = {
    "is_new_device_24h": 0,
    "transaction_count_5m": 0,
    "has_amount_history_24h": 1,
    "amount_vs_user_average_24h": 1.0,
}


def pred(score):
    return Prediction(score, "fraud-lr-balanced-v2", "features-v2", 1.0)


def test_score_alone_never_blocks_and_boundaries():
    assert decide(EVENT, BASE, pred(POLICY.review_score - 0.00001), POLICY).action == "ALLOW"
    assert decide(EVENT, BASE, pred(POLICY.review_score), POLICY).action == "REVIEW"
    assert decide(EVENT, BASE, pred(0.8), POLICY).action == "REVIEW"
    assert decide(EVENT, BASE, pred(1.0), POLICY).action == "REVIEW"


def test_corrobated_block_and_rule_review():
    signals = {
        **BASE,
        "is_new_device_24h": 1,
        "transaction_count_5m": 3,
        "amount_vs_user_average_24h": 4,
    }
    block = decide(EVENT, signals, pred(POLICY.block_score), POLICY)
    assert block.action == "BLOCK"
    assert decide(EVENT, signals, pred(POLICY.block_score - 0.00001), POLICY).action == "REVIEW"
    assert block.reason_codes == (
        "MODEL_BLOCK_BAND",
        "NEW_DEVICE_24H",
        "HIGH_TX_VELOCITY",
        "UNUSUAL_AMOUNT",
    )
    assert (
        decide(EVENT, {**signals, "amount_vs_user_average_24h": 1}, pred(0.01), POLICY).action
        == "ALLOW"
    )
    assert decide(EVENT, signals, pred(0.01), POLICY).action == "REVIEW"


def test_fallback_cannot_fabricate_score():
    fallback = decide(EVENT, None, None, POLICY, fallback_reason="MODEL_TIMEOUT")
    assert fallback.action == "REVIEW"
    assert fallback.score is None
    assert fallback.model_version is None
    with pytest.raises(ValueError):
        decide(EVENT, BASE, pred(0.9), POLICY, fallback_reason="MODEL_TIMEOUT")


def test_reason_codes_versions_and_score_persist():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    decision = decide(EVENT, BASE, pred(POLICY.review_score), POLICY)
    with Session(engine) as session, session.begin():
        save_decision(session, EVENT, decision, BASE, feature_included=True)
    with Session(engine) as session:
        saved = session.scalar(select(DecisionRow))
        assert saved.reason_codes == ["MODEL_REVIEW_BAND"]
        assert saved.policy_version == POLICY.policy_version
        assert saved.feature_version == "features-v2"
        assert saved.model_version == "fraud-lr-balanced-v2"
        assert saved.score == POLICY.review_score


def test_model_fallback_keeps_feature_snapshot_without_fabricating_score():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    fallback = decide(EVENT, BASE, None, POLICY, fallback_reason="MODEL_UNAVAILABLE")
    with Session(engine) as session, session.begin():
        save_decision(session, EVENT, fallback, BASE, feature_included=True)
    with Session(engine) as session:
        saved = session.scalar(select(DecisionRow))
        assert saved.feature_snapshot == BASE
        assert saved.score is None
        assert saved.model_version is None
        assert saved.reason_codes == ["MODEL_UNAVAILABLE"]
