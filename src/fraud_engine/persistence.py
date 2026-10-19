"""Standalone persistence retained for policy-level callers and tests.

The end-to-end claim, feature, decision, and recovery protocol lives in processing.py.
"""

from sqlalchemy.orm import Session

from fraud_engine.db import DecisionRow, TransactionRow
from fraud_engine.decision import Decision
from fraud_engine.events import TransactionEvent


def save_decision(
    session: Session,
    event: TransactionEvent,
    decision: Decision,
    snapshot: dict | None,
    *,
    feature_included: bool,
) -> DecisionRow:
    """Write an already decided transaction atomically within the caller's session.

    The caller owns commit/rollback. Kafka offsets and Redis state are not handled here.
    Concurrent duplicate races and cross-service recovery belong to checkpoint 8.
    """
    if feature_included != (snapshot is not None):
        raise ValueError("feature-included event needs its frozen snapshot")
    if decision.inference_status == "SCORED" and not feature_included:
        raise ValueError("scored event must be included in feature history")
    session.add(
        TransactionRow(
            transaction_id=event.transaction_id,
            event_id=event.event_id,
            user_id=event.user_id,
            payload_digest=event.digest(),
            payload=event.model_dump(mode="json"),
            status="DECIDED",
            event_timestamp=event.event_timestamp,
        )
    )
    # No ORM relationship is declared yet; flush the parent before its FK child.
    session.flush()
    row = DecisionRow(
        transaction_id=event.transaction_id,
        action=decision.action,
        reason_codes=list(decision.reason_codes),
        explanations=list(decision.explanations),
        feature_snapshot=snapshot,
        feature_version=decision.feature_version,
        model_version=decision.model_version,
        policy_version=decision.policy_version,
        score=decision.score,
        inference_status=decision.inference_status,
        feature_included=feature_included,
    )
    session.add(row)
    session.flush()
    return row
