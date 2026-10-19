"""Application services for persisted queries, reviews, metrics, and health."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

import numpy as np
from confluent_kafka.admin import AdminClient
from redis import Redis
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from fraud_engine.config import Settings, get_settings
from fraud_engine.db import DecisionRow, ReviewLabelRow, TransactionRow


class ResourceNotFound(LookupError):
    pass


class ReviewConflict(ValueError):
    pass


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def transaction_view(row: TransactionRow) -> dict[str, Any]:
    return {
        "transaction_id": row.transaction_id,
        "event_id": row.event_id,
        "user_id": row.user_id,
        "status": row.status,
        "event_timestamp": _iso(row.event_timestamp),
        "source": {
            "topic": row.source_topic,
            "partition": row.source_partition,
            "offset": row.source_offset,
        },
        "attempt_count": row.attempt_count,
        "last_error": row.last_error,
        "created_at": _iso(row.created_at),
        "updated_at": _iso(row.updated_at),
        "terminal_at": _iso(row.terminal_at),
        "payload": row.payload,
    }


def decision_view(row: DecisionRow) -> dict[str, Any]:
    return {
        "decision_id": row.id,
        "transaction_id": row.transaction_id,
        "action": row.action,
        "reason_codes": row.reason_codes,
        "explanations": row.explanations,
        "feature_snapshot": row.feature_snapshot,
        "feature_version": row.feature_version,
        "model_version": row.model_version,
        "policy_version": row.policy_version,
        "score": row.score,
        "inference_status": row.inference_status,
        "inference_latency_ms": row.inference_latency_ms,
        "feature_included": row.feature_included,
        "decided_at": _iso(row.decided_at),
    }


def label_view(row: ReviewLabelRow, *, idempotent: bool = False) -> dict[str, Any]:
    return {
        "label_id": row.id,
        "decision_id": row.decision_id,
        "transaction_id": row.transaction_id,
        "label": row.label,
        "reviewer_id": row.reviewer_id,
        "submission_key": row.submission_key,
        "label_source": row.label_source,
        "model_version": row.model_version,
        "feature_version": row.feature_version,
        "created_at": _iso(row.created_at),
        "available_at": _iso(row.available_at),
        "idempotent_replay": idempotent,
    }


class QueryService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def transaction(self, transaction_id: str) -> dict[str, Any]:
        row = self.session.get(TransactionRow, transaction_id)
        if row is None:
            raise ResourceNotFound("transaction not found")
        return transaction_view(row)

    def recent_transactions(self, limit: int = 100) -> dict[str, Any]:
        rows = self.session.execute(
            select(TransactionRow, DecisionRow, ReviewLabelRow)
            .outerjoin(DecisionRow, DecisionRow.transaction_id == TransactionRow.transaction_id)
            .outerjoin(ReviewLabelRow, ReviewLabelRow.decision_id == DecisionRow.id)
            .order_by(TransactionRow.created_at.desc(), TransactionRow.transaction_id.desc())
            .limit(limit)
        )
        items = []
        for transaction, decision, label in rows:
            item = transaction_view(transaction)
            item["decision"] = decision_view(decision) if decision is not None else None
            item["label"] = label_view(label) if label is not None else None
            items.append(item)
        return {"items": items}

    def decision(self, transaction_id: str) -> dict[str, Any]:
        row = self.session.scalar(
            select(DecisionRow).where(DecisionRow.transaction_id == transaction_id)
        )
        if row is None:
            raise ResourceNotFound("decision not found")
        return decision_view(row)

    def risk_history(self, user_id: str, limit: int = 100) -> dict[str, Any]:
        rows = self.session.execute(
            select(TransactionRow, DecisionRow)
            .join(DecisionRow, DecisionRow.transaction_id == TransactionRow.transaction_id)
            .where(TransactionRow.user_id == user_id)
            .order_by(DecisionRow.decided_at.desc(), DecisionRow.id.desc())
            .limit(limit)
        )
        items = [
            {"event_timestamp": _iso(transaction.event_timestamp), **decision_view(decision)}
            for transaction, decision in rows
        ]
        return {"user_id": user_id, "items": items}

    def reviews(self, limit: int = 100) -> dict[str, Any]:
        rows = self.session.execute(
            select(DecisionRow, ReviewLabelRow)
            .outerjoin(ReviewLabelRow, ReviewLabelRow.decision_id == DecisionRow.id)
            .where(DecisionRow.action == "REVIEW")
            .order_by(DecisionRow.decided_at.desc(), DecisionRow.id.desc())
            .limit(limit)
        )
        items = []
        for decision, label in rows:
            item = decision_view(decision)
            item["review_label"] = label_view(label) if label is not None else None
            items.append(item)
        return {"items": items}

    def metrics_summary(self) -> dict[str, Any]:
        action_counts = dict(
            self.session.execute(
                select(DecisionRow.action, func.count()).group_by(DecisionRow.action)
            ).all()
        )
        status_counts = dict(
            self.session.execute(
                select(TransactionRow.status, func.count()).group_by(TransactionRow.status)
            ).all()
        )
        labeled = self.session.scalar(select(func.count()).select_from(ReviewLabelRow)) or 0
        transactions = list(self.session.scalars(select(TransactionRow)))
        decisions = list(self.session.scalars(select(DecisionRow)))

        def percentiles(values: list[float]) -> dict[str, float | int | None]:
            if not values:
                return {"count": 0, "p50": None, "p95": None, "p99": None}
            return {
                "count": len(values),
                "p50": float(np.percentile(values, 50)),
                "p95": float(np.percentile(values, 95)),
                "p99": float(np.percentile(values, 99)),
            }

        processing_latencies = [
            (row.terminal_at - row.created_at).total_seconds() * 1000
            for row in transactions
            if row.terminal_at is not None and row.created_at is not None
        ]
        inference_latencies = [
            row.inference_latency_ms for row in decisions if row.inference_latency_ms is not None
        ]
        reasons: Counter[str] = Counter(reason for row in decisions for reason in row.reason_codes)
        latest = max((row.terminal_at for row in transactions if row.terminal_at), default=None)
        recent_count = 0
        if latest is not None:
            recent_count = sum(
                bool(row.terminal_at and (latest - row.terminal_at).total_seconds() <= 60)
                for row in transactions
            )
        return {
            "transactions": sum(status_counts.values()),
            "decisions": sum(action_counts.values()),
            "actions": {key: action_counts.get(key, 0) for key in ("ALLOW", "REVIEW", "BLOCK")},
            "statuses": status_counts,
            "review_labels": labeled,
            "throughput_per_minute_at_latest_activity": recent_count,
            "processing_latency_ms": percentiles(processing_latencies),
            "inference_latency_ms": percentiles(inference_latencies),
            "duplicate_deliveries": sum(max(0, row.attempt_count - 1) for row in transactions),
            "late_events": reasons.get("LATE_EVENT", 0),
            "fallbacks": {
                key: reasons.get(key, 0)
                for key in ("MODEL_UNAVAILABLE", "MODEL_TIMEOUT", "FEATURE_HISTORY_LIMIT")
            },
        }


class ReviewService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def submit(
        self, transaction_id: str, *, label: str, reviewer_id: str, submission_key: str
    ) -> dict[str, Any]:
        decision = self.session.scalar(
            select(DecisionRow).where(DecisionRow.transaction_id == transaction_id)
        )
        if decision is None:
            raise ResourceNotFound("decision not found")
        if decision.action != "REVIEW":
            raise ReviewConflict("only REVIEW decisions accept an initial human label")
        existing = self.session.scalar(
            select(ReviewLabelRow).where(ReviewLabelRow.decision_id == decision.id)
        )
        if existing is not None:
            if (
                existing.submission_key == submission_key
                and existing.label == label
                and existing.reviewer_id == reviewer_id
            ):
                return label_view(existing, idempotent=True)
            raise ReviewConflict("decision already has a label; use a correction workflow")
        row = ReviewLabelRow(
            decision_id=decision.id,
            transaction_id=transaction_id,
            label=label,
            reviewer_id=reviewer_id,
            submission_key=submission_key,
            label_source="HUMAN",
            model_version=decision.model_version,
            feature_version=decision.feature_version,
            available_at=datetime.now(UTC),
        )
        self.session.add(row)
        try:
            self.session.commit()
        except IntegrityError as exc:
            self.session.rollback()
            winner = self.session.scalar(
                select(ReviewLabelRow).where(ReviewLabelRow.decision_id == decision.id)
            )
            if (
                winner is not None
                and winner.submission_key == submission_key
                and winner.label == label
                and winner.reviewer_id == reviewer_id
            ):
                return label_view(winner, idempotent=True)
            raise ReviewConflict("concurrent conflicting label submission") from exc
        self.session.refresh(row)
        return label_view(row)


@dataclass(frozen=True)
class HealthReport:
    status: str
    components: dict[str, str]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def health_report(session: Session, settings: Settings | None = None) -> HealthReport:
    settings = settings or get_settings()
    components: dict[str, str] = {}
    try:
        session.execute(text("SELECT 1"))
        components["postgresql"] = "ok"
    except Exception:
        components["postgresql"] = "unavailable"
        session.rollback()
    try:
        components["redis"] = (
            "ok" if Redis.from_url(settings.redis_url, socket_timeout=2).ping() else "unavailable"
        )
    except Exception:
        components["redis"] = "unavailable"
    try:
        metadata = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers}).list_topics(
            timeout=2
        )
        components["kafka"] = "ok" if metadata.brokers else "unavailable"
    except Exception:
        components["kafka"] = "unavailable"
    status = "ok" if all(value == "ok" for value in components.values()) else "degraded"
    return HealthReport(status, components)
