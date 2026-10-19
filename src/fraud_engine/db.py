from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
    create_engine,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

from fraud_engine.config import get_settings


class Base(DeclarativeBase):
    pass


class TransactionRow(Base):
    __tablename__ = "transactions"
    __table_args__ = (
        CheckConstraint("status IN ('PENDING', 'DECIDED')", name="ck_transaction_status"),
        CheckConstraint("attempt_count >= 1", name="ck_transaction_attempt_count"),
    )

    transaction_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    event_id: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    user_id: Mapped[str] = mapped_column(String(128), index=True, nullable=False)
    payload_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(String(24), nullable=False)
    event_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    source_topic: Mapped[str | None] = mapped_column(String(255))
    source_partition: Mapped[int | None] = mapped_column(Integer)
    source_offset: Mapped[int | None] = mapped_column(Integer)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    last_error: Mapped[str | None] = mapped_column(String(255))
    created_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), server_default=text("CURRENT_TIMESTAMP")
    )
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), server_default=text("CURRENT_TIMESTAMP")
    )
    terminal_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class DecisionRow(Base):
    __tablename__ = "decisions"
    __table_args__ = (
        CheckConstraint("action IN ('ALLOW', 'REVIEW', 'BLOCK')", name="ck_decision_action"),
        CheckConstraint(
            "inference_status IN ('SCORED', 'FAILED', 'NOT_RUN')",
            name="ck_decision_inference_status",
        ),
        CheckConstraint("score IS NULL OR (score >= 0 AND score <= 1)", name="ck_decision_score"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    transaction_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("transactions.transaction_id"), unique=True
    )
    action: Mapped[str] = mapped_column(String(12), nullable=False)
    reason_codes: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    explanations: Mapped[list[dict[str, Any]]] = mapped_column(JSON, nullable=False, default=list)
    feature_snapshot: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    feature_version: Mapped[str] = mapped_column(String(64), nullable=False)
    model_version: Mapped[str | None] = mapped_column(String(64))
    policy_version: Mapped[str] = mapped_column(String(64), nullable=False)
    score: Mapped[float | None] = mapped_column(Float)
    inference_status: Mapped[str] = mapped_column(String(24), nullable=False)
    inference_latency_ms: Mapped[float | None] = mapped_column(Float)
    feature_included: Mapped[bool] = mapped_column(Boolean, nullable=False)
    decided_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), server_default=text("CURRENT_TIMESTAMP")
    )


class ReviewLabelRow(Base):
    __tablename__ = "review_labels"
    __table_args__ = (
        UniqueConstraint("decision_id", "submission_key", name="uq_review_submission"),
        UniqueConstraint("decision_id", name="uq_review_decision_initial_label"),
        CheckConstraint("label IN ('FRAUD', 'NOT_FRAUD', 'UNSURE')", name="ck_review_label"),
        CheckConstraint("label_source IN ('HUMAN', 'SIMULATED')", name="ck_review_label_source"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    decision_id: Mapped[int] = mapped_column(Integer, ForeignKey("decisions.id"), nullable=False)
    transaction_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("transactions.transaction_id"), nullable=False, index=True
    )
    label: Mapped[str] = mapped_column(String(16), nullable=False)
    reviewer_id: Mapped[str] = mapped_column(String(128), nullable=False)
    submission_key: Mapped[str] = mapped_column(String(128), nullable=False)
    label_source: Mapped[str] = mapped_column(String(24), nullable=False, default="HUMAN")
    model_version: Mapped[str | None] = mapped_column(String(64))
    feature_version: Mapped[str] = mapped_column(String(64), nullable=False)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), server_default=text("CURRENT_TIMESTAMP")
    )


def make_engine(url: str | None = None):
    resolved_url = url or get_settings().database_url
    connect_args = {"connect_timeout": 5} if resolved_url.startswith("postgresql") else {}
    return create_engine(resolved_url, pool_pre_ping=True, connect_args=connect_args)


def session_factory(url: str | None = None):
    return sessionmaker(make_engine(url), expire_on_commit=False)


def ping_database(url: str | None = None) -> bool:
    with make_engine(url).connect() as connection:
        connection.execute(text("SELECT 1"))
    return True
