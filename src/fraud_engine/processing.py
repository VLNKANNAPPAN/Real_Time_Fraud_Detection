"""Idempotent Kafka-to-PostgreSQL fraud decision orchestration."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from threading import Lock, RLock
from time import perf_counter
from typing import Any, Literal, Protocol

from redis.exceptions import RedisError
from sqlalchemy import or_, select, text
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from fraud_engine.db import DecisionRow, TransactionRow
from fraud_engine.decision import Decision, DecisionPolicy, decide
from fraud_engine.events import TransactionEvent
from fraud_engine.features import FEATURE_VERSION, IdentityConflict
from fraud_engine.redis_features import HistoryLimitExceeded, StateRecoveryRequired
from fraud_engine.resilience import (
    InferenceTimedOut,
    NonRetryableIdentityConflict,
    RetryableDatabaseError,
    RetryableFeatureError,
    RetryableRedisError,
    call_with_timeout,
)


class FeatureEngine(Protocol):
    def is_initialized(self, user_id: str) -> bool: ...

    def process(self, event: TransactionEvent): ...

    def restore_completed(self, user_id: str, events: list[TransactionEvent]) -> None: ...


class Predictor(Protocol):
    def predict(self, event: TransactionEvent, snapshot: dict, feature_version: str): ...


@dataclass(frozen=True)
class UnavailablePredictor:
    reason: str

    def predict(self, event: TransactionEvent, snapshot: dict, feature_version: str):
        raise RuntimeError(self.reason)


@dataclass(frozen=True)
class SourceMetadata:
    topic: str
    partition: int
    offset: int


@dataclass(frozen=True)
class ProcessingResult:
    outcome: Literal["DECIDED", "DUPLICATE"]
    transaction_id: str
    decision_id: int
    action: str
    reason_codes: tuple[str, ...]
    model_version: str | None
    feature_version: str
    processing_latency_ms: float = 0.0
    feature_latency_ms: float = 0.0
    persistence_latency_ms: float = 0.0
    inference_latency_ms: float | None = None


_local_lock_guard = Lock()
_local_user_locks: defaultdict[str, RLock] = defaultdict(RLock)


@contextmanager
def locked_user_session(factory: sessionmaker, user_id: str) -> Iterator[Session]:
    """Hold a session-level PostgreSQL lock across claim, features, and final commit.

    SQLite uses an in-process lock for deterministic tests. Production correctness
    uses PostgreSQL's session advisory lock on one pinned connection.
    """
    bind = factory.kw.get("bind")
    if bind is None:
        raise RuntimeError("session factory must have a bound engine")
    if bind.dialect.name == "postgresql":
        connection = bind.connect()
        session: Session | None = None
        try:
            connection.execute(
                text("SELECT pg_advisory_lock(hashtextextended(:user_id, 0))"),
                {"user_id": user_id},
            )
            connection.commit()
            session = Session(bind=connection, expire_on_commit=False)
            yield session
        finally:
            if session is not None:
                session.rollback()
                session.close()
            try:
                connection.execute(
                    text("SELECT pg_advisory_unlock(hashtextextended(:user_id, 0))"),
                    {"user_id": user_id},
                )
                connection.commit()
            finally:
                connection.close()
        return

    with _local_lock_guard:
        lock = _local_user_locks[user_id]
    with lock:
        with factory() as session:
            yield session


class FraudProcessor:
    def __init__(
        self,
        sessions: sessionmaker,
        feature_engine: FeatureEngine,
        predictor: Predictor,
        policy: DecisionPolicy,
        *,
        inference_timeout_seconds: float = 2.0,
        fault_hook: Callable[[str], None] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.sessions = sessions
        self.feature_engine = feature_engine
        self.predictor = predictor
        self.policy = policy
        self.inference_timeout_seconds = inference_timeout_seconds
        self.fault_hook = fault_hook or (lambda _: None)
        self.clock = clock or (lambda: datetime.now(UTC))

    def process(self, event: TransactionEvent, source: SourceMetadata) -> ProcessingResult:
        started = perf_counter()
        try:
            with locked_user_session(self.sessions, event.user_id) as session:
                existing = self._claim(session, event, source)
                if existing is not None:
                    return replace(
                        existing, processing_latency_ms=(perf_counter() - started) * 1000
                    )
                self.fault_hook("claim_committed")
                feature_started = perf_counter()
                outcome = self._features(session, event)
                feature_latency_ms = (perf_counter() - feature_started) * 1000
                if outcome.status == "LATE_EVENT":
                    decision = decide(event, None, None, self.policy, fallback_reason="LATE_EVENT")
                    snapshot = None
                    included = False
                    latency = None
                elif outcome.status == "FEATURE_FALLBACK":
                    decision = decide(
                        event, None, None, self.policy, fallback_reason=outcome.reason
                    )
                    snapshot = None
                    included = False
                    latency = None
                elif outcome.status == "SCORED":
                    snapshot = outcome.snapshot
                    if snapshot is None:
                        raise RetryableRedisError("Redis returned SCORED without a snapshot")
                    self.fault_hook("redis_updated")
                    decision, latency = self._infer_and_decide(event, snapshot)
                    included = True
                else:
                    raise RetryableRedisError(f"unsupported feature outcome {outcome.status}")
                self.fault_hook("decision_ready")
                persistence_started = perf_counter()
                result = self._finalize(session, event, decision, snapshot, included, latency)
                persistence_latency_ms = (perf_counter() - persistence_started) * 1000
                self.fault_hook("db_committed")
                return replace(
                    result,
                    processing_latency_ms=(perf_counter() - started) * 1000,
                    feature_latency_ms=feature_latency_ms,
                    persistence_latency_ms=persistence_latency_ms,
                    inference_latency_ms=latency,
                )
        except NonRetryableIdentityConflict:
            raise
        except RedisError as exc:
            raise RetryableRedisError("Redis feature state unavailable") from exc
        except SQLAlchemyError as exc:
            raise RetryableDatabaseError("PostgreSQL claim or decision write failed") from exc

    def _find_identity(self, session: Session, event: TransactionEvent) -> TransactionRow | None:
        rows = list(
            session.scalars(
                select(TransactionRow).where(
                    or_(
                        TransactionRow.event_id == event.event_id,
                        TransactionRow.transaction_id == event.transaction_id,
                    )
                )
            )
        )
        if not rows:
            return None
        exact = next(
            (
                row
                for row in rows
                if row.event_id == event.event_id
                and row.transaction_id == event.transaction_id
                and row.user_id == event.user_id
                and row.payload_digest == event.digest()
            ),
            None,
        )
        if exact is None or len({row.transaction_id for row in rows}) > 1:
            raise NonRetryableIdentityConflict(
                "event_id or transaction_id was reused with different immutable content"
            )
        return exact

    def _claim(
        self, session: Session, event: TransactionEvent, source: SourceMetadata
    ) -> ProcessingResult | None:
        row = self._find_identity(session, event)
        if row is None:
            row = TransactionRow(
                transaction_id=event.transaction_id,
                event_id=event.event_id,
                user_id=event.user_id,
                payload_digest=event.digest(),
                payload=event.model_dump(mode="json"),
                status="PENDING",
                event_timestamp=event.event_timestamp,
                source_topic=source.topic,
                source_partition=source.partition,
                source_offset=source.offset,
                attempt_count=1,
                updated_at=self.clock(),
            )
            session.add(row)
            try:
                session.commit()
            except IntegrityError:
                session.rollback()
                row = self._find_identity(session, event)
                if row is None:
                    raise
        else:
            row.attempt_count += 1
            row.updated_at = self.clock()
            session.commit()

        if row.status == "DECIDED":
            stored = session.scalar(
                select(DecisionRow).where(DecisionRow.transaction_id == event.transaction_id)
            )
            if stored is None:
                raise RetryableDatabaseError("terminal transaction is missing its decision")
            return ProcessingResult(
                "DUPLICATE",
                event.transaction_id,
                stored.id,
                stored.action,
                tuple(stored.reason_codes),
                stored.model_version,
                stored.feature_version,
                inference_latency_ms=stored.inference_latency_ms,
            )
        if row.status != "PENDING":
            raise NonRetryableIdentityConflict(f"identity has terminal status {row.status}")
        return None

    def _completed_history(self, session: Session, user_id: str) -> list[TransactionEvent]:
        payloads = session.execute(
            select(TransactionRow.payload)
            .join(DecisionRow, DecisionRow.transaction_id == TransactionRow.transaction_id)
            .where(
                TransactionRow.user_id == user_id,
                TransactionRow.status == "DECIDED",
                DecisionRow.feature_included.is_(True),
            )
            .order_by(DecisionRow.id)
        ).scalars()
        return [TransactionEvent.model_validate(payload) for payload in payloads]

    def _features(self, session: Session, event: TransactionEvent):
        try:
            if not self.feature_engine.is_initialized(event.user_id):
                self.feature_engine.restore_completed(
                    event.user_id, self._completed_history(session, event.user_id)
                )
            try:
                return self.feature_engine.process(event)
            except StateRecoveryRequired:
                self.feature_engine.restore_completed(
                    event.user_id, self._completed_history(session, event.user_id)
                )
                return self.feature_engine.process(event)
            except HistoryLimitExceeded:
                return _FeatureFallback("FEATURE_HISTORY_LIMIT")
            except IdentityConflict as exc:
                raise NonRetryableIdentityConflict(str(exc)) from exc
        except RedisError:
            raise
        except NonRetryableIdentityConflict:
            raise
        except Exception as exc:
            raise RetryableFeatureError("feature calculation failed before a safe outcome") from exc

    def _infer_and_decide(self, event: TransactionEvent, snapshot: dict[str, Any]):
        started = perf_counter()
        try:
            prediction = call_with_timeout(
                lambda: self.predictor.predict(event, snapshot, FEATURE_VERSION),
                self.inference_timeout_seconds,
            )
        except InferenceTimedOut:
            return (
                decide(event, snapshot, None, self.policy, fallback_reason="MODEL_TIMEOUT"),
                None,
            )
        except Exception:
            return (
                decide(event, snapshot, None, self.policy, fallback_reason="MODEL_UNAVAILABLE"),
                None,
            )
        # Keep policy and model-contract defects out of the availability fallback.
        # Those are processing bugs that should remain visible and retryable.
        return (
            decide(event, snapshot, prediction, self.policy),
            (perf_counter() - started) * 1000,
        )

    def _finalize(
        self,
        session: Session,
        event: TransactionEvent,
        decision: Decision,
        snapshot: dict[str, Any] | None,
        feature_included: bool,
        inference_latency_ms: float | None,
    ) -> ProcessingResult:
        if decision.inference_status == "SCORED" and not feature_included:
            raise ValueError("a scored decision must be feature-included")
        row = session.get(TransactionRow, event.transaction_id)
        if row is None:
            raise RetryableDatabaseError("pending claim disappeared")
        existing = session.scalar(
            select(DecisionRow).where(DecisionRow.transaction_id == event.transaction_id)
        )
        if existing is not None:
            session.rollback()
            return ProcessingResult(
                "DUPLICATE",
                event.transaction_id,
                existing.id,
                existing.action,
                tuple(existing.reason_codes),
                existing.model_version,
                existing.feature_version,
                inference_latency_ms=existing.inference_latency_ms,
            )
        stored = DecisionRow(
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
            inference_latency_ms=inference_latency_ms,
            feature_included=feature_included,
            decided_at=self.clock(),
        )
        session.add(stored)
        row.status = "DECIDED"
        row.last_error = decision.reason_codes[0] if decision.inference_status != "SCORED" else None
        row.updated_at = self.clock()
        row.terminal_at = self.clock()
        self.fault_hook("before_db_commit")
        session.commit()
        return ProcessingResult(
            "DECIDED",
            event.transaction_id,
            stored.id,
            stored.action,
            tuple(stored.reason_codes),
            stored.model_version,
            stored.feature_version,
            inference_latency_ms=stored.inference_latency_ms,
        )


@dataclass(frozen=True)
class _FeatureFallback:
    reason: str
    status: str = "FEATURE_FALLBACK"
