"""Thin FastAPI controllers over query, review, and health services."""

import secrets
from collections.abc import Iterator
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from fraud_engine.config import get_settings
from fraud_engine.db import session_factory
from fraud_engine.drift import monitoring_records, recent_psi_report
from fraud_engine.evaluation import evaluate_session
from fraud_engine.observability import RUNTIME_METRICS, kafka_consumer_lag
from fraud_engine.services import (
    QueryService,
    ResourceNotFound,
    ReviewConflict,
    ReviewService,
    health_report,
)

app = FastAPI(title="Real-Time Fraud Engine", version="1.0.0")
SessionLocal = session_factory()


class ReviewSubmission(BaseModel):
    label: Literal["FRAUD", "NOT_FRAUD", "UNSURE"]
    reviewer_id: str = Field(min_length=1, max_length=128)
    submission_key: str = Field(min_length=1, max_length=128)


def get_session() -> Iterator[Session]:
    with SessionLocal() as session:
        yield session


SessionDependency = Annotated[Session, Depends(get_session)]
PageLimit = Annotated[int, Query(ge=1, le=500)]


def require_review_auth(x_review_api_key: Annotated[str | None, Header()] = None) -> None:
    configured = get_settings().fraud_review_api_key
    if configured is None:
        raise HTTPException(status_code=503, detail="review API authentication is not configured")
    supplied = x_review_api_key or ""
    if not secrets.compare_digest(supplied, configured.get_secret_value()):
        raise HTTPException(status_code=401, detail="invalid review API key")


def _not_found(exc: ResourceNotFound) -> HTTPException:
    return HTTPException(status_code=404, detail=str(exc))


@app.middleware("http")
async def observe_http(request: Request, call_next):
    from time import perf_counter

    started = perf_counter()
    try:
        response = await call_next(request)
        RUNTIME_METRICS.increment(f"http_{response.status_code}")
        return response
    finally:
        RUNTIME_METRICS.increment("http_requests")
        RUNTIME_METRICS.observe("http_latency", (perf_counter() - started) * 1000)


@app.get("/transactions")
def get_recent_transactions(
    session: SessionDependency,
    limit: PageLimit = 100,
):
    return QueryService(session).recent_transactions(limit)


@app.get("/transactions/{transaction_id}")
def get_transaction(transaction_id: str, session: SessionDependency):
    try:
        return QueryService(session).transaction(transaction_id)
    except ResourceNotFound as exc:
        raise _not_found(exc) from exc


@app.get("/decisions/{transaction_id}")
def get_decision(transaction_id: str, session: SessionDependency):
    try:
        return QueryService(session).decision(transaction_id)
    except ResourceNotFound as exc:
        raise _not_found(exc) from exc


@app.get("/users/{user_id}/risk-history")
def get_risk_history(
    user_id: str,
    session: SessionDependency,
    limit: PageLimit = 100,
):
    return QueryService(session).risk_history(user_id, limit)


@app.get("/reviews")
def get_reviews(
    session: SessionDependency,
    _authenticated: Annotated[None, Depends(require_review_auth)],
    limit: PageLimit = 100,
):
    return QueryService(session).reviews(limit)


@app.post("/reviews/{transaction_id}/label", status_code=201)
def submit_review_label(
    transaction_id: str,
    submission: ReviewSubmission,
    session: SessionDependency,
    _authenticated: Annotated[None, Depends(require_review_auth)],
):
    try:
        return ReviewService(session).submit(transaction_id, **submission.model_dump())
    except ResourceNotFound as exc:
        raise _not_found(exc) from exc
    except ReviewConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get("/metrics/summary")
def get_metrics_summary(session: SessionDependency):
    summary = QueryService(session).metrics_summary()
    summary["consumer_lag"] = kafka_consumer_lag()
    return summary


@app.get("/metrics/model-performance")
def get_model_performance(session: SessionDependency):
    return evaluate_session(session)


@app.get("/metrics/runtime")
def get_runtime_metrics():
    return RUNTIME_METRICS.snapshot()


@app.get("/monitoring/export")
def get_monitoring_export(
    session: SessionDependency,
    limit: PageLimit = 100,
):
    return {
        "schema_version": "fraud-monitoring-export-v1",
        "items": monitoring_records(session, limit=limit),
    }


@app.get("/metrics/drift")
def get_drift_metrics(session: SessionDependency):
    return recent_psi_report(session)


@app.get("/metrics", response_class=PlainTextResponse)
def get_prometheus_metrics(session: SessionDependency):
    summary = QueryService(session).metrics_summary()
    drift = recent_psi_report(session)
    lines = [RUNTIME_METRICS.prometheus_text().rstrip()]
    lines.extend(
        [
            f"fraud_engine_transactions {summary['transactions']}",
            f"fraud_engine_decisions {summary['decisions']}",
            f"fraud_engine_review_labels {summary['review_labels']}",
            f"fraud_engine_duplicate_deliveries {summary['duplicate_deliveries']}",
            f"fraud_engine_late_events {summary['late_events']}",
        ]
    )
    for action, count in summary["actions"].items():
        lines.append(f'fraud_engine_decisions_by_action{{action="{action}"}} {count}')
    for feature, value in drift["feature_psi"].items():
        if value is not None:
            lines.append(f'fraud_engine_feature_psi{{feature="{feature}"}} {value}')
    if drift["score_psi"] is not None:
        lines.append(f"fraud_engine_score_psi {drift['score_psi']}")
    return "\n".join(lines) + "\n"


@app.get("/system/health")
def get_system_health(session: SessionDependency):
    return health_report(session).as_dict()


@app.get("/system/config")
def public_config() -> dict[str, str]:
    settings = get_settings()
    return {"raw_topic": settings.kafka_raw_topic, "dlq_topic": settings.kafka_dlq_topic}
