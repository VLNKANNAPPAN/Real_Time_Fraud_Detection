"""Small, controlled processor load test for an 8 GB development machine."""

from __future__ import annotations

import argparse
import json
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
from redis import Redis
from sqlalchemy import delete, select

from fraud_engine.config import get_settings
from fraud_engine.db import DecisionRow, ReviewLabelRow, TransactionRow, session_factory
from fraud_engine.decision import DecisionPolicy
from fraud_engine.events import TransactionEvent
from fraud_engine.inference import InferenceService
from fraud_engine.observability import kafka_consumer_lag
from fraud_engine.processing import FraudProcessor, ProcessingResult, SourceMetadata
from fraud_engine.redis_features import RedisFeatureEngine


@dataclass(frozen=True)
class LoadSample:
    processing_ms: float
    feature_ms: float
    inference_ms: float | None
    persistence_ms: float


def distribution(values: list[float]) -> dict[str, float | int | None]:
    if not values:
        return {"count": 0, "p50": None, "p95": None, "p99": None, "mean": None}
    return {
        "count": len(values),
        "p50": float(np.percentile(values, 50)),
        "p95": float(np.percentile(values, 95)),
        "p99": float(np.percentile(values, 99)),
        "mean": float(np.mean(values)),
    }


def summarize_samples(samples: list[LoadSample], elapsed_seconds: float) -> dict[str, Any]:
    return {
        "events": len(samples),
        "elapsed_seconds": elapsed_seconds,
        "events_per_second": len(samples) / elapsed_seconds if elapsed_seconds else None,
        "end_to_end_ms": distribution([item.processing_ms for item in samples]),
        "redis_feature_ms": distribution([item.feature_ms for item in samples]),
        "model_inference_ms": distribution(
            [item.inference_ms for item in samples if item.inference_ms is not None]
        ),
        "db_persistence_ms": distribution([item.persistence_ms for item in samples]),
    }


class DelayedFeatures:
    def __init__(self, wrapped, seconds: float) -> None:
        self.wrapped = wrapped
        self.seconds = seconds

    def is_initialized(self, user_id):
        return self.wrapped.is_initialized(user_id)

    def restore_completed(self, user_id, events):
        return self.wrapped.restore_completed(user_id, events)

    def process(self, event):
        time.sleep(self.seconds)
        return self.wrapped.process(event)


class DelayedPredictor:
    def __init__(self, wrapped, seconds: float) -> None:
        self.wrapped = wrapped
        self.seconds = seconds

    def predict(self, event, snapshot, feature_version):
        time.sleep(self.seconds)
        return self.wrapped.predict(event, snapshot, feature_version)


def load_events(prefix: str, count: int, *, burst: bool) -> list[TransactionEvent]:
    started = datetime.now(UTC) - timedelta(minutes=1)
    spacing = 0.001 if burst else 0.5
    return [
        TransactionEvent(
            event_id=f"{prefix}-event-{index}",
            transaction_id=f"{prefix}-transaction-{index}",
            user_id=f"{prefix}-user-{index % 20}",
            event_timestamp=started + timedelta(seconds=index * spacing),
            producer_timestamp=started + timedelta(seconds=index * spacing),
            amount=10_000 + index * 17,
            merchant_category="groceries" if index % 3 else "electronics",
            country="IN",
            device_id=f"device-{index % 25}",
            payment_method="card",
            account_age_days=200,
        )
        for index in range(count)
    ]


def run_scenario(scenario: str, count: int) -> dict[str, Any]:
    settings = get_settings()
    sessions = session_factory()
    redis_client = Redis.from_url(settings.redis_url)
    features: Any = RedisFeatureEngine(redis_client)
    predictor: Any = InferenceService(Path(settings.model_artifact_dir))
    fault_hook = None
    if scenario == "slow_redis":
        features = DelayedFeatures(features, 0.02)
    if scenario == "slow_model":
        predictor = DelayedPredictor(predictor, 0.02)
    if scenario == "slow_db":

        def delay_db(stage):
            if stage == "before_db_commit":
                time.sleep(0.02)

        fault_hook = delay_db
    processor = FraudProcessor(
        sessions,
        features,
        predictor,
        DecisionPolicy.load(Path("config/decision-policy-v2.json")),
        inference_timeout_seconds=max(settings.inference_timeout_seconds, 1.0),
        fault_hook=fault_hook,
    )
    prefix = f"load-{uuid.uuid4().hex[:10]}"
    events = load_events(prefix, count, burst=scenario == "burst")
    samples: list[LoadSample] = []
    started = time.perf_counter()
    try:
        for offset, event in enumerate(events):
            result: ProcessingResult = processor.process(
                event, SourceMetadata("load-test.direct", 0, offset)
            )
            samples.append(
                LoadSample(
                    result.processing_latency_ms,
                    result.feature_latency_ms,
                    result.inference_latency_ms,
                    result.persistence_latency_ms,
                )
            )
        elapsed = time.perf_counter() - started
        return {
            "scenario": scenario,
            "pipeline_scope": (
                "processor claim through terminal DB commit; Kafka transport excluded"
            ),
            **summarize_samples(samples, elapsed),
            "consumer_lag_observed_after_run": kafka_consumer_lag(settings),
        }
    finally:
        with sessions() as session:
            transaction_ids = [event.transaction_id for event in events]
            decision_ids = list(
                session.scalars(
                    select(DecisionRow.id).where(DecisionRow.transaction_id.in_(transaction_ids))
                )
            )
            if decision_ids:
                session.execute(
                    delete(ReviewLabelRow).where(ReviewLabelRow.decision_id.in_(decision_ids))
                )
            session.execute(
                delete(DecisionRow).where(DecisionRow.transaction_id.in_(transaction_ids))
            )
            session.execute(
                delete(TransactionRow).where(TransactionRow.transaction_id.in_(transaction_ids))
            )
            session.commit()
        for event in events:
            redis_client.delete(*RedisFeatureEngine._keys(event.user_id, event.event_id))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--scenario",
        choices=("normal", "burst", "slow_model", "slow_db", "slow_redis"),
        default="normal",
    )
    parser.add_argument("--events", type=int, default=100)
    parser.add_argument("--output", type=Path, default=Path("artifacts/performance/latest.json"))
    args = parser.parse_args()
    if not 1 <= args.events <= 2000:
        raise SystemExit("--events must be between 1 and 2000 for this local harness")
    report = run_scenario(args.scenario, args.events)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
