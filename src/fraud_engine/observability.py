"""Lightweight structured logs and bounded runtime metrics."""

from __future__ import annotations

import json
import logging
from collections import Counter, defaultdict, deque
from threading import Lock
from typing import Any

import numpy as np
from confluent_kafka import Consumer, TopicPartition

from fraud_engine.config import Settings, get_settings


class MetricsRegistry:
    def __init__(self, sample_limit: int = 2048) -> None:
        self._lock = Lock()
        self._counters: Counter[str] = Counter()
        self._samples: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=sample_limit))

    def increment(self, name: str, amount: int = 1) -> None:
        with self._lock:
            self._counters[name] += amount

    def observe(self, name: str, value: float) -> None:
        with self._lock:
            self._samples[name].append(float(value))

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            counters = dict(self._counters)
            samples = {key: list(values) for key, values in self._samples.items()}
        distributions = {}
        for name, values in samples.items():
            distributions[name] = {
                "count": len(values),
                "p50": float(np.percentile(values, 50)),
                "p95": float(np.percentile(values, 95)),
                "p99": float(np.percentile(values, 99)),
            }
        return {"counters": counters, "distributions": distributions}

    def prometheus_text(self) -> str:
        snapshot = self.snapshot()
        lines = ["# fraud_engine bounded process-local metrics"]
        for name, value in sorted(snapshot["counters"].items()):
            lines.append(f"fraud_engine_{name}_total {value}")
        for name, values in sorted(snapshot["distributions"].items()):
            for percentile in ("p50", "p95", "p99"):
                quantile = str(int(percentile[1:]) / 100)
                lines.append(
                    f'fraud_engine_{name}_milliseconds{{quantile="{quantile}"}} '
                    f"{values[percentile]}"
                )
            lines.append(f"fraud_engine_{name}_samples {values['count']}")
        return "\n".join(lines) + "\n"


RUNTIME_METRICS = MetricsRegistry()


def structured_log(logger: logging.Logger, level: int, event: str, **fields: Any) -> None:
    logger.log(level, json.dumps({"event": event, **fields}, default=str, sort_keys=True))


def kafka_consumer_lag(settings: Settings | None = None) -> dict[str, Any]:
    settings = settings or get_settings()
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": "fraud-processor-v1",
            "enable.auto.commit": False,
            "session.timeout.ms": 6000,
        }
    )
    try:
        metadata = consumer.list_topics(settings.kafka_raw_topic, timeout=2)
        partitions = [
            TopicPartition(settings.kafka_raw_topic, number)
            for number in metadata.topics[settings.kafka_raw_topic].partitions
        ]
        committed = consumer.committed(partitions, timeout=2)
        items = []
        for position in committed:
            _, high = consumer.get_watermark_offsets(position, timeout=2, cached=False)
            offset = position.offset if position.offset >= 0 else 0
            items.append(
                {
                    "partition": position.partition,
                    "committed_offset": position.offset,
                    "high_watermark": high,
                    "lag": max(0, high - offset),
                }
            )
        return {"available": True, "total": sum(item["lag"] for item in items), "partitions": items}
    except Exception as exc:
        return {"available": False, "error": type(exc).__name__, "total": None, "partitions": []}
    finally:
        consumer.close()
