import json
import logging

from fraud_engine.observability import MetricsRegistry, structured_log


def test_runtime_metrics_are_bounded_and_export_prometheus():
    registry = MetricsRegistry(sample_limit=3)
    for value in (1, 2, 3, 100):
        registry.observe("processing_latency", value)
    registry.increment("transactions_processed", 2)
    snapshot = registry.snapshot()
    assert snapshot["distributions"]["processing_latency"]["count"] == 3
    assert snapshot["distributions"]["processing_latency"]["p50"] == 3
    text = registry.prometheus_text()
    assert "fraud_engine_transactions_processed_total 2" in text
    assert 'quantile="0.95"' in text


def test_structured_log_is_json(caplog):
    logger = logging.getLogger("test-structured")
    with caplog.at_level(logging.INFO):
        structured_log(logger, logging.INFO, "processed", transaction_id="t1", score=0.4)
    payload = json.loads(caplog.records[-1].message)
    assert payload == {"event": "processed", "score": 0.4, "transaction_id": "t1"}
