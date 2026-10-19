from fraud_engine.load_test import LoadSample, distribution, summarize_samples


def test_load_report_uses_measured_percentiles_and_rate():
    samples = [
        LoadSample(10, 2, 1, 3),
        LoadSample(20, 4, 2, 6),
        LoadSample(30, 6, None, 9),
        LoadSample(40, 8, 4, 12),
    ]
    report = summarize_samples(samples, elapsed_seconds=2)
    assert report["events_per_second"] == 2
    assert report["end_to_end_ms"]["p50"] == 25
    assert report["model_inference_ms"]["count"] == 3
    assert report["db_persistence_ms"]["p95"] > 9
    assert distribution([])["p99"] is None
