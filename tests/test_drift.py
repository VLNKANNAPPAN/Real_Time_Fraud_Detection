from fraud_engine.drift import distribution_report, population_stability_index


def test_distribution_report_separates_drift_from_performance():
    reference = [
        {"features": {"amount_ratio": float(index)}, "score": index / 100, "action": "ALLOW"}
        for index in range(20)
    ]
    current = [
        {
            "features": {"amount_ratio": float(index + 20)},
            "score": (index + 20) / 100,
            "action": "REVIEW",
        }
        for index in range(20)
    ]
    report = distribution_report(reference, current)
    assert report["feature_psi"]["amount_ratio"] > 0
    assert report["score_psi"] > 0
    assert report["decision_distribution"]["reference"]["ALLOW"] == 1
    assert report["decision_distribution"]["current"]["REVIEW"] == 1
    assert "not proof" in report["interpretation"]


def test_psi_requires_enough_data_and_handles_constant_feature():
    assert population_stability_index([1, 2], [2, 3]) is None
    assert population_stability_index([1.0] * 20, [1.0] * 20) == 0
