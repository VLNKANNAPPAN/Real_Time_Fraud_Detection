import pandas as pd

from fraud_engine.simulator import generate_dataset


def test_generation_is_deterministic_and_labels_separate():
    first = generate_dataset(seed=42, n_users=40, n_transactions=500)
    second = generate_dataset(seed=42, n_users=40, n_transactions=500)
    pd.testing.assert_frame_equal(first.events, second.events)
    pd.testing.assert_frame_equal(first.labels, second.labels)
    pd.testing.assert_frame_equal(first.generator_truth, second.generator_truth)
    assert "fraud_label" not in first.events.columns
    assert "fraud_label" not in first.users.columns
    assert "attack_intent" not in first.events.columns
    assert first.events["event_timestamp"].le(first.events["producer_timestamp"]).all()
    assert first.events["amount"].gt(0).all()
    assert first.events["account_age_days"].ge(0).all()


def test_class_imbalance_and_nontrivial_cases():
    data = generate_dataset(seed=17, n_users=250, n_transactions=6000)
    rate = data.labels["fraud_label"].mean()
    assert 0.005 < rate < 0.15
    joined = data.events.merge(data.labels, on="transaction_id")
    assert (
        data.generator_truth["scenario"]
        .isin(["ACCOUNT_TAKEOVER", "CARD_TESTING", "AMOUNT_ANOMALY"])
        .any()
    )
    assert ((joined["amount"] < joined["amount"].median()) & (joined["fraud_label"] == 1)).any()
    assert (
        (joined["amount"] > joined["amount"].quantile(0.95)) & (joined["fraud_label"] == 0)
    ).any()
