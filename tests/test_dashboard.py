from fraud_engine.dashboard import recent_rows


def test_recent_rows_flattens_explainable_decision():
    payload = {
        "items": [
            {
                "transaction_id": "t1",
                "user_id": "u1",
                "status": "DECIDED",
                "event_timestamp": "2025-01-01T00:00:00+00:00",
                "decision": {
                    "action": "REVIEW",
                    "score": 0.7,
                    "reason_codes": ["NEW_DEVICE_24H", "MODEL_REVIEW_BAND"],
                    "model_version": "m1",
                },
            }
        ]
    }
    assert recent_rows(payload) == [
        {
            "transaction_id": "t1",
            "user_id": "u1",
            "status": "DECIDED",
            "event_time": "2025-01-01T00:00:00+00:00",
            "action": "REVIEW",
            "score": 0.7,
            "reasons": "NEW_DEVICE_24H, MODEL_REVIEW_BAND",
            "model": "m1",
        }
    ]
