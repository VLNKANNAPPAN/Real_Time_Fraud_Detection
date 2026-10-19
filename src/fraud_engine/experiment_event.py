"""Publish one traceable event for live resilience demonstrations."""

import argparse
import json
from datetime import UTC, datetime
from uuid import uuid4

from fraud_engine.events import TransactionEvent
from fraud_engine.kafka_io import make_producer, produce_event


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--prefix", default="experiment")
    args = parser.parse_args()
    suffix = uuid4().hex[:12]
    now = datetime.now(UTC)
    event = TransactionEvent(
        event_id=f"{args.prefix}-event-{suffix}",
        transaction_id=f"{args.prefix}-transaction-{suffix}",
        user_id=f"{args.prefix}-user",
        event_timestamp=now,
        producer_timestamp=now,
        amount=125_00,
        merchant_category="electronics",
        country="IN",
        device_id=f"{args.prefix}-device",
        payment_method="card",
        account_age_days=365,
    )
    produce_event(make_producer(), event)
    print(json.dumps(event.model_dump(mode="json"), sort_keys=True))


if __name__ == "__main__":
    main()
