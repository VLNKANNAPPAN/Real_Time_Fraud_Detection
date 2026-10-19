"""Pure offline reference semantics for features-v2."""

from collections import defaultdict
from dataclasses import dataclass
from datetime import timedelta

from fraud_engine.events import TransactionEvent

FEATURE_VERSION = "features-v2"
LATENESS = timedelta(minutes=2)
HORIZON = timedelta(hours=24)
FEATURE_NAMES = (
    "transaction_count_5m",
    "transaction_count_1h",
    "average_amount_24h",
    "has_amount_history_24h",
    "amount_vs_user_average_24h",
    "amount_zscore_24h",
    "small_transaction_count_5m",
    "unique_devices_24h",
    "is_new_device_24h",
    "device_age_seconds_24h",
    "unique_countries_24h",
    "unique_merchants_24h",
    "seconds_since_previous_transaction",
    "has_previous_transaction",
    "hour_of_day_deviation_24h",
)


class IdentityConflict(ValueError):
    pass


@dataclass(frozen=True)
class FeatureOutcome:
    status: str  # SCORED or LATE_EVENT
    snapshot: dict[str, float | int] | None
    duplicate: bool = False


def calculate_snapshot(
    event: TransactionEvent, history: list[TransactionEvent]
) -> dict[str, float | int]:
    t = event.event_timestamp
    prior = [item for item in history if t - HORIZON <= item.event_timestamp < t]
    five = sum(item.event_timestamp >= t - timedelta(minutes=5) for item in prior)
    one_hour = sum(item.event_timestamp >= t - timedelta(hours=1) for item in prior)
    average = sum(item.amount for item in prior) / len(prior) if prior else 0.0
    variance = sum((item.amount - average) ** 2 for item in prior) / len(prior) if prior else 0.0
    standard_deviation = variance**0.5
    previous = max((item.event_timestamp for item in prior), default=None)
    device_times = [item.event_timestamp for item in prior if item.device_id == event.device_id]
    mean_hour = sum(item.event_timestamp.hour for item in prior) / len(prior) if prior else 0.0
    raw_hour_difference = abs(event.event_timestamp.hour - mean_hour) if prior else 0.0
    hour_deviation = min(raw_hour_difference, 24 - raw_hour_difference) if prior else 0.0
    small_threshold = average * 0.25
    return {
        "transaction_count_5m": five,
        "transaction_count_1h": one_hour,
        "average_amount_24h": average,
        "has_amount_history_24h": int(bool(prior)),
        "amount_vs_user_average_24h": event.amount / average if prior else 0.0,
        "amount_zscore_24h": (
            max(-20.0, min(20.0, (event.amount - average) / standard_deviation))
            if standard_deviation > 0
            else 0.0
        ),
        "small_transaction_count_5m": sum(
            item.event_timestamp >= t - timedelta(minutes=5) and item.amount <= small_threshold
            for item in prior
        ),
        "unique_devices_24h": len({item.device_id for item in prior}),
        "is_new_device_24h": int(event.device_id not in {item.device_id for item in prior}),
        "device_age_seconds_24h": (
            (t - min(device_times)).total_seconds() if device_times else 0.0
        ),
        "unique_countries_24h": len({item.country for item in prior}),
        "unique_merchants_24h": len({item.merchant_category for item in prior}),
        "seconds_since_previous_transaction": (t - previous).total_seconds() if previous else -1.0,
        "has_previous_transaction": int(previous is not None),
        "hour_of_day_deviation_24h": hour_deviation,
    }


class OfflineFeatureEngine:
    def __init__(self, *, track_identities: bool = True) -> None:
        self._track_identities = track_identities
        self._history: dict[str, list[TransactionEvent]] = defaultdict(list)
        self._watermark: dict[str, object] = {}
        self._seen_events: dict[str, tuple[str, FeatureOutcome]] = {}
        self._seen_transactions: dict[str, str] = {}

    def process(self, event: TransactionEvent) -> FeatureOutcome:
        digest = event.digest() if self._track_identities else ""
        seen = self._seen_events.get(event.event_id) if self._track_identities else None
        if seen:
            if seen[0] != digest:
                raise IdentityConflict("event_id reused with modified payload")
            return FeatureOutcome(seen[1].status, seen[1].snapshot, duplicate=True)
        other_event_id = (
            self._seen_transactions.get(event.transaction_id) if self._track_identities else None
        )
        if other_event_id is not None:
            raise IdentityConflict("transaction_id reused under a different event_id")
        watermark = self._watermark.get(event.user_id)
        if watermark is not None and event.event_timestamp < watermark - LATENESS:
            outcome = FeatureOutcome("LATE_EVENT", None)
        else:
            snapshot = calculate_snapshot(event, self._history[event.user_id])
            outcome = FeatureOutcome("SCORED", snapshot)
            self._history[event.user_id].append(event)
            new_watermark = (
                max(watermark, event.event_timestamp) if watermark else event.event_timestamp
            )
            self._watermark[event.user_id] = new_watermark
            cutoff = new_watermark - HORIZON - LATENESS
            self._history[event.user_id] = [
                item for item in self._history[event.user_id] if item.event_timestamp >= cutoff
            ]
        if self._track_identities:
            self._seen_events[event.event_id] = (digest, outcome)
            self._seen_transactions[event.transaction_id] = event.event_id
        return outcome
