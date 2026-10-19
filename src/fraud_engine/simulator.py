"""Deterministic synthetic transactions with learnable fraud campaigns."""

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from fraud_engine.events import TransactionEvent

MERCHANTS = ("groceries", "electronics", "travel", "fuel", "fashion", "utilities")
PAYMENTS = ("card", "upi", "wallet")
COUNTRIES = ("IN", "SG", "AE", "US")


@dataclass(frozen=True)
class SyntheticDataset:
    users: pd.DataFrame
    events: pd.DataFrame
    labels: pd.DataFrame
    generator_truth: pd.DataFrame


def generate_dataset(
    seed: int = 17,
    n_users: int = 2000,
    n_transactions: int = 200_000,
    start: datetime = datetime(2025, 1, 1, tzinfo=UTC),
    days: int = 30,
) -> SyntheticDataset:
    if n_users < 2 or n_transactions < 10 or days < 2:
        raise ValueError("dataset needs at least 2 users, 10 transactions and 2 days")
    if start.tzinfo is None:
        raise ValueError("start must be timezone-aware")
    rng = np.random.default_rng(seed)
    profile_amounts = rng.lognormal(mean=np.log(230_000), sigma=0.85, size=n_users)
    account_age_start = rng.integers(1, 1000, size=n_users)
    main_devices = [f"device-{i}-main" for i in range(n_users)]
    users = pd.DataFrame(
        {
            "user_id": [f"user-{i:04d}" for i in range(n_users)],
            "profile_amount_paise": np.round(profile_amounts).astype(int),
            "account_age_days_at_start": account_age_start,
            "main_device_id": main_devices,
        }
    )
    popularity = rng.gamma(shape=1.6, scale=1.0, size=n_users)
    popularity /= popularity.sum()
    attack_rows = max(3, int(round(n_transactions * 0.02)))
    normal_rows = n_transactions - attack_rows
    drafts: list[dict] = []

    def draft(
        uidx: int,
        second: float,
        *,
        scenario: str = "LEGITIMATE",
        campaign_id: str = "",
        campaign_position: int = 0,
        amount_multiplier: float | None = None,
        device: str | None = None,
        country: str | None = None,
        merchant: str | None = None,
        probability: float = 0.0015,
    ) -> None:
        event_time = start + timedelta(seconds=float(second))
        generated_new_device = device is not None
        resolved_device = device or (
            f"device-{uidx}-alt-{rng.integers(1, 40)}"
            if rng.random() < 0.08
            else main_devices[uidx]
        )
        resolved_country = country or str(rng.choice(COUNTRIES, p=[0.96, 0.018, 0.014, 0.008]))
        multiplier = amount_multiplier if amount_multiplier is not None else rng.lognormal(0, 0.55)
        drafts.append(
            {
                "uidx": int(uidx),
                "second": float(second),
                "event_time": event_time,
                "amount": max(1, int(profile_amounts[uidx] * multiplier)),
                "device": resolved_device,
                "country": resolved_country,
                "merchant": merchant or str(rng.choice(MERCHANTS)),
                "payment": str(rng.choice(PAYMENTS, p=[0.58, 0.33, 0.09])),
                "scenario": scenario,
                "campaign_id": campaign_id,
                "campaign_position": campaign_position,
                "generated_new_device": generated_new_device,
                "probability": probability,
            }
        )

    selected_users = rng.choice(n_users, size=normal_rows, p=popularity)
    normal_seconds = rng.uniform(0, days * 86_400, size=normal_rows)
    for uidx, second in zip(selected_users, normal_seconds, strict=True):
        draft(int(uidx), float(second))

    # Campaigns create event-time patterns observable by the online features. Labels remain
    # stochastic so suspicious legitimate and ordinary-looking fraudulent cases still exist.
    remaining = attack_rows
    campaign_number = 0
    while remaining:
        uidx = int(rng.choice(n_users, p=popularity))
        base = float(rng.uniform(86_400, max(86_401, (days - 1) * 86_400)))
        kind = campaign_number % 3
        campaign_id = f"campaign-{campaign_number:07d}"
        if kind == 0 and remaining >= 4:  # account takeover
            device = f"device-{uidx}-ato-{campaign_number}"
            foreign = str(rng.choice(COUNTRIES[1:]))
            for position in range(4):
                draft(
                    uidx,
                    base + position * 35,
                    scenario="ACCOUNT_TAKEOVER",
                    campaign_id=campaign_id,
                    campaign_position=position,
                    amount_multiplier=float(rng.uniform(0.7, 2.2)),
                    device=device,
                    country=foreign,
                    merchant="electronics" if position >= 2 else None,
                    probability=0.88,
                )
            remaining -= 4
        elif kind == 1 and remaining >= 6:  # card testing then monetisation
            device = f"device-{uidx}-testing-{campaign_number}"
            for position in range(6):
                final = position == 5
                draft(
                    uidx,
                    base + position * 25,
                    scenario="CARD_TESTING",
                    campaign_id=campaign_id,
                    campaign_position=position,
                    amount_multiplier=(
                        float(rng.uniform(4.0, 8.0)) if final else float(rng.uniform(0.02, 0.08))
                    ),
                    device=device,
                    country="IN",
                    merchant="electronics" if final else "utilities",
                    probability=0.95 if final else 0.72,
                )
            remaining -= 6
        else:  # isolated amount anomaly
            draft(
                uidx,
                base,
                scenario="AMOUNT_ANOMALY",
                campaign_id=campaign_id,
                amount_multiplier=float(rng.uniform(6.0, 12.0)),
                probability=0.90,
            )
            remaining -= 1
        campaign_number += 1

    drafts.sort(key=lambda row: (row["event_time"], row["uidx"], row["campaign_id"]))
    rows: list[dict] = []
    labels: list[dict] = []
    truth: list[dict] = []
    for i, item in enumerate(drafts):
        uidx = item["uidx"]
        event_time = item["event_time"]
        second = item["second"]
        # A small false-negative tail and rare background fraud keep the task non-deterministic.
        probability = float(item["probability"])
        fraud = int(rng.random() < probability)
        delay = float(rng.uniform(0, 20))
        if rng.random() < 0.015:
            delay += float(rng.uniform(60, 240))
        producer_time = event_time + timedelta(seconds=delay)
        event = TransactionEvent(
            event_id=f"evt-{i:08d}",
            transaction_id=f"tx-{i:08d}",
            user_id=f"user-{uidx:04d}",
            event_timestamp=event_time,
            producer_timestamp=producer_time,
            amount=item["amount"],
            merchant_category=item["merchant"],
            country=item["country"],
            device_id=item["device"],
            payment_method=item["payment"],
            account_age_days=int(account_age_start[uidx] + second // 86_400),
        )
        rows.append(event.model_dump(mode="json"))
        labels.append(
            {
                "transaction_id": event.transaction_id,
                "fraud_label": fraud,
                "label_available_at": (
                    producer_time + timedelta(days=int(rng.integers(1, 3)))
                ).isoformat(),
            }
        )
        truth.append(
            {
                "transaction_id": event.transaction_id,
                "scenario": item["scenario"],
                "campaign_id": item["campaign_id"],
                "campaign_position": item["campaign_position"],
                "attack_intent": int(item["scenario"] != "LEGITIMATE"),
                "hidden_profile_amount_paise": int(profile_amounts[uidx]),
                "generated_new_device": int(item["generated_new_device"]),
                "generated_foreign_country": int(item["country"] != "IN"),
                "latent_fraud_probability": probability,
            }
        )
    events = (
        pd.DataFrame(rows)
        .sort_values(["producer_timestamp", "transaction_id"], kind="stable")
        .reset_index(drop=True)
    )
    label_frame = pd.DataFrame(labels)
    truth_frame = pd.DataFrame(truth)
    return SyntheticDataset(
        users=users, events=events, labels=label_frame, generator_truth=truth_frame
    )


def write_dataset(dataset: SyntheticDataset, directory: Path, seed: int) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    dataset.users.to_csv(directory / "users.csv", index=False)
    dataset.events.to_csv(directory / "events.csv", index=False)
    dataset.labels.to_csv(directory / "labels.csv", index=False)
    dataset.generator_truth.to_csv(directory / "generator_truth.csv", index=False)
    (directory / "manifest.json").write_text(
        json.dumps(
            {
                "seed": seed,
                "generator_version": "synthetic-v2",
                "rows": len(dataset.events),
                "fraud_prevalence": float(dataset.labels["fraud_label"].mean()),
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", type=int, default=17)
    parser.add_argument("--users", type=int, default=2000)
    parser.add_argument("--transactions", type=int, default=200_000)
    parser.add_argument("--output", type=Path, default=Path("data/generated"))
    args = parser.parse_args()
    write_dataset(
        generate_dataset(args.seed, args.users, args.transactions), args.output, args.seed
    )


if __name__ == "__main__":
    main()
