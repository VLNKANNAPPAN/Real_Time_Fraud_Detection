"""Versioned, explainable business action separate from the ML score."""

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, model_validator

from fraud_engine.events import TransactionEvent
from fraud_engine.inference import Prediction


class DecisionPolicy(BaseModel):
    policy_version: str = Field(min_length=1)
    review_score: float = Field(ge=0, le=1)
    block_score: float = Field(ge=0, le=1)
    velocity_count_5m: int = Field(ge=1)
    unusual_amount_ratio: float = Field(gt=1)
    small_transaction_count_5m: int = Field(default=3, ge=1)
    minimum_review_signals: int = Field(ge=1)
    minimum_block_signals: int = Field(ge=1)

    @model_validator(mode="after")
    def threshold_order(self):
        if self.review_score >= self.block_score:
            raise ValueError("review_score must be below block_score")
        return self

    @classmethod
    def load(cls, path: Path) -> "DecisionPolicy":
        return cls.model_validate(json.loads(path.read_text(encoding="utf-8")))


@dataclass(frozen=True)
class Decision:
    action: Literal["ALLOW", "REVIEW", "BLOCK"]
    reason_codes: tuple[str, ...]
    policy_version: str
    score: float | None
    model_version: str | None
    feature_version: str
    inference_status: str
    explanations: tuple[dict[str, float | str], ...] = ()


def decide(
    event: TransactionEvent,
    snapshot: dict[str, float | int] | None,
    prediction: Prediction | None,
    policy: DecisionPolicy,
    *,
    fallback_reason: str | None = None,
    feature_version: str = "features-v2",
) -> Decision:
    if fallback_reason is not None:
        if fallback_reason not in (
            "MODEL_UNAVAILABLE",
            "MODEL_TIMEOUT",
            "LATE_EVENT",
            "FEATURE_HISTORY_LIMIT",
        ):
            raise ValueError("unsupported fallback reason")
        if prediction is not None:
            raise ValueError("fallback cannot carry a model prediction")
        return Decision(
            "REVIEW",
            (fallback_reason,),
            policy.policy_version,
            None,
            None,
            feature_version,
            "NOT_RUN" if fallback_reason in ("LATE_EVENT", "FEATURE_HISTORY_LIMIT") else "FAILED",
            (),
        )
    if prediction is None or snapshot is None:
        raise ValueError("normal decision requires prediction and feature snapshot")
    score = prediction.fraud_probability
    if not math.isfinite(score) or not 0 <= score <= 1:
        raise ValueError("invalid score")
    if prediction.feature_version != feature_version:
        raise ValueError("prediction and snapshot feature versions differ")
    signals: list[str] = []
    if snapshot["is_new_device_24h"]:
        signals.append("NEW_DEVICE_24H")
    if snapshot["transaction_count_5m"] >= policy.velocity_count_5m:
        signals.append("HIGH_TX_VELOCITY")
    if snapshot["has_amount_history_24h"] and (
        snapshot["amount_vs_user_average_24h"] >= policy.unusual_amount_ratio
    ):
        signals.append("UNUSUAL_AMOUNT")
    if (
        snapshot.get("small_transaction_count_5m", 0) >= policy.small_transaction_count_5m
        and snapshot.get("amount_vs_user_average_24h", 0) >= policy.unusual_amount_ratio
    ):
        signals.append("CARD_TESTING_PATTERN")
    if event.country != "IN":
        signals.append("FOREIGN_COUNTRY")
    if score >= policy.block_score and len(signals) >= policy.minimum_block_signals:
        return Decision(
            "BLOCK",
            tuple(["MODEL_BLOCK_BAND", *signals]),
            policy.policy_version,
            score,
            prediction.model_version,
            feature_version,
            "SCORED",
            prediction.explanations,
        )
    if score >= policy.review_score or len(signals) >= policy.minimum_review_signals:
        reasons = ["MODEL_REVIEW_BAND"] if score >= policy.review_score else []
        return Decision(
            "REVIEW",
            tuple([*reasons, *signals]),
            policy.policy_version,
            score,
            prediction.model_version,
            feature_version,
            "SCORED",
            prediction.explanations,
        )
    return Decision(
        "ALLOW",
        ("LOW_RISK",),
        policy.policy_version,
        score,
        prediction.model_version,
        feature_version,
        "SCORED",
        prediction.explanations,
    )
