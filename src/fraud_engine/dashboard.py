"""Lightweight Streamlit analyst dashboard backed only by FastAPI."""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class ApiError(RuntimeError):
    pass


def cli_main() -> None:
    """Launch this module with the Streamlit runner."""
    from streamlit.web import cli as streamlit_cli

    sys.argv = ["streamlit", "run", __file__, *sys.argv[1:]]
    streamlit_cli.main()


@dataclass(frozen=True)
class ApiClient:
    base_url: str
    timeout_seconds: float = 3.0
    api_key: str | None = None

    def request(self, path: str, *, method: str = "GET", body: dict | None = None) -> Any:
        payload = json.dumps(body).encode() if body is not None else None
        request = Request(
            f"{self.base_url.rstrip('/')}{path}",
            data=payload,
            method=method,
            headers={
                "Content-Type": "application/json",
                **({"X-Review-API-Key": self.api_key} if self.api_key else {}),
            },
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                return json.loads(response.read())
        except HTTPError as exc:
            detail = exc.read().decode(errors="replace")
            raise ApiError(f"API returned {exc.code}: {detail}") from exc
        except (URLError, TimeoutError) as exc:
            raise ApiError(f"API unavailable: {exc}") from exc

    def get(self, path: str) -> Any:
        return self.request(path)

    def post(self, path: str, body: dict) -> Any:
        return self.request(path, method="POST", body=body)


def recent_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for item in payload.get("items", []):
        decision = item.get("decision") or {}
        rows.append(
            {
                "transaction_id": item["transaction_id"],
                "user_id": item["user_id"],
                "status": item["status"],
                "event_time": item["event_timestamp"],
                "action": decision.get("action"),
                "score": decision.get("score"),
                "reasons": ", ".join(decision.get("reason_codes", [])),
                "model": decision.get("model_version"),
            }
        )
    return rows


def main() -> None:
    try:
        import streamlit as st
    except ImportError as exc:
        raise SystemExit(
            "Install dashboard dependencies with: pip install -e '.[dashboard]'"
        ) from exc

    st.set_page_config(page_title="Fraud Decision Console", page_icon="🛡️", layout="wide")
    api = ApiClient(
        os.getenv("FRAUD_API_URL", "http://127.0.0.1:8000"),
        api_key=os.getenv("FRAUD_REVIEW_API_KEY"),
    )
    st.title("Fraud Decision Console")
    st.caption("Persisted decisions and delayed labels; this UI never recalculates risk.")

    try:
        health = api.get("/system/health")
        summary = api.get("/metrics/summary")
        performance = api.get("/metrics/model-performance")
        recent = api.get("/transactions?limit=100")
        reviews = api.get("/reviews?limit=100")
    except ApiError as exc:
        st.error(str(exc))
        st.stop()

    if health["status"] == "ok":
        st.success("All dependencies responding")
    else:
        st.warning(f"System degraded: {health['components']}")

    columns = st.columns(5)
    columns[0].metric("Transactions", summary["transactions"])
    columns[1].metric("ALLOW", summary["actions"]["ALLOW"])
    columns[2].metric("REVIEW", summary["actions"]["REVIEW"])
    columns[3].metric("BLOCK", summary["actions"]["BLOCK"])
    columns[4].metric("Consumer lag", summary["consumer_lag"].get("total") or 0)

    overview, review_tab, inspect_tab, model_tab = st.tabs(
        ["Live overview", "Review queue", "Transaction inspector", "Model performance"]
    )
    with overview:
        latency_left, latency_right = st.columns(2)
        latency_left.metric("Processing P95 ms", summary["processing_latency_ms"].get("p95") or 0)
        latency_right.metric("Inference P95 ms", summary["inference_latency_ms"].get("p95") or 0)
        st.bar_chart(summary["actions"])
        st.subheader("Recent transactions")
        st.dataframe(recent_rows(recent), use_container_width=True, hide_index=True)
        st.subheader("Errors and recovery signals")
        st.json(
            {
                "fallbacks": summary["fallbacks"],
                "late_events": summary["late_events"],
                "duplicate_deliveries": summary["duplicate_deliveries"],
                "consumer_lag": summary["consumer_lag"],
            }
        )

    with review_tab:
        items = reviews.get("items", [])
        unlabeled = [item for item in items if item.get("review_label") is None]
        st.write(f"{len(unlabeled)} unlabeled REVIEW decisions")
        st.dataframe(unlabeled, use_container_width=True, hide_index=True)
        if unlabeled:
            transaction_id = st.selectbox(
                "Transaction to label", [item["transaction_id"] for item in unlabeled]
            )
            selected = next(item for item in unlabeled if item["transaction_id"] == transaction_id)
            st.write("Why this decision")
            st.dataframe(selected.get("explanations", []), hide_index=True)
            label = st.radio("Outcome", ["FRAUD", "NOT_FRAUD", "UNSURE"], horizontal=True)
            reviewer = st.text_input("Reviewer ID", value="demo-analyst")
            if st.button("Submit label", type="primary"):
                submission_key = f"dashboard:{reviewer}:{transaction_id}"
                try:
                    saved = api.post(
                        f"/reviews/{transaction_id}/label",
                        {"label": label, "reviewer_id": reviewer, "submission_key": submission_key},
                    )
                    st.success(f"Saved label {saved['label']} for {transaction_id}")
                except ApiError as exc:
                    st.error(str(exc))

    with inspect_tab:
        transaction_id = st.text_input("Transaction ID", key="inspect_transaction")
        if transaction_id:
            try:
                transaction = api.get(f"/transactions/{transaction_id}")
                decision = api.get(f"/decisions/{transaction_id}")
                st.subheader("Raw event")
                st.json(transaction["payload"])
                left, right = st.columns(2)
                left.subheader("Streaming features")
                left.json(decision["feature_snapshot"])
                right.subheader("Decision")
                right.json(decision)
            except ApiError as exc:
                st.error(str(exc))

    with model_tab:
        overall = performance["overall"]
        st.write(
            "Metrics use only matured FRAUD/NOT_FRAUD labels. "
            "UNSURE and unlabeled decisions are excluded."
        )
        metric_columns = st.columns(5)
        for column, key in zip(
            metric_columns, ("precision", "recall", "f1", "pr_auc", "roc_auc"), strict=True
        ):
            value = overall.get(key)
            column.metric(key.upper(), "n/a" if value is None else f"{value:.3f}")
        st.subheader("Decision outcomes")
        st.json(performance["decision_outcomes"])
        st.subheader("By model version")
        st.dataframe(
            [
                {"model_version": key, **value}
                for key, value in performance["by_model_version"].items()
            ],
            use_container_width=True,
            hide_index=True,
        )


if __name__ == "__main__":
    main()
