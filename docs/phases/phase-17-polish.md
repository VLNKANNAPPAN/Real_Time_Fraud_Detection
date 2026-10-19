# Phase 17 — Project polish and demo

## Delivered

- A production-style README with architecture, guarantees, quick start, commands, API routes, measured results, and limitations.
- `scripts/start-demo.ps1` and `scripts/stop-demo.ps1` for repeatable local operation.
- `scripts/run-performance.ps1` for the five bounded performance scenarios.
- A rendered dashboard screenshot using persisted live decisions.
- CI checks for formatting, lint, compilation, and deterministic tests.
- An interview guide tied to the actual implementation and observed tradeoffs.

## Demo flow

1. Start infrastructure and applications with `scripts/start-demo.ps1`.
2. Publish generated events with `fraud-produce --limit 100`.
3. Inspect consumer lag and outcomes in Streamlit.
4. Open a transaction and show raw payload, feature snapshot, versions, score, action, and reasons.
5. Submit an idempotent review label and view matured performance metrics.
6. Show `docs/failure-experiments.md` and the measured performance artifacts.
