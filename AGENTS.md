# Real-Time Fraud / Anomaly Detection Engine

This repository is the project memory. Read the architecture and relevant phase/ADR documents before changing behavior. Checkpoints 0–18 cover the design, scaffold, simulator, baseline model, ingestion, Redis features, inference, decision policy, idempotent processor, resilience, API, delayed labels, dashboard, observability, performance, failure injection, drift export, polish, and final audit.

- Stream: one local Kafka KRaft broker; at-least-once consumption, keyed by `user_id`.
- Online state: Redis is a rebuildable, bounded cache of per-user event-time history.
- Durable record: PostgreSQL owns event/transaction identity, completed decisions, feature snapshots, and later review labels.
- ML: a versioned scikit-learn preprocessing and model pipeline estimates risk; a separate versioned decision policy chooses ALLOW, REVIEW, or BLOCK.
- Interfaces: FastAPI for queries/review and a lightweight Streamlit dashboard. The simulator publishes to Kafka.

Core design: [architecture](docs/architecture.md), [event model](docs/event-model.md), [invariants](docs/system-invariants.md), [streaming semantics](docs/streaming-semantics.md), [features](docs/feature-definitions.md), [ML methodology](docs/ml-methodology.md), [failure model](docs/failure-model.md). Decisions live in `docs/decisions/`; checkpoint records live in `docs/phases/`.

Rules: never assume exactly-once delivery or global Kafka ordering; deduplicate before mutating state; enforce identities in PostgreSQL; make late events explicit; use event-time feature semantics and compatible offline/online preprocessing; persist the exact model, feature, and policy versions (or explicit `NOT_RUN` status); commit offsets only after a durable terminal outcome; treat Redis as disposable; keep inference score separate from business action. Add deterministic tests for each implemented invariant.
