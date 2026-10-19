# Checkpoint 12 — Streamlit dashboard

## Scope

The dashboard is an internal analyst and demonstration interface, implemented in `dashboard.py`. It calls FastAPI through a small standard-library HTTP client. It never reads PostgreSQL or Redis directly and never reruns features, inference, or policy.

## Views

### Live overview

- transaction and ALLOW/REVIEW/BLOCK counts;
- PostgreSQL, Redis, and Kafka health;
- consumer lag;
- processing and inference P95 latency;
- recent transaction table with score, action, reasons, and model version;
- late event, duplicate delivery, and fallback counts.

### Review queue

- unlabeled REVIEW decisions;
- FRAUD, NOT_FRAUD, and UNSURE controls;
- reviewer identity and deterministic submission key;
- API conflict display for duplicate/conflicting labels.

### Transaction inspector

- immutable raw event;
- frozen streaming feature snapshot;
- score and inference status;
- model, feature, and policy versions;
- action and reason codes.

### Model performance

- matured-label precision, recall, F1, PR-AUC, and ROC-AUC;
- decision outcomes;
- metrics by model version.

## Run

```powershell
.\.venv\Scripts\streamlit.exe run src/fraud_engine/dashboard.py
```

Set `FRAUD_API_URL` when FastAPI is not at `http://127.0.0.1:8000`.

## Design choices

Streamlit matches the current need: a lightweight local analyst view with little UI infrastructure. React remains a future option for strong authentication, role-based workflows, complex case management, and higher UI polish. The dashboard is intentionally not a source of truth.

## Testing

The API supplies integration coverage for every data source used by the dashboard. A deterministic dashboard helper test verifies that recent decisions are flattened without losing explanation fields. The dashboard dependency is optional under `.[dashboard]`, so backend CI does not need to boot a browser.
