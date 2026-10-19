# Checkpoint 10 — Query and human-review API

## Outcome

Checkpoint 10 exposes persisted facts without recalculating fraud decisions. FastAPI controllers remain thin; `QueryService` and `ReviewService` own database behavior in `src/fraud_engine/services.py`.

## Endpoints

| Method and path | Purpose |
|---|---|
| `GET /transactions/{transaction_id}` | raw event, status, Kafka source coordinates, attempts, and timestamps |
| `GET /decisions/{transaction_id}` | action, reasons, score, snapshot, all versions, and inference status |
| `GET /users/{user_id}/risk-history` | newest persisted decisions for one user |
| `GET /reviews` | REVIEW decisions and their optional human label |
| `POST /reviews/{transaction_id}/label` | create one immutable initial label |
| `GET /metrics/summary` | transaction/status/action/review-label counts |
| `GET /system/health` | bounded PostgreSQL, Redis, and Kafka probes |

History and review-list endpoints accept `limit` from 1 through 500, defaulting to 100.

## Thin controller pattern

Controllers translate HTTP input and service exceptions only:

```python
@app.get("/decisions/{transaction_id}")
def get_decision(transaction_id: str, session: SessionDependency):
    try:
        return QueryService(session).decision(transaction_id)
    except ResourceNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
```

SQL queries, idempotency, race handling, and response mapping stay in services. This makes those rules testable without an HTTP server and prevents query endpoints from accidentally invoking Redis, inference, or decision policy code.

## Human label contract

Accepted labels are exactly:

- `FRAUD`
- `NOT_FRAUD`
- `UNSURE`

A request also supplies `reviewer_id` and `submission_key`. The stored label copies the immutable decision's `decision_id`, `transaction_id`, `model_version`, and `feature_version`. This preserves the precise model context that the reviewer evaluated.

Example:

```json
POST /reviews/txn-123/label
{
  "label": "FRAUD",
  "reviewer_id": "analyst-17",
  "submission_key": "case-tool-request-8842"
}
```

The database enforces both one initial label per decision and uniqueness of `(decision_id, submission_key)`. Service behavior is:

1. Find the immutable decision and require action `REVIEW`.
2. If no label exists, insert it and copy decision/model/feature identities.
3. If the same key, label, and reviewer are submitted again, return the existing row with `idempotent_replay=true`.
4. If content differs or another initial label already exists, return HTTP 409.
5. If concurrent inserts race, catch the uniqueness error, read the winner, and apply the same equality check.

Labels are never overwritten. A future correction feature must create an explicit correction record with author, reason, and link to the superseded label.

## Data ownership

The API reads PostgreSQL because it owns completed facts. It does not query Redis for history; Redis is disposable and may be rebuilding. It does not run the model or policy; those versions and outputs are already frozen in the decision. Metrics are simple operational counts rather than a training evaluation report.

## API tests

`tests/test_api.py` uses FastAPI `TestClient` with an injected SQLite session. It verifies:

- transaction and decision response audit fields;
- ordered user risk history;
- REVIEW queue content before and after labeling;
- action and status metric counts;
- 404 for unknown identities;
- accepted label vocabulary;
- exact replay idempotency;
- 409 on conflicting label submission;
- copied model and feature versions;
- degraded component health response.

The session dependency override exercises the HTTP routes, validation, services, and ORM schema together while remaining deterministic. Container-backed PostgreSQL migration and processor tests provide the database-specific integration layer.

## Errors encountered and resolutions

### Ruff B008 on FastAPI dependencies

Direct `session: Session = Depends(get_session)` declarations triggered Ruff's function-call-in-default rule. The API now uses `Annotated` aliases:

```python
SessionDependency = Annotated[Session, Depends(get_session)]
PageLimit = Annotated[int, Query(ge=1, le=500)]
```

This preserves FastAPI dependency injection while satisfying static checks.

### SQLite rejected PostgreSQL connection arguments

The original engine factory always supplied `connect_timeout`, which is valid for psycopg but not SQLite. `make_engine` now applies that option only to PostgreSQL URLs, allowing isolated API and service tests without changing production connection behavior.

### Duplicate submission race

An application-only existence check can race: two requests may both observe no label. Database unique constraints remain authoritative. `ReviewService` catches `IntegrityError`, rolls back, loads the winning label, and returns it only when the retry is exactly identical.

## Running the API

Apply migrations and start the service:

```powershell
.\.venv\Scripts\alembic.exe upgrade head
.\.venv\Scripts\uvicorn.exe fraud_engine.api:app --reload
```

Interactive OpenAPI documentation is available at `/docs`. This local learning build records a caller-supplied reviewer identity; production deployment must put authenticated reviewer identity and authorization in front of the label endpoint.
