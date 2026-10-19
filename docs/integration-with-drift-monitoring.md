# Integration with drift monitoring

## Export contract

`drift.py` produces `fraud-monitoring-export-v1` JSON Lines records containing:

- event and decision timestamps;
- transaction ID;
- feature snapshot and feature version;
- score and model version;
- action, reasons, and policy version;
- label, source, and availability timestamp only after the label is mature.

Run:

```powershell
.\.venv\Scripts\python.exe -m fraud_engine.drift --output artifacts/monitoring/export.jsonl
```

The API also exposes a bounded `GET /monitoring/export` response.

## Monitoring signals

The lightweight comparison functions calculate:

- per-feature Population Stability Index;
- fraud-score PSI;
- ALLOW/REVIEW/BLOCK distribution change.

PSI is a warning signal, not proof that fraud performance changed. Performance degradation requires matured labels and must use the delayed-label evaluation path.

## Standalone platform integration

A separate Data Drift Platform can consume JSONL batches or poll the API, partition by event date and model version, and compare a frozen training/reference population with recent production windows. It should preserve `schema_version`, `model_version`, and `feature_version`; mixing incompatible versions can create false drift alerts.

The platform may alert on feature, score, or action shifts and separately track label-based precision/recall after labels mature. It must not feed current labels back into historical feature snapshots or alter decisions in place.

## Privacy and volume

The export omits the full raw event by default and includes only the identifiers and audit fields needed for monitoring. A production pipeline should pseudonymize transaction IDs, apply access control, set retention, and use a durable batch or outbox delivery mechanism.
