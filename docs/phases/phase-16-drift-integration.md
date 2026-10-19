# Phase 16 — Drift integration

## Objective

Provide a small, versioned monitoring export and reference statistics without coupling the transaction processor to a second platform.

## Implementation

- `fraud-monitoring-export-v1` records feature snapshots, scores, actions, reasons, versions, and matured labels.
- The CLI writes bounded JSON Lines batches for offline systems.
- `GET /monitoring/export` provides the same contract for a local integration.
- PSI compares numeric feature and score distributions; action distribution is kept separate.
- Label-based model quality remains in the delayed-label evaluator.

## Result

The interface is ready for a standalone drift service while the core processor remains independently deployable. See [integration with drift monitoring](../integration-with-drift-monitoring.md).
