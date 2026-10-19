# Phase 05C — adversarial feature audit

## Attack sequences

| Sequence | Expected result | Test |
|---|---|---|
| exact duplicate immediately or after another event | original frozen vector; no second insertion | `test_redis_matches_offline_for_order_duplicate_and_late` |
| one-second out-of-order within tolerance | only previously completed records with earlier event time | `test_microsecond_boundary_same_time_and_out_of_order_second` |
| more than two minutes late | `LATE_EVENT`, no feature insertion | parity test |
| same transaction ID/new event ID or same event ID/changed payload | explicit identity conflict | `test_identity_conflicts_and_missing_snapshot_require_recovery` |
| Redis restart after pending mutation | restore completed events only, recalculate pending vector | `test_rebuild_after_cache_loss_excludes_pending` |
| equal timestamps and exact 5-minute edge | equal excluded; exact lower boundary included | microsecond boundary test |
| 200-event burst and separate user | parity without cross-user contamination | `test_large_user_burst_and_user_isolation` |
| history cap followed by 25-hour gap | explicit cap, then prune and accept | `test_burst_limit_prunes_expired_history_before_rejecting` |
| TTL expiry | keys expire; explicit completed-event rebuild restores history | `test_ttl_expiry_requires_rebuild` |

## Bugs found and fixed

**Microsecond boundary corruption.** Initial Lua code concatenated the large numeric timestamp into the sorted-set bound. Lua converted it to a short scientific string, rounding away microseconds. In a sequence with a prior event exactly at `t`, Redis counted it while the offline reference excluded it. This violated the exact `[t-W,t)` and same-time invariants. The fix passes the original integer UTC-microsecond timestamp as a decimal string for upper bounds and `ZADD`, formats computed lower bounds with zero decimal places, and stores the watermark as the exact string. The regression test uses events one microsecond around the window boundary and equal timestamps.

**False history-cap rejection after a long gap.** The first script checked `ZCARD` before pruning expired members. A user at the cap could be rejected even after all records fell outside the 24h+2m retention horizon. Pruning now precedes the cap check. The regression test uses a deliberately small cap of three events and a 25-hour gap.

**Kafka test start position.** The live Kafka test initially read outside its intended fresh run and then timed out waiting for the second target record. It now explicitly seeks the assigned partitions to end *before* publishing, and asserts event IDs. This was a test-harness fault, not a feature-engine bug.

## Remaining risks

Fakeredis is not a substitute for live Redis under load. The real Redis script was exercised, but scan latency, memory footprint, concurrent rebalance overlap, and cross-service crash recovery are not proved at this checkpoint. Redis loss without a PostgreSQL loader is an explicit recovery requirement, not a silent zero-history fallback. The adversarial audit is limited to the listed sequences and should be extended during checkpoint 8's end-to-end fault injection.
