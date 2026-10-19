# ADR 0003: Redis storage for features-v1

Status: implemented in checkpoints 5A/5B; adversarially reviewed in 5C.

## Problem

The same user can have duplicates, out-of-order events, and a process crash between reading history and inserting the current transaction. Feature state must be bounded and recoverable from PostgreSQL.

## Naive solution and why it fails

Separate `ZRANGEBYSCORE`, feature calculation, and `ZADD` calls can interleave or crash halfway. Plain counters double-increment on replay and cannot answer a late event's own event-time window. Processing-time TTL alone can discard history required by a within-tolerance late event.

## Chosen representation

For user `u` and feature version `features-v1`:

| Key | Value | Expiry |
|---|---|---|
| `fraud:features-v1:u:history` | Sorted set: transaction ID scored by integer UTC microseconds | 26h inactivity TTL; members older than watermark minus 24h 2m pruned |
| `...:entries` | Hash: transaction ID to immutable event ID, digest, amount, device, country, timestamp | Same |
| `...:event-ids` | Hash: event ID to transaction ID for replay/conflict checks | Same |
| `...:meta` | Hash: completed/observed watermark and cache generation | Same |
| `...:snapshot:event_id` | Frozen pre-insert feature vector and generation for retry | 26h TTL |

All initial features use the same event-time history and hash. Count windows filter sorted-set scores; 24h average sums prior amounts; device/country diversity uses sets assembled from prior records inside Lua; previous transaction uses the maximum strictly earlier timestamp. The current event is excluded by an exclusive upper score bound. The exact lower boundary is included. Same timestamp is excluded. The result and current event insertion are one atomic Lua operation. Exact duplicate returns the saved snapshot; conflicting digest or ID returns an error. A too-late event returns `LATE_EVENT` before state mutation.

## Crash, expiry, and memory

If the process dies after the Lua operation but before DB commit, the snapshot key supports deterministic retry. If Redis loses keys, the caller must rebuild from completed, feature-included PostgreSQL events under a per-user lock. `restore_completed` replaces one user's core cache keys atomically and changes generation; a saved snapshot from an older generation cannot be reused. The DB loader/lock is checkpoint 8 work, so direct use of the engine without that orchestration cannot tell a genuinely new user from a lost cache.

Per-user history is capped at 20,000 events, and Redis has a 192 MB local `noeviction` limit. Actual bytes per event depend on Redis encoding and snapshot size. A rough planning range of hundreds of bytes per event implies several MB for a hot 20,000-event user; measure `MEMORY USAGE` and burst latency before claiming a capacity number. On limit or memory error, processing must pause or follow a documented fallback. It must not silently drop history.

## Tests and limits

The parity tests cover duplicates, out-of-order events, lateness, window boundaries, same timestamp, TTL expiry, cache loss/rebuild, identity conflicts, a 200-event burst, and the history cap. Fakeredis executes Lua for deterministic unit tests; a separate live Redis test verifies the script against the container. Whole-user scans are O(number of prior 24h events) and remain a performance risk.
