# Phase 05B — online Redis feature engine

`RedisFeatureEngine.process` sends one immutable event representation and the five per-user keys to a Lua script. The script checks conflicts and duplicates, checks lateness against the per-user watermark, prunes expired history, selects prior records in event-time windows, calculates the ten v1 outputs, saves the pre-event snapshot, and inserts the event. No normal transaction produces multiple Redis commands from Python that could leave partial feature state. `restore_completed` atomically replaces core user state from completed events and changes the generation. The caller must supply those events from the durable ledger; this linkage arrives in checkpoint 8.

The key inclusion expression is:

```lua
local members = redis.call(
  'ZRANGEBYSCORE', hist, string.format('%.0f', t - 86400000000), '(' .. time_text
)
```

The lower bound is inclusive. The `(` upper bound excludes the current event and any equal-timestamp prior arrival. Exact decimal strings retain microsecond precision. Other windows filter members at five minutes and one hour. The script checks identity before lateness so a duplicate of an older event can return its frozen snapshot while the key still exists.

Deterministic parity tests compare the Lua result with `OfflineFeatureEngine` on ordered, duplicate, in-bound out-of-order, late, first-event, equal-time, boundary, and burst sequences. A live Redis test confirms the script runs on Redis 7. A 1-second test TTL demonstrates expiration and explicit rebuild from a supplied completed-event list. **Guarantee boundary:** direct use without a PostgreSQL loader cannot distinguish a cold user from expired Redis state; the full recovery protocol must be implemented in checkpoint 8.
