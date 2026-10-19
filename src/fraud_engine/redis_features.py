"""Atomic Redis implementation of features-v2, with explicit rebuild support."""

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

from redis import Redis

from fraud_engine.events import TransactionEvent
from fraud_engine.features import FEATURE_VERSION, FeatureOutcome, IdentityConflict

STATE_TTL_SECONDS = 26 * 3600
MAX_HISTORY = 20_000
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def epoch_microseconds(timestamp: datetime) -> int:
    delta = timestamp - EPOCH
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


FEATURE_SCRIPT = r"""
local hist, entries, ids, meta, snapshot_key = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
local event = cjson.decode(ARGV[1])
local t = tonumber(event.time_us)
local time_text = ARGV[4]
local watermark_text = redis.call('HGET', meta, 'watermark')
local watermark = tonumber(watermark_text or '')
local generation = redis.call('HGET', meta, 'generation') or 'first'
local existing_id = redis.call('HGET', ids, event.event_id)
if existing_id and existing_id ~= event.transaction_id then
  return cjson.encode({status='IDENTITY_CONFLICT'})
end
local existing = redis.call('HGET', entries, event.transaction_id)
if existing then
  local old = cjson.decode(existing)
  if old.digest ~= event.digest or old.event_id ~= event.event_id then
    return cjson.encode({status='IDENTITY_CONFLICT'})
  end
  local saved = redis.call('GET', snapshot_key)
  if not saved then return cjson.encode({status='RECOVERY_REQUIRED'}) end
  local result = cjson.decode(saved)
  if result.generation ~= generation then
    return cjson.encode({status='RECOVERY_REQUIRED'})
  end
  result.duplicate = true
  return cjson.encode(result)
end
if watermark and t < watermark - 120000000 then
  return cjson.encode({status='LATE_EVENT'})
end
local new_watermark = watermark and math.max(watermark, t) or t
local new_watermark_text = (not watermark or t > watermark) and time_text or watermark_text
local expired = redis.call(
  'ZRANGEBYSCORE', hist, '-inf', '(' .. string.format('%.0f', new_watermark - 86520000000)
)
for _, transaction_id in ipairs(expired) do
  local old = cjson.decode(redis.call('HGET', entries, transaction_id))
  redis.call('HDEL', ids, old.event_id)
  redis.call('HDEL', entries, transaction_id)
  redis.call('ZREM', hist, transaction_id)
end
if redis.call('ZCARD', hist) >= tonumber(ARGV[2]) then
  return cjson.encode({status='HISTORY_LIMIT'})
end
local members = redis.call(
  'ZRANGEBYSCORE', hist, string.format('%.0f', t - 86400000000), '(' .. time_text
)
local count5, count1, sum, sumsq, hour_sum, previous = 0, 0, 0, 0, 0, nil
local devices, countries, merchants, device_first, recent = {}, {}, {}, {}, {}
local unique_devices, unique_countries, unique_merchants = 0, 0, 0
for _, transaction_id in ipairs(members) do
  local item = cjson.decode(redis.call('HGET', entries, transaction_id))
  if item.time_us >= t - 300000000 then
    count5 = count5 + 1
    table.insert(recent, item.amount)
  end
  if item.time_us >= t - 3600000000 then count1 = count1 + 1 end
  sum = sum + item.amount
  sumsq = sumsq + item.amount * item.amount
  hour_sum = hour_sum + item.event_hour
  if not previous or item.time_us > previous then previous = item.time_us end
  if not devices[item.device_id] then
    devices[item.device_id] = true
    device_first[item.device_id] = item.time_us
    unique_devices = unique_devices + 1
  elseif item.time_us < device_first[item.device_id] then
    device_first[item.device_id] = item.time_us
  end
  if not countries[item.country] then
    countries[item.country] = true
    unique_countries = unique_countries + 1
  end
  if not merchants[item.merchant_category] then
    merchants[item.merchant_category] = true
    unique_merchants = unique_merchants + 1
  end
end
local n = #members
local average = n > 0 and sum / n or 0
local variance = n > 0 and math.max(0, sumsq / n - average * average) or 0
local standard_deviation = math.sqrt(variance)
local small_count = 0
for _, amount in ipairs(recent) do
  if n > 0 and amount <= average * 0.25 then small_count = small_count + 1 end
end
local mean_hour = n > 0 and hour_sum / n or 0
local hour_difference = n > 0 and math.abs(event.event_hour - mean_hour) or 0
local hour_deviation = n > 0 and math.min(hour_difference, 24 - hour_difference) or 0
local snapshot = {
  transaction_count_5m=count5, transaction_count_1h=count1,
  average_amount_24h=average, has_amount_history_24h=n > 0 and 1 or 0,
  amount_vs_user_average_24h=n > 0 and event.amount / average or 0,
  amount_zscore_24h=standard_deviation > 0
    and math.max(-20, math.min(20, (event.amount - average) / standard_deviation)) or 0,
  small_transaction_count_5m=small_count,
  unique_devices_24h=unique_devices,
  is_new_device_24h=devices[event.device_id] and 0 or 1,
  device_age_seconds_24h=device_first[event.device_id]
    and (t - device_first[event.device_id]) / 1000000 or 0,
  unique_countries_24h=unique_countries,
  unique_merchants_24h=unique_merchants,
  seconds_since_previous_transaction=previous and (t - previous) / 1000000 or -1,
  has_previous_transaction=previous and 1 or 0,
  hour_of_day_deviation_24h=hour_deviation
}
redis.call('ZADD', hist, time_text, event.transaction_id)
redis.call('HSET', entries, event.transaction_id, ARGV[1])
redis.call('HSET', ids, event.event_id, event.transaction_id)
redis.call('HSET', meta, 'watermark', new_watermark_text, 'generation', generation)
local result = {status='SCORED', snapshot=snapshot, generation=generation, duplicate=false}
redis.call('SET', snapshot_key, cjson.encode(result), 'EX', tonumber(ARGV[3]))
for _, key in ipairs({hist, entries, ids, meta}) do
  redis.call('EXPIRE', key, tonumber(ARGV[3]))
end
return cjson.encode(result)
"""


class StateRecoveryRequired(RuntimeError):
    pass


class HistoryLimitExceeded(RuntimeError):
    pass


@dataclass
class RedisFeatureEngine:
    redis: Redis
    max_history: int = MAX_HISTORY

    @staticmethod
    def _keys(user_id: str, event_id: str) -> list[str]:
        base = f"fraud:{FEATURE_VERSION}:{user_id}"
        return [
            f"{base}:history",
            f"{base}:entries",
            f"{base}:event-ids",
            f"{base}:meta",
            f"{base}:snapshot:{event_id}",
        ]

    def process(self, event: TransactionEvent) -> FeatureOutcome:
        payload = {
            "event_id": event.event_id,
            "transaction_id": event.transaction_id,
            "digest": event.digest(),
            "time_us": epoch_microseconds(event.event_timestamp),
            "amount": event.amount,
            "device_id": event.device_id,
            "country": event.country,
            "merchant_category": event.merchant_category,
            "event_hour": event.event_timestamp.hour,
        }
        raw = self.redis.eval(
            FEATURE_SCRIPT,
            5,
            *self._keys(event.user_id, event.event_id),
            json.dumps(payload, separators=(",", ":")),
            self.max_history,
            STATE_TTL_SECONDS,
            str(payload["time_us"]),
        )
        result = json.loads(raw)
        if result["status"] == "IDENTITY_CONFLICT":
            raise IdentityConflict("Redis identity conflict")
        if result["status"] == "RECOVERY_REQUIRED":
            raise StateRecoveryRequired("saved snapshot missing or stale; rebuild from PostgreSQL")
        if result["status"] == "HISTORY_LIMIT":
            raise HistoryLimitExceeded("per-user history limit reached")
        return FeatureOutcome(
            result["status"], result.get("snapshot"), result.get("duplicate", False)
        )

    def is_initialized(self, user_id: str) -> bool:
        """A missing marker means the disposable cache must be rebuilt from PostgreSQL."""
        return bool(self.redis.exists(self._keys(user_id, "unused")[3]))

    def restore_completed(self, user_id: str, events: list[TransactionEvent]) -> None:
        """Replace one user cache atomically under the external per-user DB lock."""
        keys = self._keys(user_id, "unused")
        history, entries, ids, meta = keys[:4]
        ordered = sorted(events, key=lambda event: event.event_timestamp)
        watermark = max(
            (epoch_microseconds(event.event_timestamp) for event in ordered), default=None
        )
        cutoff = watermark - 86_520_000_000 if watermark is not None else None
        with self.redis.pipeline(transaction=True) as pipe:
            pipe.delete(history, entries, ids, meta)
            for event in ordered:
                time_us = epoch_microseconds(event.event_timestamp)
                if cutoff is not None and time_us < cutoff:
                    continue
                payload = {
                    "event_id": event.event_id,
                    "transaction_id": event.transaction_id,
                    "digest": event.digest(),
                    "time_us": time_us,
                    "amount": event.amount,
                    "device_id": event.device_id,
                    "country": event.country,
                    "merchant_category": event.merchant_category,
                    "event_hour": event.event_timestamp.hour,
                }
                pipe.zadd(history, {event.transaction_id: time_us})
                pipe.hset(entries, event.transaction_id, json.dumps(payload))
                pipe.hset(ids, event.event_id, event.transaction_id)
            pipe.hset(
                meta, mapping={"generation": str(uuid.uuid4()), "watermark": int(watermark or 0)}
            )
            for key in (history, entries, ids, meta):
                pipe.expire(key, STATE_TTL_SECONDS)
            pipe.execute()
