"""Redis StateStore with atomic Lua CAS and monotonically fenced job leases."""
from __future__ import annotations

import time
from typing import Any

from synapse.adapters.state.base import StateStore, decode, encode

_CAS_LUA = r"""
local current = redis.call('GET', KEYS[1])
local expected_revision = ARGV[1]
local expected_etag = ARGV[2]
if expected_revision == 'ABSENT' then
  if current then return 0 end
  local replacement = cjson.decode(ARGV[3])
  if tonumber(replacement['revision'] or 1) ~= 1 then return 0 end
else
  if not current then return 0 end
  local value = cjson.decode(current)
  if tonumber(value['revision'] or -1) ~= tonumber(expected_revision) then return 0 end
  if expected_etag ~= '' and value['etag'] ~= expected_etag then return 0 end
  local replacement = cjson.decode(ARGV[3])
  if tonumber(replacement['revision'] or -1) ~= tonumber(expected_revision) + 1 then return 0 end
end
redis.call('SET', KEYS[1], ARGV[3])
return 1
"""

_ACQUIRE_LUA = r"""
if redis.call('EXISTS', KEYS[1]) == 1 then return nil end
local fence = redis.call('INCR', KEYS[2])
local expires = tonumber(ARGV[1]) / 1000 + tonumber(ARGV[2])
local lease = cjson.encode({job_id=ARGV[3], worker_id=ARGV[4], fence=fence, expires_at=expires})
redis.call('PSETEX', KEYS[1], ARGV[1], lease)
return {tostring(fence), lease}
"""

_HEARTBEAT_LUA = r"""
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local lease = cjson.decode(raw)
if lease['worker_id'] ~= ARGV[1] or tonumber(lease['fence']) ~= tonumber(ARGV[2]) then return 0 end
local now = tonumber(ARGV[3])
if tonumber(lease['expires_at']) <= now then return 0 end
lease['expires_at'] = now + tonumber(ARGV[4])
redis.call('PSETEX', KEYS[1], ARGV[4] * 1000, cjson.encode(lease))
return 1
"""

_RELEASE_LUA = r"""
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local lease = cjson.decode(raw)
if lease['worker_id'] ~= ARGV[1] or tonumber(lease['fence']) ~= tonumber(ARGV[2]) then return 0 end
return redis.call('DEL', KEYS[1])
"""


class RedisStateStore(StateStore):
    def __init__(self, url: str | None = None, *, client: Any | None = None, prefix: str = "synapse:"):
        if client is None:
            try:
                import redis
            except ImportError as exc:
                raise RuntimeError("redis-py is required for RedisStateStore") from exc
            if not url:
                raise ValueError("REDIS_URL is required")
            client = redis.Redis.from_url(url, decode_responses=True, health_check_interval=30, socket_timeout=5, socket_connect_timeout=5)
        self.client = client
        self.prefix = prefix

    def _key(self, key: str) -> str:
        return self.prefix + key

    def get(self, key: str) -> Any | None:
        value = self.client.get(self._key(key))
        return decode(value)

    def set(self, key: str, value: Any, ttl_seconds: int | None = None) -> None:
        serialized = encode(value)
        if ttl_seconds is None:
            self.client.set(self._key(key), serialized)
        else:
            if ttl_seconds < 1:
                self.delete(key)
            else:
                self.client.set(self._key(key), serialized, ex=int(ttl_seconds))

    def delete(self, key: str) -> bool:
        return bool(self.client.delete(self._key(key)))

    def list_prefix(self, prefix: str) -> list[tuple[str, Any]]:
        result: list[tuple[str, Any]] = []
        pattern = self._key(prefix) + "*"
        for raw_key in self.client.scan_iter(match=pattern, count=500):
            suffix = raw_key[len(self.prefix):]
            value = self.client.get(raw_key)
            if value is not None:
                result.append((suffix, decode(value)))
        return sorted(result, key=lambda pair: pair[0])

    def compare_and_set(self, key: str, expected_revision: int | None, expected_etag: str | None, value: Any) -> bool:
        expected = "ABSENT" if expected_revision is None else str(expected_revision)
        return bool(self.client.eval(_CAS_LUA, 1, self._key(key), expected, expected_etag or "", encode(value)))

    def acquire_lease(self, job_id: str, worker_id: str, ttl_seconds: int) -> dict[str, Any] | None:
        if ttl_seconds < 3:
            raise ValueError("lease TTL must be at least three seconds")
        keys = [self._key(f"lease:job:{job_id}"), self._key(f"lease:fence:{job_id}")]
        result = self.client.eval(_ACQUIRE_LUA, 2, *keys, int(ttl_seconds * 1000), time.time(), job_id, worker_id)
        if not result:
            return None
        return decode(result[1])

    def heartbeat_lease(self, job_id: str, worker_id: str, fence: int, ttl_seconds: int) -> bool:
        return bool(self.client.eval(_HEARTBEAT_LUA, 1, self._key(f"lease:job:{job_id}"), worker_id, fence, time.time(), ttl_seconds))

    def release_lease(self, job_id: str, worker_id: str, fence: int) -> bool:
        return bool(self.client.eval(_RELEASE_LUA, 1, self._key(f"lease:job:{job_id}"), worker_id, fence))

    def health(self) -> bool:
        return self.client.ping() is True

    def close(self) -> None:
        close = getattr(self.client, "close", None)
        if close:
            close()
