"""Durable state-store protocol and resilient Redis→atomic-file fallback."""
from __future__ import annotations

import json
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any


def json_default(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {"$binary": bytes(value).hex()}
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if hasattr(value, "__str__"):
        return str(value)
    raise TypeError(f"cannot encode {type(value).__name__}")


def encode(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=json_default, ensure_ascii=False)


def decode(value: str | bytes | None) -> Any:
    if value is None:
        return None
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    return json.loads(value)


class StateStore(ABC):
    """Persistence interface. API workers keep no authoritative state in process memory."""
    @abstractmethod
    def get(self, key: str) -> Any | None: ...

    @abstractmethod
    def set(self, key: str, value: Any, ttl_seconds: int | None = None) -> None: ...

    @abstractmethod
    def delete(self, key: str) -> bool: ...

    @abstractmethod
    def list_prefix(self, prefix: str) -> list[tuple[str, Any]]: ...

    @abstractmethod
    def compare_and_set(self, key: str, expected_revision: int | None, expected_etag: str | None, value: Any) -> bool: ...

    @abstractmethod
    def acquire_lease(self, job_id: str, worker_id: str, ttl_seconds: int) -> dict[str, Any] | None: ...

    @abstractmethod
    def heartbeat_lease(self, job_id: str, worker_id: str, fence: int, ttl_seconds: int) -> bool: ...

    @abstractmethod
    def release_lease(self, job_id: str, worker_id: str, fence: int) -> bool: ...

    def health(self) -> bool:
        return True

    def close(self) -> None:
        return None


class ResilientStateStore(StateStore):
    """Use Redis as primary and a durable file store for single-node/failure fallback."""
    def __init__(self, primary: StateStore, fallback: StateStore):
        self.primary = primary
        self.fallback = fallback

    def get(self, key: str) -> Any | None:
        try:
            value = self.primary.get(key)
            if value is not None:
                return value
        except Exception:
            pass
        return self.fallback.get(key)

    def set(self, key: str, value: Any, ttl_seconds: int | None = None) -> None:
        try:
            self.primary.set(key, value, ttl_seconds)
        except Exception:
            self.fallback.set(key, value, ttl_seconds)
            return
        # The file copy is a durable recovery snapshot; its TTL is applied by FileStateStore.
        self.fallback.set(key, value, ttl_seconds)

    def delete(self, key: str) -> bool:
        deleted = False
        try:
            deleted = self.primary.delete(key) or deleted
        except Exception:
            pass
        return self.fallback.delete(key) or deleted

    def list_prefix(self, prefix: str) -> list[tuple[str, Any]]:
        items: dict[str, Any] = {}
        try:
            items.update(dict(self.fallback.list_prefix(prefix)))
        except Exception:
            pass
        try:
            items.update(dict(self.primary.list_prefix(prefix)))
        except Exception:
            pass
        return sorted(items.items())

    def compare_and_set(self, key: str, expected_revision: int | None, expected_etag: str | None, value: Any) -> bool:
        try:
            if self.primary.compare_and_set(key, expected_revision, expected_etag, value):
                self.fallback.set(key, value)
                return True
        except Exception:
            pass
        return self.fallback.compare_and_set(key, expected_revision, expected_etag, value)

    def acquire_lease(self, job_id: str, worker_id: str, ttl_seconds: int) -> dict[str, Any] | None:
        try:
            lease = self.primary.acquire_lease(job_id, worker_id, ttl_seconds)
            if lease is not None:
                self.fallback.set(f"lease:job:{job_id}", lease, ttl_seconds)
                return lease
        except Exception:
            pass
        return self.fallback.acquire_lease(job_id, worker_id, ttl_seconds)

    def heartbeat_lease(self, job_id: str, worker_id: str, fence: int, ttl_seconds: int) -> bool:
        try:
            if self.primary.heartbeat_lease(job_id, worker_id, fence, ttl_seconds):
                self.fallback.set(f"lease:job:{job_id}", {"job_id": job_id, "worker_id": worker_id, "fence": fence,
                                                            "expires_at": datetime.now(timezone.utc).timestamp() + ttl_seconds}, ttl_seconds)
                return True
        except Exception:
            pass
        return self.fallback.heartbeat_lease(job_id, worker_id, fence, ttl_seconds)

    def release_lease(self, job_id: str, worker_id: str, fence: int) -> bool:
        released = False
        try:
            released = self.primary.release_lease(job_id, worker_id, fence)
        except Exception:
            pass
        return self.fallback.release_lease(job_id, worker_id, fence) or released

    def health(self) -> bool:
        try:
            return bool(self.primary.health()) or bool(self.fallback.health())
        except Exception:
            return bool(self.fallback.health())

    def close(self) -> None:
        self.primary.close()
        self.fallback.close()
