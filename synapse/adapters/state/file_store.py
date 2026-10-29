"""Process-safe file state store using fsync + atomic rename and advisory locking."""
from __future__ import annotations

import hashlib
import os
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from synapse.adapters.file_lock import exclusive_file_lock
from synapse.adapters.state.base import StateStore, decode, encode


class FileStateStore(StateStore):
    def __init__(self, root: str | Path):
        self.root = Path(root).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.lock_path = self.root / ".store.lock"
        self.lock_path.touch(exist_ok=True)

    def _path(self, key: str) -> Path:
        return self.root / (hashlib.sha256(key.encode("utf-8")).hexdigest() + ".json")

    @contextmanager
    def _locked(self) -> Iterator[None]:
        """Serialize state access using an OS-appropriate cross-process file lock."""
        with self.lock_path.open("a+b") as handle, exclusive_file_lock(handle):
            yield

    def _read_unlocked(self, key: str) -> Any | None:
        path = self._path(key)
        try:
            document = decode(path.read_bytes())
        except FileNotFoundError:
            return None
        if not isinstance(document, dict) or document.get("_state_key") != key:
            return None
        expiry = document.get("_expires_at")
        if expiry is not None and float(expiry) <= time.time():
            path.unlink(missing_ok=True)
            return None
        return document.get("value")

    def get(self, key: str) -> Any | None:
        with self._locked():
            return self._read_unlocked(key)

    def _write_unlocked(self, key: str, value: Any, ttl_seconds: int | None = None) -> None:
        path = self._path(key)
        envelope = {"_state_key": key, "_expires_at": time.time() + ttl_seconds if ttl_seconds is not None else None, "value": value}
        data = encode(envelope).encode("utf-8")
        fd, temp_name = tempfile.mkstemp(prefix=".state-", suffix=".tmp", dir=self.root)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_name, path)
            # Directory handles cannot be fsynced on Windows; the data file itself
            # has already been flushed, and os.replace remains atomic on the same volume.
            if os.name != "nt":
                directory_fd = os.open(self.root, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
        finally:
            Path(temp_name).unlink(missing_ok=True)

    def set(self, key: str, value: Any, ttl_seconds: int | None = None) -> None:
        if ttl_seconds is not None and ttl_seconds < 1:
            self.delete(key)
            return
        with self._locked():
            self._write_unlocked(key, value, ttl_seconds)

    def delete(self, key: str) -> bool:
        with self._locked():
            path = self._path(key)
            existed = path.exists()
            path.unlink(missing_ok=True)
            return existed

    def list_prefix(self, prefix: str) -> list[tuple[str, Any]]:
        items: list[tuple[str, Any]] = []
        with self._locked():
            for path in self.root.glob("*.json"):
                try:
                    document = decode(path.read_bytes())
                except (OSError, ValueError):
                    continue
                key = document.get("_state_key") if isinstance(document, dict) else None
                if not isinstance(key, str) or not key.startswith(prefix):
                    continue
                expiry = document.get("_expires_at")
                if expiry is not None and float(expiry) <= time.time():
                    path.unlink(missing_ok=True)
                    continue
                items.append((key, document.get("value")))
        return sorted(items, key=lambda pair: pair[0])

    def compare_and_set(self, key: str, expected_revision: int | None, expected_etag: str | None, value: Any) -> bool:
        with self._locked():
            current = self._read_unlocked(key)
            if expected_revision is None:
                if current is not None:
                    return False
                if isinstance(value, dict) and int(value.get("revision", 1)) != 1:
                    return False
            else:
                if not isinstance(current, dict):
                    return False
                if int(current.get("revision", -1)) != expected_revision:
                    return False
                if expected_etag is not None and current.get("etag") != expected_etag:
                    return False
                if isinstance(value, dict) and int(value.get("revision", -1)) != expected_revision + 1:
                    return False
            self._write_unlocked(key, value)
            return True

    def acquire_lease(self, job_id: str, worker_id: str, ttl_seconds: int) -> dict[str, Any] | None:
        if ttl_seconds < 3:
            raise ValueError("lease TTL must be at least three seconds")
        now = time.time()
        with self._locked():
            key = f"lease:job:{job_id}"
            current = self._read_unlocked(key)
            if current and float(current.get("expires_at", 0)) > now:
                return None
            counter_key = f"lease:fence:{job_id}"
            counter = self._read_unlocked(counter_key)
            fence = int(counter or 0) + 1
            self._write_unlocked(counter_key, fence)
            lease = {"job_id": job_id, "worker_id": worker_id, "fence": fence, "expires_at": now + ttl_seconds}
            self._write_unlocked(key, lease, ttl_seconds)
            return lease

    def heartbeat_lease(self, job_id: str, worker_id: str, fence: int, ttl_seconds: int) -> bool:
        now = time.time()
        with self._locked():
            key = f"lease:job:{job_id}"
            lease = self._read_unlocked(key)
            if not lease or lease.get("worker_id") != worker_id or int(lease.get("fence", -1)) != fence or float(lease.get("expires_at", 0)) <= now:
                return False
            lease["expires_at"] = now + ttl_seconds
            self._write_unlocked(key, lease, ttl_seconds)
            return True

    def release_lease(self, job_id: str, worker_id: str, fence: int) -> bool:
        with self._locked():
            key = f"lease:job:{job_id}"
            lease = self._read_unlocked(key)
            if not lease or lease.get("worker_id") != worker_id or int(lease.get("fence", -1)) != fence:
                return False
            self._path(key).unlink(missing_ok=True)
            return True

    def health(self) -> bool:
        try:
            test = self.root / ".health"
            with self._locked():
                test.write_text("ok", encoding="utf-8")
                test.unlink(missing_ok=True)
            return True
        except OSError:
            return False
