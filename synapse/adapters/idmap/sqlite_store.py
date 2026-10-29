"""SQLite WAL ID-map with WITHOUT ROWID storage and a strict bounded LRU."""
from __future__ import annotations

import os
import sqlite3
import threading
from collections import OrderedDict
from pathlib import Path
from typing import Any, Iterable, Iterator

from synapse.adapters.idmap.base import IdMapStore, canonical_key_bytes


class SqliteIdMapStore(IdMapStore):
    def __init__(self, path: str | Path, *, cache_entries: int = 100_000, synchronous: str = "NORMAL"):
        self.path = Path(path).expanduser().resolve()
        if str(path) == ":memory:":
            raise ValueError("ID maps must be on disk so they survive a worker restart")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if cache_entries < 0:
            raise ValueError("cache_entries cannot be negative")
        if synchronous not in {"NORMAL", "FULL", "EXTRA"}:
            raise ValueError("SQLite synchronous must be NORMAL, FULL, or EXTRA")
        self.cache_entries = int(cache_entries)
        self._cache: OrderedDict[tuple[str, bytes], bytes] = OrderedDict()
        self._lock = threading.RLock()
        self.cache_hits = 0
        self.cache_misses = 0
        self.cache_evictions = 0
        self.connection = sqlite3.connect(str(self.path), timeout=30, isolation_level=None, check_same_thread=False)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute(f"PRAGMA synchronous={synchronous}")
        self.connection.execute("PRAGMA busy_timeout=30000")
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.execute("""
            CREATE TABLE IF NOT EXISTS idmap (
                ns TEXT NOT NULL,
                key BLOB NOT NULL,
                val BLOB NOT NULL,
                materialized INTEGER NOT NULL DEFAULT 0 CHECK (materialized IN (0,1)),
                PRIMARY KEY (ns, key)
            ) WITHOUT ROWID
        """)
        mode = self.connection.execute("PRAGMA journal_mode").fetchone()[0]
        if str(mode).lower() != "wal":
            raise RuntimeError("SQLite ID map could not enable WAL mode")

    def _cache_get(self, cache_key: tuple[str, bytes]) -> bytes | None:
        if self.cache_entries == 0:
            return None
        value = self._cache.get(cache_key)
        if value is not None:
            self._cache.move_to_end(cache_key)
            self.cache_hits += 1
        else:
            self.cache_misses += 1
        return value

    def _cache_put(self, cache_key: tuple[str, bytes], value: bytes) -> None:
        if self.cache_entries == 0:
            return
        self._cache[cache_key] = value
        self._cache.move_to_end(cache_key)
        while len(self._cache) > self.cache_entries:
            self._cache.popitem(last=False)
            self.cache_evictions += 1

    @staticmethod
    def _bytes(value: Any) -> bytes:
        if hasattr(value, "binary"):
            value = value.binary
        if isinstance(value, memoryview):
            value = value.tobytes()
        if isinstance(value, bytearray):
            value = bytes(value)
        if not isinstance(value, bytes):
            raise TypeError("ID-map values must be bytes (ObjectIds use their 12-byte binary form)")
        if not value:
            raise ValueError("ID-map values cannot be empty")
        return value

    def get(self, ns: str, key: Any) -> bytes | None:
        if not ns:
            raise ValueError("ID-map namespace must be non-empty")
        encoded = canonical_key_bytes(key)
        cache_key = (ns, encoded)
        with self._lock:
            cached = self._cache_get(cache_key)
            if cached is not None:
                return cached
            row = self.connection.execute("SELECT val FROM idmap WHERE ns=? AND key=?", (ns, sqlite3.Binary(encoded))).fetchone()
            if row is None:
                return None
            value = bytes(row[0])
            self._cache_put(cache_key, value)
            return value

    def get_many(self, ns: str, keys: Iterable[Any]) -> list[bytes | None]:
        encoded_keys = [canonical_key_bytes(key) for key in keys]
        result: list[bytes | None] = [None] * len(encoded_keys)
        missing: list[tuple[int, bytes]] = []
        with self._lock:
            for index, encoded in enumerate(encoded_keys):
                cache_key = (ns, encoded)
                cached = self._cache_get(cache_key)
                if cached is not None:
                    result[index] = cached
                else:
                    missing.append((index, encoded))
            for offset in range(0, len(missing), 500):
                chunk = missing[offset:offset + 500]
                placeholders = ",".join("?" for _ in chunk)
                query = f"SELECT key,val FROM idmap WHERE ns=? AND key IN ({placeholders})"
                rows = self.connection.execute(query, [ns, *(sqlite3.Binary(key) for _, key in chunk)]).fetchall()
                found = {bytes(row[0]): bytes(row[1]) for row in rows}
                for index, encoded in chunk:
                    value = found.get(encoded)
                    if value is not None:
                        result[index] = value
                        self._cache_put((ns, encoded), value)
        return result

    def get_or_allocate(self, ns: str, key: Any, allocator: Any | None = None) -> bytes:
        if not ns:
            raise ValueError("ID-map namespace must be non-empty")
        encoded = canonical_key_bytes(key)
        cache_key = (ns, encoded)
        with self._lock:
            cached = self._cache_get(cache_key)
            if cached is not None:
                return cached
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                row = self.connection.execute("SELECT val FROM idmap WHERE ns=? AND key=?", (ns, sqlite3.Binary(encoded))).fetchone()
                if row is None:
                    if allocator is None:
                        candidate = os.urandom(12)
                    else:
                        candidate = allocator() if callable(allocator) else allocator
                    value = self._bytes(candidate)
                    self.connection.execute("INSERT OR IGNORE INTO idmap(ns,key,val,materialized) VALUES(?,?,?,0)",
                                            (ns, sqlite3.Binary(encoded), sqlite3.Binary(value)))
                    row = self.connection.execute("SELECT val FROM idmap WHERE ns=? AND key=?", (ns, sqlite3.Binary(encoded))).fetchone()
                    if row is None:
                        raise RuntimeError("ID-map allocation was not persisted")
                    value = bytes(row[0])
                else:
                    value = bytes(row[0])
                self.connection.execute("COMMIT")
                self._cache_put(cache_key, value)
                return value
            except BaseException:
                self.connection.execute("ROLLBACK")
                raise

    def put(self, ns: str, key: Any, value: bytes, *, materialized: bool = False) -> None:
        self.put_many([(ns, key, value, materialized)])

    def put_many(self, entries: Iterable[tuple[str, Any, bytes, bool]]) -> None:
        batch = [(ns, canonical_key_bytes(key), self._bytes(value), int(materialized)) for ns, key, value, materialized in entries]
        if not batch:
            return
        with self._lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                self.connection.executemany(
                    "INSERT INTO idmap(ns,key,val,materialized) VALUES(?,?,?,?) "
                    "ON CONFLICT(ns,key) DO UPDATE SET val=excluded.val, materialized=MAX(idmap.materialized,excluded.materialized)",
                    [(ns, sqlite3.Binary(key), sqlite3.Binary(value), materialized) for ns, key, value, materialized in batch],
                )
                self.connection.execute("COMMIT")
                for ns, key, value, _ in batch:
                    self._cache_put((ns, key), value)
            except BaseException:
                self.connection.execute("ROLLBACK")
                raise

    def materialize(self, ns: str, key: Any) -> bool:
        encoded = canonical_key_bytes(key)
        with self._lock:
            cursor = self.connection.execute("UPDATE idmap SET materialized=1 WHERE ns=? AND key=?", (ns, sqlite3.Binary(encoded)))
            return cursor.rowcount > 0

    def scan_unmaterialized(self, ns: str, *, limit: int = 1000) -> Iterator[tuple[bytes, bytes]]:
        if limit < 1:
            raise ValueError("limit must be positive")
        with self._lock:
            cursor = self.connection.execute("SELECT key,val FROM idmap WHERE ns=? AND materialized=0 ORDER BY key", (ns,))
            while True:
                rows = cursor.fetchmany(min(limit, 1000))
                if not rows:
                    break
                chunk = [(bytes(row[0]), bytes(row[1])) for row in rows]
                for item in chunk:
                    yield item

    def count(self, ns: str | None = None) -> int:
        with self._lock:
            if ns is None:
                return int(self.connection.execute("SELECT COUNT(*) FROM idmap").fetchone()[0])
            return int(self.connection.execute("SELECT COUNT(*) FROM idmap WHERE ns=?", (ns,)).fetchone()[0])

    def close(self) -> None:
        with self._lock:
            self.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            self.connection.close()
            self._cache.clear()
