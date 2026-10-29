"""Append-only JSON Lines dead-letter log, SQLite browsing index, and recursive bisection."""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator

from synapse.adapters.file_lock import exclusive_file_lock
from synapse.adapters.state.base import encode

ERROR_CODES = frozenset({
    "BSON_TOO_LARGE", "VALIDATION_FAILED", "DUPLICATE_KEY", "TYPE_CONVERSION", "DRIFT_VALUE_TOO_LONG",
    "PARENT_NOT_LOADED", "ORPHAN_REFERENCE", "SQL_CONSTRAINT", "TRANSIENT_EXHAUSTED", "UNKNOWN",
})
_NON_REPLAYABLE = {"BSON_TOO_LARGE", "PARENT_NOT_LOADED"}


def error_code_for(exc: BaseException) -> str:
    text = str(exc).lower()
    code = getattr(exc, "code", None)
    if code in {121}: return "VALIDATION_FAILED"
    if code in {11000, 1062}: return "DUPLICATE_KEY"
    if code in {10334, 17419} or "too large" in text or "bsonsize" in text: return "BSON_TOO_LARGE"
    if code in {1406, 1264, 22001} or "data too long" in text: return "DRIFT_VALUE_TOO_LONG"
    if code in {1452, 3819, 1048, 1265, 1366}: return "SQL_CONSTRAINT"
    if "orphan" in text or "reference not found" in text: return "ORPHAN_REFERENCE"
    if "parent_not_loaded" in text: return "PARENT_NOT_LOADED"
    if isinstance(exc, (ValueError, TypeError, OverflowError)): return "TYPE_CONVERSION"
    if code in {91, 189, 11602} or any(word in text for word in ("timeout", "not primary", "network")):
        return "TRANSIENT_EXHAUSTED"
    return "UNKNOWN"


class DeadLetterLog:
    def __init__(self, path: str | Path, job_id: str, index_db: str | Path, *, redact_payload: bool = False, fsync_every: int = 100):
        self.path = Path(path).expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.job_id = job_id
        self.redact_payload = redact_payload
        self.fsync_every = max(1, int(fsync_every))
        self._lock = threading.RLock()
        self._pending_fsync = 0
        self._handle = self.path.open("a+b")
        # Lock a sidecar, not the JSONL file: Windows byte-range locks require a
        # real byte at offset zero, which would otherwise corrupt an empty log.
        self.lock_path = self.path.with_name(self.path.name + ".lock")
        self.lock_path.touch(exist_ok=True)
        self.db = sqlite3.connect(str(index_db), timeout=30, isolation_level=None, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS dlq_index (
                error_id TEXT PRIMARY KEY, line_number INTEGER NOT NULL UNIQUE,
                unit TEXT NOT NULL, stage TEXT NOT NULL, error_code TEXT NOT NULL,
                source_key TEXT, resolved INTEGER NOT NULL DEFAULT 0,
                payload_offset INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL
            ) WITHOUT ROWID
        """)

    def append(self, *, unit: str, stage: str, source_key: list[Any] | None, error_code: str,
               error: str, attempt: int = 1, retryable: bool = False, hint: str | None = None,
               payload: Any = None, details: dict[str, Any] | None = None) -> dict[str, Any]:
        code = error_code if error_code in ERROR_CODES else "UNKNOWN"
        timestamp = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        payload_bytes = encode(payload).encode("utf-8") if payload is not None else b""
        payload_ref = "sha256:" + hashlib.sha256(payload_bytes).hexdigest() if payload is not None else None
        self._handle.flush()
        with self._lock, self.lock_path.open("a+b") as lock_handle, exclusive_file_lock(lock_handle):
            self._handle.seek(0, os.SEEK_END)
            offset = self._handle.tell()
            line_number = int(self.db.execute("SELECT COALESCE(MAX(line_number),0)+1 FROM dlq_index").fetchone()[0])
            error_id = f"err_{line_number:09d}"
            record = {
                "error_id": error_id, "ts": timestamp, "job_id": self.job_id, "unit": unit, "stage": stage,
                "source_key": source_key, "error_code": code, "error": str(error), "attempt": int(attempt),
                "retryable": bool(retryable), "hint": hint, "payload_ref": payload_ref,
                "payload": None if self.redact_payload else payload,
                "replayable": code not in _NON_REPLAYABLE, "resolved": False,
            }
            if details:
                record["details"] = details
            line = encode(record).encode("utf-8") + b"\n"
            self._handle.write(line)
            self._handle.flush()
            self.db.execute("INSERT INTO dlq_index(error_id,line_number,unit,stage,error_code,source_key,resolved,payload_offset,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                            (error_id, line_number, unit, stage, code, encode(source_key) if source_key is not None else None, 0, offset, timestamp))
            self._pending_fsync += 1
            if self._pending_fsync >= self.fsync_every:
                os.fsync(self._handle.fileno())
                self._pending_fsync = 0
            return record


    def get(self, error_id: str, *, include_payload: bool = True) -> dict[str, Any] | None:
        row = self.db.execute("SELECT payload_offset,resolved FROM dlq_index WHERE error_id=?", (error_id,)).fetchone()
        if row is None:
            return None
        with self._lock:
            self._handle.flush()
            self._handle.seek(int(row[0]))
            line = self._handle.readline()
        if not line:
            return None
        record = json.loads(line)
        record["resolved"] = bool(row[1])
        if not include_payload:
            record["payload"] = None
        return record

    def list(self, *, code: str | None = None, unit: str | None = None, stage: str | None = None,
             resolved: bool | None = False, limit: int = 100, cursor: str | None = None) -> tuple[list[dict[str, Any]], str | None, bool]:
        if not 1 <= limit <= 500:
            raise ValueError("DLQ page limit must be 1..500")
        clauses: list[str] = []
        params: list[Any] = []
        if code: clauses.append("error_code=?"); params.append(code)
        if unit: clauses.append("unit=?"); params.append(unit)
        if stage: clauses.append("stage=?"); params.append(stage)
        if resolved is not None: clauses.append("resolved=?"); params.append(int(resolved))
        if cursor: clauses.append("line_number > (SELECT line_number FROM dlq_index WHERE error_id=?)"); params.append(cursor)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        rows = self.db.execute(f"SELECT error_id FROM dlq_index{where} ORDER BY line_number LIMIT ?", (*params, limit + 1)).fetchall()
        has_more = len(rows) > limit
        rows = rows[:limit]
        items = [entry for entry in (self.get(row[0], include_payload=False) for row in rows) if entry is not None]
        next_cursor = rows[-1][0] if has_more and rows else None
        return items, next_cursor, has_more

    def mark_resolved(self, error_id: str) -> bool:
        cursor = self.db.execute("UPDATE dlq_index SET resolved=1 WHERE error_id=?", (error_id,))
        if cursor.rowcount:
            # The append-only log keeps the original immutable record; resolution is indexed state.
            return True
        return False

    def resolved(self, error_id: str) -> bool:
        row = self.db.execute("SELECT resolved FROM dlq_index WHERE error_id=?", (error_id,)).fetchone()
        return bool(row and row[0])

    def count(self, *, code: str | None = None, resolved: bool | None = None) -> int:
        clauses, params = [], []
        if code: clauses.append("error_code=?"); params.append(code)
        if resolved is not None: clauses.append("resolved=?"); params.append(int(resolved))
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        return int(self.db.execute(f"SELECT COUNT(*) FROM dlq_index{where}", params).fetchone()[0])

    def download(self) -> bytes:
        self._handle.flush()
        return self.path.read_bytes()

    def close(self) -> None:
        with self._lock:
            if self._pending_fsync:
                self._handle.flush()
                os.fsync(self._handle.fileno())
                self._pending_fsync = 0
            self._handle.close()
            self.db.close()

    def __enter__(self) -> "DeadLetterLog": return self
    def __exit__(self, *_: Any) -> None: self.close()


def bisect_batch(items: list[Any], writer: Callable[[list[Any]], Any], dlq: DeadLetterLog, *, unit: str, stage: str,
                 key_of: Callable[[Any], list[Any] | None], error_code: Callable[[BaseException], str] = error_code_for,
                 hint_of: Callable[[BaseException], str | None] | None = None, payload_of: Callable[[Any], Any] | None = None,
                 attempt: int = 1) -> tuple[int, int]:
    """Retry a failed write by recursive batch bisection; isolate and DLQ poison singleton rows."""
    if not items:
        return 0, 0
    try:
        writer(items)
        return len(items), 0
    except BaseException as exc:
        if len(items) == 1:
            item = items[0]
            code = error_code(exc)
            hint = hint_of(exc) if hint_of else None
            dlq.append(unit=unit, stage=stage, source_key=key_of(item), error_code=code, error=str(exc), attempt=attempt,
                       retryable=code == "TRANSIENT_EXHAUSTED", hint=hint,
                       payload=None if payload_of is None else payload_of(item))
            return 0, 1
        middle = len(items) // 2
        left_good, left_bad = bisect_batch(items[:middle], writer, dlq, unit=unit, stage=stage, key_of=key_of,
                                           error_code=error_code, hint_of=hint_of, payload_of=payload_of, attempt=attempt + 1)
        right_good, right_bad = bisect_batch(items[middle:], writer, dlq, unit=unit, stage=stage, key_of=key_of,
                                              error_code=error_code, hint_of=hint_of, payload_of=payload_of, attempt=attempt + 1)
        return left_good + right_good, left_bad + right_bad
