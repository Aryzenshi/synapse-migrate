"""Resumable checkpoint records and distributed worker leases with fencing tokens."""
from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator
import base64
import datetime as dt
import decimal
import uuid

from synapse.adapters.state.base import StateStore


def _pack_checkpoint_key(value: Any) -> Any:
    if isinstance(value, list): return [_pack_checkpoint_key(item) for item in value]
    if isinstance(value, tuple): return [_pack_checkpoint_key(item) for item in value]
    if isinstance(value, dict): return {str(key): _pack_checkpoint_key(item) for key, item in value.items()}
    if isinstance(value, dt.datetime): return {"$synapse_datetime": value.isoformat(), "fold": value.fold}
    if isinstance(value, dt.date): return {"$synapse_date": value.isoformat()}
    if isinstance(value, decimal.Decimal): return {"$synapse_decimal": str(value)}
    if isinstance(value, (bytes, bytearray, memoryview)): return {"$synapse_bytes": base64.b64encode(bytes(value)).decode("ascii")}
    if isinstance(value, uuid.UUID): return {"$synapse_uuid": str(value)}
    if type(value).__name__.lower() == "objectid": return {"$synapse_oid": str(value)}
    return value


def _unpack_checkpoint_key(value: Any) -> Any:
    if isinstance(value, list): return [_unpack_checkpoint_key(item) for item in value]
    if isinstance(value, dict):
        if "$synapse_datetime" in value: return dt.datetime.fromisoformat(value["$synapse_datetime"]).replace(fold=int(value.get("fold", 0)))
        if "$synapse_date" in value: return dt.date.fromisoformat(value["$synapse_date"])
        if "$synapse_decimal" in value: return decimal.Decimal(value["$synapse_decimal"])
        if "$synapse_bytes" in value: return base64.b64decode(value["$synapse_bytes"])
        if "$synapse_uuid" in value: return uuid.UUID(value["$synapse_uuid"])
        if "$synapse_oid" in value:
            from bson import ObjectId
            return ObjectId(value["$synapse_oid"])
        return {str(key): _unpack_checkpoint_key(item) for key, item in value.items()}
    return value


class StaleFenceError(RuntimeError):
    pass


class PlanChangedError(RuntimeError):
    pass


class LeaseUnavailableError(RuntimeError):
    pass


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass
class Checkpoint:
    job_id: str
    unit_id: str
    stage: str
    last_key: list[Any] | None
    batch_seq: int
    rows_done: int
    docs_written: int | None
    errors: int
    plan_hash: str
    fence: int
    updated_at: str
    status: str = "running"

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["last_key"] = _pack_checkpoint_key(value["last_key"])
        return value


@dataclass(frozen=True)
class FencedLease:
    job_id: str
    worker_id: str
    fence: int
    ttl_seconds: int
    expires_at: float


class LeaseHeartbeat:
    def __init__(self, store: StateStore, lease: FencedLease):
        self.store = store
        self.lease = lease
        self.lost = threading.Event()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name=f"synapse-lease-{lease.job_id}", daemon=True)

    def start(self) -> "LeaseHeartbeat":
        self._thread.start()
        return self

    def _run(self) -> None:
        interval = max(1.0, self.lease.ttl_seconds / 3)
        while not self._stop.wait(interval):
            try:
                okay = self.store.heartbeat_lease(self.lease.job_id, self.lease.worker_id, self.lease.fence, self.lease.ttl_seconds)
            except Exception:
                okay = False
            if not okay:
                self.lost.set()
                return

    def assert_current(self) -> None:
        if self.lost.is_set():
            raise StaleFenceError(f"worker lost lease for job {self.lease.job_id}; writes must stop")

    def close(self) -> None:
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join(timeout=2)
        try:
            self.store.release_lease(self.lease.job_id, self.lease.worker_id, self.lease.fence)
        except Exception:
            pass

    def __enter__(self) -> "LeaseHeartbeat": return self.start()
    def __exit__(self, *_: Any) -> None: self.close()


class CheckpointManager:
    def __init__(self, job_id: str, db_path: str | Path, state_store: StateStore, *, lease_ttl_seconds: int = 30):
        self.job_id = job_id
        self.db_path = Path(db_path).expanduser().resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_store = state_store
        self.lease_ttl_seconds = lease_ttl_seconds
        self._lock = threading.RLock()
        self.db = sqlite3.connect(str(self.db_path), timeout=30, isolation_level=None, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.execute("PRAGMA busy_timeout=30000")
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS checkpoint (
                job_id TEXT NOT NULL, unit_id TEXT NOT NULL, stage TEXT NOT NULL,
                last_key TEXT, batch_seq INTEGER NOT NULL, rows_done INTEGER NOT NULL,
                docs_written INTEGER, errors INTEGER NOT NULL, plan_hash TEXT NOT NULL,
                fence INTEGER NOT NULL, updated_at TEXT NOT NULL, status TEXT NOT NULL,
                PRIMARY KEY(job_id,unit_id,stage)
            ) WITHOUT ROWID
        """)
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS dlq_index (
                error_id TEXT PRIMARY KEY, line_number INTEGER NOT NULL UNIQUE,
                unit TEXT NOT NULL, stage TEXT NOT NULL, error_code TEXT NOT NULL,
                source_key TEXT, resolved INTEGER NOT NULL DEFAULT 0,
                payload_offset INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL
            ) WITHOUT ROWID
        """)

    def acquire(self, worker_id: str | None = None) -> FencedLease | None:
        worker = worker_id or f"worker-{uuid.uuid4().hex[:12]}"
        lease = self.state_store.acquire_lease(self.job_id, worker, self.lease_ttl_seconds)
        if lease is None:
            return None
        return FencedLease(self.job_id, worker, int(lease["fence"]), self.lease_ttl_seconds, float(lease["expires_at"]))

    def lease(self, worker_id: str | None = None) -> LeaseHeartbeat:
        lease = self.acquire(worker_id)
        if lease is None:
            raise LeaseUnavailableError(f"job {self.job_id} already has an unexpired worker lease")
        return LeaseHeartbeat(self.state_store, lease)

    def load(self, unit_id: str, stage: str, *, plan_hash: str, minimum_fence: int | None = None) -> Checkpoint | None:
        with self._lock:
            row = self.db.execute("SELECT last_key,batch_seq,rows_done,docs_written,errors,plan_hash,fence,updated_at,status FROM checkpoint WHERE job_id=? AND unit_id=? AND stage=?",
                                  (self.job_id, unit_id, stage)).fetchone()
        if row is None:
            value = self.state_store.get(f"checkpoint:{self.job_id}:{unit_id}:{stage}")
            if value is None:
                return None
            checkpoint = Checkpoint(**{**value, "last_key": _unpack_checkpoint_key(value.get("last_key"))})
        else:
            checkpoint = Checkpoint(self.job_id, unit_id, stage, _unpack_checkpoint_key(json.loads(row[0])) if row[0] else None,
                                    int(row[1]), int(row[2]), row[3], int(row[4]), row[5], int(row[6]), row[7], row[8])
        if checkpoint.plan_hash != plan_hash:
            raise PlanChangedError("checkpoint plan_hash does not match the Approved Plan")
        if minimum_fence is not None and checkpoint.fence > minimum_fence:
            raise StaleFenceError("checkpoint was written by a worker with a newer fencing token")
        return checkpoint

    def commit_forward(self, checkpoint: Checkpoint) -> None:
        if checkpoint.job_id != self.job_id:
            raise ValueError("checkpoint belongs to a different job")
        with self._lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                current = self.db.execute("SELECT fence,batch_seq,plan_hash FROM checkpoint WHERE job_id=? AND unit_id=? AND stage=?",
                                         (self.job_id, checkpoint.unit_id, checkpoint.stage)).fetchone()
                if current and int(current[0]) > checkpoint.fence:
                    raise StaleFenceError("stale worker cannot overwrite a checkpoint")
                if current and current[2] != checkpoint.plan_hash:
                    raise PlanChangedError("checkpoint plan_hash changed during the job")
                if current and checkpoint.batch_seq < int(current[1]):
                    raise ValueError("checkpoint batch_seq cannot move backwards")
                self.db.execute("""
                    INSERT INTO checkpoint(job_id,unit_id,stage,last_key,batch_seq,rows_done,docs_written,errors,plan_hash,fence,updated_at,status)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(job_id,unit_id,stage) DO UPDATE SET
                      last_key=excluded.last_key,batch_seq=excluded.batch_seq,rows_done=excluded.rows_done,
                      docs_written=excluded.docs_written,errors=excluded.errors,plan_hash=excluded.plan_hash,
                      fence=excluded.fence,updated_at=excluded.updated_at,status=excluded.status
                """, (checkpoint.job_id, checkpoint.unit_id, checkpoint.stage,
                      json.dumps(_pack_checkpoint_key(checkpoint.last_key), separators=(",", ":"), ensure_ascii=False) if checkpoint.last_key is not None else None,
                      checkpoint.batch_seq, checkpoint.rows_done, checkpoint.docs_written, checkpoint.errors,
                      checkpoint.plan_hash, checkpoint.fence, checkpoint.updated_at, checkpoint.status))
                self.db.execute("COMMIT")
            except BaseException:
                self.db.execute("ROLLBACK")
                raise
        self.state_store.set(f"checkpoint:{self.job_id}:{checkpoint.unit_id}:{checkpoint.stage}", checkpoint.to_dict())

    def list(self) -> list[Checkpoint]:
        with self._lock:
            rows = self.db.execute("SELECT unit_id,stage,last_key,batch_seq,rows_done,docs_written,errors,plan_hash,fence,updated_at,status FROM checkpoint WHERE job_id=? ORDER BY unit_id,stage", (self.job_id,)).fetchall()
        return [Checkpoint(self.job_id, row[0], row[1], _unpack_checkpoint_key(json.loads(row[2])) if row[2] else None,
                           int(row[3]), int(row[4]), row[5], int(row[6]), row[7], int(row[8]), row[9], row[10]) for row in rows]

    def write_reverse_checkpoint(self, connection: Any, checkpoint: Checkpoint, *, table: str = "_synapse_checkpoint") -> None:
        """Write the reverse checkpoint within the caller's existing SQL batch transaction."""
        if not getattr(connection, "in_transaction", lambda: False)():
            raise RuntimeError("reverse checkpoint must be written inside the same open SQL transaction as its data")
        from sqlalchemy import text
        dialect = connection.dialect.name
        qtable = connection.dialect.identifier_preparer.quote(table)
        try:
            row = connection.execute(text(f"SELECT fence, plan_hash, batch_seq FROM {qtable} WHERE job_id=:job AND unit_id=:unit AND stage=:stage FOR UPDATE"),
                                     {"job": checkpoint.job_id, "unit": checkpoint.unit_id, "stage": checkpoint.stage}).first()
        except Exception:
            row = connection.execute(text(f"SELECT fence, plan_hash, batch_seq FROM {qtable} WHERE job_id=:job AND unit_id=:unit AND stage=:stage"),
                                     {"job": checkpoint.job_id, "unit": checkpoint.unit_id, "stage": checkpoint.stage}).first()
        if row and int(row[0]) > checkpoint.fence:
            raise StaleFenceError("stale worker cannot advance reverse checkpoint")
        if row and row[1] != checkpoint.plan_hash:
            raise PlanChangedError("reverse checkpoint has a different plan_hash")
        if row and checkpoint.batch_seq < int(row[2]):
            raise ValueError("reverse checkpoint batch_seq cannot move backwards")
        params = {"job": checkpoint.job_id, "unit": checkpoint.unit_id, "stage": checkpoint.stage,
                  "last_key": json.dumps(_pack_checkpoint_key(checkpoint.last_key), separators=(",", ":"), ensure_ascii=False) if checkpoint.last_key is not None else None,
                  "batch_seq": checkpoint.batch_seq, "rows_done": checkpoint.rows_done, "docs_written": checkpoint.docs_written,
                  "errors": checkpoint.errors, "plan_hash": checkpoint.plan_hash, "fence": checkpoint.fence,
                  "status": checkpoint.status, "updated_at": datetime.fromisoformat(checkpoint.updated_at.replace("Z", "+00:00"))}
        if dialect == "mysql":
            params["updated_at"] = params["updated_at"].replace(tzinfo=None)
            last_key_expr = ":last_key"
            sql = f"INSERT INTO {qtable}(job_id,unit_id,stage,last_key,batch_seq,rows_done,docs_written,errors,plan_hash,fence,status,updated_at) VALUES(:job,:unit,:stage,{last_key_expr},:batch_seq,:rows_done,:docs_written,:errors,:plan_hash,:fence,:status,:updated_at) ON DUPLICATE KEY UPDATE last_key=VALUES(last_key),batch_seq=VALUES(batch_seq),rows_done=VALUES(rows_done),docs_written=VALUES(docs_written),errors=VALUES(errors),plan_hash=VALUES(plan_hash),fence=VALUES(fence),status=VALUES(status),updated_at=VALUES(updated_at)"
        else:
            last_key_expr = "CAST(:last_key AS JSONB)"
            sql = f"INSERT INTO {qtable}(job_id,unit_id,stage,last_key,batch_seq,rows_done,docs_written,errors,plan_hash,fence,status,updated_at) VALUES(:job,:unit,:stage,{last_key_expr},:batch_seq,:rows_done,:docs_written,:errors,:plan_hash,:fence,:status,:updated_at) ON CONFLICT(job_id,unit_id,stage) DO UPDATE SET last_key=EXCLUDED.last_key,batch_seq=EXCLUDED.batch_seq,rows_done=EXCLUDED.rows_done,docs_written=EXCLUDED.docs_written,errors=EXCLUDED.errors,plan_hash=EXCLUDED.plan_hash,fence=EXCLUDED.fence,status=EXCLUDED.status,updated_at=EXCLUDED.updated_at"
        connection.execute(text(sql), params)
        # Local/UI mirror is deliberately updated only after the SQL transaction commits.

    def mirror_reverse_after_commit(self, checkpoint: Checkpoint) -> None:
        self.commit_forward(checkpoint)

    def close(self) -> None:
        with self._lock:
            self.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            self.db.close()
