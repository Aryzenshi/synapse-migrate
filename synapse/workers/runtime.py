"""Fenced, restartable job executor used by the worker container and API dev mode."""
from __future__ import annotations

import json
import os
import socket
import time
import traceback
from collections import Counter
from pathlib import Path
from typing import Any

from synapse.adapters.connections import create_mongo_client, create_sql_engine
from synapse.adapters.idmap.sqlite_store import SqliteIdMapStore
from synapse.adapters.vault import CredentialExpiredError
from synapse.api.runtime import build_vault
from synapse.api.store import ApiRepository, utc_now
from synapse.domain.ir.plan import ApprovedPlan
from synapse.etl.checkpoint import CheckpointManager, LeaseUnavailableError, PlanChangedError, StaleFenceError
from synapse.etl.dlq import DeadLetterLog
from synapse.etl.forward import ForwardETLRunner
from synapse.etl.reverse import ReverseETLRunner
from synapse.etl.verify import Verifier


class JobWorker:
    def __init__(self, repository: ApiRepository, *, worker_id: str | None = None, lease_ttl_seconds: int = 30):
        self.repository = repository
        self.store = repository.store
        self.vault = build_vault(self.store)
        self.worker_id = worker_id or f"{socket.gethostname()}-{os.getpid()}"
        self.lease_ttl_seconds = lease_ttl_seconds

    def run_once(self) -> int:
        processed = 0
        for job in self.repository.list_jobs():
            if job.get("status") != "queued":
                continue
            if self.run_job(job["job_id"]):
                processed += 1
        return processed

    def run_job(self, job_id: str) -> bool:
        job = self.repository.get_job(job_id)
        if not job or job.get("status") not in {"queued", "running"}:
            return False
        job_type = job.get("job_type")
        plan_wrapper = self.repository.get_plan(job.get("plan_id", "")) if job_type in {"migration", "verify", "replay"} else None
        if job_type not in {"migration", "verify", "replay"} or plan_wrapper is None:
            return False
        plan = ApprovedPlan.model_validate(plan_wrapper["plan"])
        if not plan.verify_hash(job.get("plan_hash")):
            job.update(status="failed", error_code="plan_changed", error="Approved Plan hash mismatch", updated_at=utc_now())
            self.repository.save_job(job)
            return True
        directory = self.repository.job_dir / job_id
        directory.mkdir(parents=True, exist_ok=True)
        checkpoints = CheckpointManager(job_id, directory / "checkpoints.sqlite", self.store, lease_ttl_seconds=self.lease_ttl_seconds)
        try:
            lease = checkpoints.lease(self.worker_id)
        except LeaseUnavailableError:
            checkpoints.close()
            return False
        heartbeat = lease
        source_engine = None
        target_engine = None
        source_client = None
        target_client = None
        dlq = None
        idmap = None
        try:
            with heartbeat:
                job = self.repository.get_job(job_id) or job
                job.update(status="running", worker={"id": self.worker_id, "fence": heartbeat.lease.fence,
                                                      "lease_expires_at": heartbeat.lease.expires_at},
                           started_at=job.get("started_at") or utc_now(), updated_at=utc_now())
                self.repository.save_job(job)
                try:
                    source_meta, source_credentials = self._connection(job["source_connection_id"])
                    target_meta, target_credentials = self._connection(job["target_connection_id"])
                except CredentialExpiredError:
                    job.update(status="waiting_for_credentials", updated_at=utc_now(), error_code="connection_expired")
                    self.repository.save_job(job)
                    return True
                direction = plan.direction.value
                if job_type == "verify":
                    parent_id = job.get("parent_job_id")
                    parent_dir = self.repository.job_dir / str(parent_id)
                    source, target, source_engine, target_engine, source_client, target_client = self._open_endpoints(
                        direction, source_meta, source_credentials, target_meta, target_credentials)
                    idmap = SqliteIdMapStore(parent_dir / "idmap.sqlite", cache_entries=int(plan.etl_options.get("idmap_cache_entries", 100_000)))
                    dlq = DeadLetterLog(parent_dir / "migration_errors.log", str(parent_id), parent_dir / "checkpoints.sqlite")
                    verification = self._verify(plan, source, target, idmap, dlq, direction)
                    result = {"status": "completed", "verification": verification}
                elif job.get("mode") == "dry_run":
                    result = self._dry_run(plan, source_meta, source_credentials, target_meta, target_credentials, job)
                else:
                    source, target, source_engine, target_engine, source_client, target_client = self._open_endpoints(
                        direction, source_meta, source_credentials, target_meta, target_credentials)
                    if direction == "RELATIONAL_TO_DOCUMENT":
                        self._apply_mongo_schema(target, plan)
                    map_directory = self.repository.job_dir / str(job.get("replay_parent_job_id")) if job.get("replay_parent_job_id") else directory
                    idmap = SqliteIdMapStore(map_directory / "idmap.sqlite", cache_entries=int(plan.etl_options.get("idmap_cache_entries", 100_000)))
                    dlq = DeadLetterLog(directory / "migration_errors.log", job_id, directory / "checkpoints.sqlite",
                                        redact_payload=bool((job.get("options", {}).get("dlq") or {}).get("redact_payload", False)))
                    options = {**plan.etl_options, **job.get("options", {})}
                    options.update({"on_progress": self._progress_callback(job_id),
                                    "stop_requested": lambda: self._control_state(job_id) in {"pause_requested", "cancel_requested"},
                                    "stop_status": lambda: "cancelled" if self._control_state(job_id) == "cancel_requested" else "paused",
                                    "apply_ddl": True})
                    if direction == "RELATIONAL_TO_DOCUMENT":
                        runner = ForwardETLRunner(plan, source, target, idmap, checkpoints, dlq, options=options, lease=heartbeat)
                    else:
                        runner = ReverseETLRunner(plan, source, target, idmap, checkpoints, dlq, options=options, lease=heartbeat)
                    result = runner.run()
                    plan_wrapper.update(applied=True, applied_connection_id=job["target_connection_id"], applied_at=utc_now())
                    self.repository.save_plan(plan_wrapper)
                    if job.get("options", {}).get("verify_after"):
                        result["verification"] = self._verify(plan, source, target, idmap, dlq, direction)
                    parent_id = job.get("replay_parent_job_id")
                    if parent_id and result.get("status", "").startswith("completed"):
                        self._resolve_replayed_errors(str(parent_id), job.get("selected_errors", []))
                heartbeat.assert_current()
                current = self.repository.get_job(job_id) or job
                current["result"] = result
                current["progress"] = self._final_progress(result)
                current["error_summary"] = self._error_summary(dlq) if dlq else {"total": 0, "by_code": {}}
                current["drift"] = {"widened_columns": result.get("widened_columns", [])}
                control = self._control_state(job_id)
                if control in {"pause_requested", "paused"} or result.get("status") == "paused":
                    status = "paused"
                elif control == "cancel_requested" or result.get("status") == "cancelled":
                    status = "cancelled"
                elif result.get("status") == "failed_threshold":
                    status = "failed_threshold"
                else:
                    errors = int(result.get("errors", 0))
                    status = "completed_with_errors" if errors else "completed"
                current.update(status=status, updated_at=utc_now(), completed_at=utc_now() if status not in {"paused", "cancelled"} else None)
                current.pop("control", None)
                self.repository.save_job(current)
                return True
        except CredentialExpiredError:
            current = self.repository.get_job(job_id) or job
            current.update(status="waiting_for_credentials", error_code="connection_expired", updated_at=utc_now())
            self.repository.save_job(current)
            return True
        except (PlanChangedError, StaleFenceError) as exc:
            current = self.repository.get_job(job_id) or job
            current.update(status="failed", error_code="plan_changed" if isinstance(exc, PlanChangedError) else "lease_lost",
                           error=str(exc), updated_at=utc_now())
            self.repository.save_job(current)
            return True
        except Exception as exc:
            current = self.repository.get_job(job_id) or job
            current.update(status="failed_threshold" if "failed_threshold" in str(exc) else "failed",
                           error_code="worker_error", error=str(exc), traceback=traceback.format_exc()[-4000:], updated_at=utc_now())
            self.repository.save_job(current)
            return True
        finally:
            if dlq: dlq.close()
            if idmap: idmap.close()
            if source_engine: source_engine.dispose()
            if target_engine: target_engine.dispose()
            if source_client: source_client.close()
            if target_client: target_client.close()
            checkpoints.close()

    def _open_endpoints(self, direction: str, source_meta: dict[str, Any], source_credentials: dict[str, Any],
                        target_meta: dict[str, Any], target_credentials: dict[str, Any]):
        source_engine = target_engine = source_client = target_client = None
        if direction == "RELATIONAL_TO_DOCUMENT":
            source_engine = create_sql_engine(source_meta["engine"], source_credentials)
            target_client = create_mongo_client(target_credentials)
            return source_engine, target_client[target_meta["database"]], source_engine, target_engine, source_client, target_client
        source_client = create_mongo_client(source_credentials)
        target_engine = create_sql_engine(target_meta["engine"], target_credentials)
        return source_client[source_meta["database"]], target_engine, source_engine, target_engine, source_client, target_client

    def _resolve_replayed_errors(self, parent_job_id: str, error_ids: list[str]) -> None:
        if not error_ids: return
        parent_dir = self.repository.job_dir / parent_job_id
        log = DeadLetterLog(parent_dir / "migration_errors.log", parent_job_id, parent_dir / "checkpoints.sqlite")
        try:
            for error_id in error_ids: log.mark_resolved(error_id)
        finally:
            log.close()

    def _connection(self, connection_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
        metadata = self.repository.get_connection(connection_id)
        if not metadata:
            raise CredentialExpiredError(connection_id)
        credentials = self.vault.get(connection_id)
        return metadata, credentials

    def _control_state(self, job_id: str) -> str | None:
        job = self.repository.get_job(job_id)
        return job.get("control") if job else None

    def _progress_callback(self, job_id: str):
        def update(event: dict[str, Any]) -> None:
            job = self.repository.get_job(job_id)
            if not job: return
            progress = job.setdefault("progress", {"units": []})
            units = progress.setdefault("units", [])
            identity = (event.get("unit_id"), event.get("stage"))
            existing = next((item for item in units if (item.get("unit_id"), item.get("stage")) == identity), None)
            if existing is None:
                existing = {"unit_id": identity[0], "stage": identity[1]}
                units.append(existing)
            existing.update({key: value for key, value in event.items() if key != "batch_seconds"})
            existing["status"] = "running"
            rows = sum(int(item.get("rows_done", 0)) for item in units)
            progress["rows_done"] = rows
            progress["throughput_rows_per_sec"] = float(event.get("rows_done", 0)) / max(float(event.get("batch_seconds", 1)), 0.001)
            progress["memory_mb"] = round(float(event.get("batch_bytes", 0)) / (1024 * 1024), 3)
            job["updated_at"] = utc_now()
            self.repository.save_job(job)
            self.repository.append_event(job_id, {"event": "progress", **event})
        return update

    def _apply_mongo_schema(self, database: Any, plan: ApprovedPlan) -> None:
        validators = plan.options.get("validators", {})
        index_map = plan.options.get("indexes", {})
        for collection_name, validator in validators.items():
            names = database.list_collection_names()
            if collection_name not in names:
                database.create_collection(collection_name, validator=validator, validationLevel="strict", validationAction="error")
            else:
                database.command({"collMod": collection_name, "validator": validator, "validationLevel": "strict", "validationAction": "error"})
        for collection_name, indexes in index_map.items():
            collection = database[collection_name]
            for index in indexes:
                collection.create_index(list(index["keys"].items()), **index.get("options", {}))

    def _dry_run(self, plan: ApprovedPlan, source_meta: dict[str, Any], source_credentials: dict[str, Any],
                 target_meta: dict[str, Any], target_credentials: dict[str, Any], job: dict[str, Any]) -> dict[str, Any]:
        direction = plan.direction.value
        rows = 0
        sampled = 0
        max_document_bytes = 0
        if direction == "RELATIONAL_TO_DOCUMENT":
            engine = create_sql_engine(source_meta["engine"], source_credentials)
            try:
                from sqlalchemy import text
                with engine.connect() as connection:
                    for table in plan.source_ir.get("tables", []):
                        name = table["table_name"]
                        q = connection.dialect.identifier_preparer.quote(name)
                        if table.get("schema_name"): q = connection.dialect.identifier_preparer.quote_schema(table["schema_name"]) + "." + q
                        rows += int(connection.execute(text(f"SELECT COUNT(*) FROM {q}")).scalar_one())
            finally:
                engine.dispose()
        else:
            client = create_mongo_client(source_credentials)
            try:
                database = client[source_meta["database"]]
                for collection_name in [table["collection"] for table in plan.tables if table.get("role") == "root"]:
                    coll = database[collection_name]
                    rows += int(coll.estimated_document_count())
                    for doc in coll.find({}, limit=100):
                        sampled += 1
                        max_document_bytes = max(max_document_bytes, len(json.dumps(doc, default=str).encode("utf-8")))
            finally:
                client.close()
        return {"status": "completed", "dry_run": True, "rows_read": rows, "sampled_documents": sampled,
                "max_document_bytes_sampled": max_document_bytes, "dlq_would_be": 0,
                "target_writes": 0, "plan_hash": plan.plan_hash}

    @staticmethod
    def _dlq_counts_by_unit(dlq: Any) -> Counter:
        # DeadLetterLog.list returns a page tuple (items, next_cursor, has_more), not an item list.
        items, _next_cursor, _has_more = dlq.list(resolved=None, limit=500)
        return Counter(item["unit"] for item in items)

    @staticmethod
    def _verify(plan: ApprovedPlan, source: Any, target: Any, idmap: Any, dlq: Any, direction: str) -> dict[str, Any]:
        dlq_counts = JobWorker._dlq_counts_by_unit(dlq)
        verifier = Verifier(idmap=idmap)
        if direction == "RELATIONAL_TO_DOCUMENT":
            with source.connect() as connection:
                return verifier.verify_forward(connection, target, plan, dlq_counts=dlq_counts)
        with target.connect() as connection:
            return verifier.verify_reverse(source, connection, plan, dlq_counts=dlq_counts)

    @staticmethod
    def _error_summary(dlq: DeadLetterLog) -> dict[str, Any]:
        items, _, more = dlq.list(resolved=None, limit=500)
        counts = Counter(item["error_code"] for item in items)
        total = dlq.count()
        return {"total": total, "by_code": dict(counts), "truncated": more}

    @staticmethod
    def _final_progress(result: dict[str, Any]) -> dict[str, Any]:
        units = result.get("units", [])
        for item in units:
            item.setdefault("status", result.get("status", "completed"))
        return {"units": units, "rows_done": result.get("rows_done", 0), "overall_percent": 100 if result.get("status", "").startswith("completed") else None}
