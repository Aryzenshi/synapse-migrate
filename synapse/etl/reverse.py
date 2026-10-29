"""Two-phase MongoDB→SQL normalization runner with transactional checkpoints and bisection."""
from __future__ import annotations

import datetime as dt
import decimal
import hashlib
import json
import math
import time
from contextlib import contextmanager, nullcontext
from typing import Any

from synapse.adapters.idmap.base import IdMapStore
from synapse.domain.ir.plan import ApprovedPlan
from synapse.etl.checkpoint import (Checkpoint, CheckpointManager, PlanChangedError, StaleFenceError,
                                    _unpack_checkpoint_key, utc_iso)
from synapse.etl.dlq import DeadLetterLog, error_code_for
from synapse.domain.typemap.bson_to_sql import sql_widening


@contextmanager
def _existing_connection_transaction(connection: Any):
    transaction = connection.begin_nested() if connection.in_transaction() else connection.begin()
    try:
        yield connection
        transaction.commit()
    except BaseException:
        if transaction.is_active:
            transaction.rollback()
        raise


class DriftDetected(RuntimeError):
    def __init__(self, table: dict[str, Any], column: dict[str, Any], next_type: str, value: Any, original: BaseException):
        super().__init__(str(original))
        self.table = table
        self.column = column
        self.next_type = next_type
        self.value = value
        self.original = original


class ReverseETLRunner:
    def __init__(self, plan: ApprovedPlan, source_database: Any, target: Any, idmap: IdMapStore,
                 checkpoints: CheckpointManager, dlq: DeadLetterLog, *, options: dict[str, Any] | None = None,
                 lease: Any | None = None):
        if not plan.verify_hash():
            raise ValueError("Approved Plan hash is invalid")
        self.plan = plan
        self.source = source_database
        self.target = target
        self.idmap = idmap
        self.checkpoints = checkpoints
        self.dlq = dlq
        self.options = {**plan.etl_options, **(options or {})}
        self.lease = lease
        self.batch_size = int(self.options.get("batch_size", 2000))
        self.max_children = int(self.options.get("max_children_per_parent", plan.thresholds.max_children_per_parent))
        self.on_orphan = str(self.options.get("on_orphan", "DLQ")).upper()
        self.on_drift = str(self.options.get("on_drift", "widen")).lower()
        self.max_error_rate = float(self.options.get("max_error_rate", 0.01))
        self.tables = {table["name"]: table for table in plan.tables}
        self.collections = {table["collection"] for table in plan.tables if table.get("role") == "root"}
        self.id_column_types = {table["name"]: {column["name"]: column["sql_type"] for column in table.get("columns", [])} for table in plan.tables}
        self._error_window: list[tuple[int, int]] = []
        self._mapping_entries: list[tuple[str, Any, bytes, bool]] = []
        self._row_errors = 0
        self._dedup_count = 0
        self._drift_changes: list[dict[str, Any]] = []
        self._init_widening_index()

    def run(self) -> dict[str, Any]:
        self._assert_lease()
        self._apply_preload_if_required()
        root_tables = [table for table in self.plan.tables if table.get("role") == "root"]
        results: list[dict[str, Any]] = []
        # Global phase barrier: every root/reference target is materialized before any child/junction writes.
        for table in root_tables:
            results.append(self._run_collection_stage(table, "phase1"))
            if results[-1]["status"] in {"paused", "cancelled"}:
                errors = sum(result["errors"] for result in results)
                return {"status": results[-1]["status"], "units": results, "rows_done": sum(item["rows_done"] for item in results), "errors": errors, "dedup_count": self._dedup_count, "widened_columns": self._drift_changes}
        for table in root_tables:
            results.append(self._run_collection_stage(table, "phase2"))
            if results[-1]["status"] in {"paused", "cancelled"}:
                errors = sum(result["errors"] for result in results)
                return {"status": results[-1]["status"], "units": results, "rows_done": sum(item["rows_done"] for item in results), "errors": errors, "dedup_count": self._dedup_count, "widened_columns": self._drift_changes}
        self._apply_postload_if_required()
        errors = sum(result["errors"] for result in results)
        return {"status": "completed_with_errors" if errors else "completed", "units": results,
                "rows_done": sum(result["rows_done"] for result in results), "errors": errors,
                "dedup_count": self._dedup_count, "widened_columns": self._drift_changes}

    def _connect(self):
        try:
            from sqlalchemy.engine import Connection, Engine
            if isinstance(self.target, Engine):
                return self.target.begin()
            if isinstance(self.target, Connection):
                return _existing_connection_transaction(self.target)
        except ImportError:
            pass
        if hasattr(self.target, "begin") and hasattr(self.target, "connect"):
            return self.target.begin()
        return nullcontext(self.target)

    def _read_connection(self):
        try:
            from sqlalchemy.engine import Connection, Engine
            if isinstance(self.target, Engine):
                return self.target.connect()
            if isinstance(self.target, Connection):
                return nullcontext(self.target)
        except ImportError:
            pass
        if hasattr(self.target, "connect"):
            return self.target.connect()
        return nullcontext(self.target)

    def _assert_lease(self) -> None:
        if self.lease:
            self.lease.assert_current()

    def _fence(self) -> int:
        return int(self.lease.lease.fence) if self.lease else 0

    def _apply_preload_if_required(self) -> None:
        sql = self.plan.options.get("pre_load_sql")
        if not sql or not self.options.get("apply_ddl", True):
            return
        from sqlalchemy import inspect, text
        with self._read_connection() as connection:
            existing = set(inspect(connection).get_table_names())
        root_names = {table["name"] for table in self.plan.tables if table.get("role") == "root"}
        map_table = self.plan.options.get("reverse_idmap_table")
        has_map_table = any(table.get("name") == map_table for table in self.plan.tables) if map_table else False
        if root_names and root_names <= existing:
            if "_synapse_checkpoint" not in existing or (has_map_table and map_table not in existing):
                with self._connect() as connection:
                    if "_synapse_checkpoint" not in existing:
                        self._create_checkpoint_table(connection)
                    if has_map_table and map_table not in existing:
                        self._create_reverse_idmap_table(connection, str(map_table))
            return
        with self._connect() as connection:
            self._execute_script(connection, sql)

    def _apply_postload_if_required(self) -> None:
        sql = self.plan.options.get("post_load_sql")
        if not sql or not self.options.get("apply_ddl", True):
            return
        for statement in (part.strip() for part in sql.split(";")):
            if not statement:
                continue
            with self._read_connection() as probe:
                if self._constraint_exists(probe, statement):
                    continue
            with self._connect() as connection:
                from sqlalchemy import text
                connection.execute(text(statement))

    def _create_checkpoint_table(self, connection: Any) -> None:
        from sqlalchemy import text
        q = connection.dialect.identifier_preparer.quote
        if connection.dialect.name == "mysql":
            sql = f"CREATE TABLE IF NOT EXISTS {q('_synapse_checkpoint')} (job_id VARCHAR(128) NOT NULL, unit_id VARCHAR(255) NOT NULL, stage VARCHAR(32) NOT NULL, last_key JSON NULL, batch_seq BIGINT NOT NULL DEFAULT 0, rows_done BIGINT NOT NULL DEFAULT 0, docs_written BIGINT NULL, errors BIGINT NOT NULL DEFAULT 0, plan_hash VARCHAR(80) NOT NULL, fence BIGINT NOT NULL, status VARCHAR(32) NOT NULL, updated_at TIMESTAMP NOT NULL, PRIMARY KEY(job_id,unit_id,stage)) ENGINE=InnoDB"
        else:
            sql = f"CREATE TABLE IF NOT EXISTS {q('_synapse_checkpoint')} (job_id VARCHAR(128) NOT NULL, unit_id VARCHAR(255) NOT NULL, stage VARCHAR(32) NOT NULL, last_key JSONB NULL, batch_seq BIGINT NOT NULL DEFAULT 0, rows_done BIGINT NOT NULL DEFAULT 0, docs_written BIGINT NULL, errors BIGINT NOT NULL DEFAULT 0, plan_hash VARCHAR(80) NOT NULL, fence BIGINT NOT NULL, status VARCHAR(32) NOT NULL, updated_at TIMESTAMPTZ NOT NULL, PRIMARY KEY(job_id,unit_id,stage))"
        connection.execute(text(sql))

    def _create_reverse_idmap_table(self, connection: Any, table_name: str) -> None:
        from sqlalchemy import text
        q = connection.dialect.identifier_preparer.quote
        table = q(table_name)
        if connection.dialect.name == "mysql":
            sql = (f"CREATE TABLE IF NOT EXISTS {table} (source_namespace VARCHAR(128) NOT NULL, "
                   "source_collection VARCHAR(255) NOT NULL, source_key_hash CHAR(64) NOT NULL, "
                   "target_table VARCHAR(255) NOT NULL, target_id BIGINT UNSIGNED NOT NULL, "
                   "PRIMARY KEY(source_namespace,source_collection,source_key_hash)) ENGINE=InnoDB")
        else:
            sql = (f"CREATE TABLE IF NOT EXISTS {table} (source_namespace VARCHAR(128) NOT NULL, "
                   "source_collection VARCHAR(255) NOT NULL, source_key_hash CHAR(64) NOT NULL, "
                   "target_table VARCHAR(255) NOT NULL, target_id BIGINT NOT NULL, "
                   "PRIMARY KEY(source_namespace,source_collection,source_key_hash))")
        connection.execute(text(sql))

    @staticmethod
    def _unquote_identifier(value: str) -> str:
        return value.strip().strip('`').strip('"')

    def _constraint_exists(self, connection: Any, statement: str) -> bool:
        import re
        from sqlalchemy import inspect
        match = re.match(r'ALTER\s+TABLE\s+([`\"]?[A-Za-z0-9_]+[`\"]?)\s+ADD\s+CONSTRAINT\s+([`\"]?[A-Za-z0-9_]+[`\"]?)', statement, re.I)
        if match:
            table, name = map(self._unquote_identifier, match.groups())
            inspector = inspect(connection)
            existing = {item.get("name") for item in inspector.get_foreign_keys(table)} | {item.get("name") for item in inspector.get_check_constraints(table)}
            return name in existing
        match = re.match(r'CREATE\s+(?:UNIQUE\s+)?INDEX\s+([`\"]?[A-Za-z0-9_]+[`\"]?)\s+ON\s+([`\"]?[A-Za-z0-9_]+[`\"]?)', statement, re.I)
        if match:
            name, table = map(self._unquote_identifier, match.groups())
            try:
                return name in {item.get("name") for item in inspect(connection).get_indexes(table)}
            except Exception:
                return False
        return False

    @staticmethod
    def _execute_script(connection: Any, sql: str) -> None:
        from sqlalchemy import text
        for statement in (part.strip() for part in sql.split(";")):
            if statement:
                connection.execute(text(statement))

    def _load_checkpoint(self, unit: str, stage: str) -> Checkpoint | None:
        from sqlalchemy import text
        try:
            with self._read_connection() as connection:
                table_name = connection.dialect.identifier_preparer.quote("_synapse_checkpoint")
                row = connection.execute(text(f"SELECT last_key,batch_seq,rows_done,docs_written,errors,plan_hash,fence,updated_at,status FROM {table_name} WHERE job_id=:job AND unit_id=:unit AND stage=:stage"),
                                         {"job": self.checkpoints.job_id, "unit": unit, "stage": stage}).first()
            if row:
                last_key = _unpack_checkpoint_key((json.loads(row[0]) if isinstance(row[0], str) else row[0])) if row[0] else None
                checkpoint = Checkpoint(self.checkpoints.job_id, unit, stage, last_key, int(row[1]), int(row[2]), row[3], int(row[4]), row[5], int(row[6]), str(row[7]), str(row[8]))
                if checkpoint.plan_hash != self.plan.plan_hash:
                    raise PlanChangedError("target checkpoint plan_hash does not match the Approved Plan")
                if checkpoint.fence > self._fence():
                    raise StaleFenceError("a newer worker fencing token already advanced this checkpoint")
                return checkpoint
        except (PlanChangedError, StaleFenceError):
            raise
        except Exception:
            pass
        return self.checkpoints.load(unit, stage, plan_hash=self.plan.plan_hash)

    def _run_collection_stage(self, root: dict[str, Any], stage: str) -> dict[str, Any]:
        collection_name = root["collection"]
        source_collection = self.source[collection_name]
        checkpoint = self._load_checkpoint(collection_name, stage)
        last_key = checkpoint.last_key if checkpoint else None
        batch_seq = checkpoint.batch_seq if checkpoint else 0
        rows_done = checkpoint.rows_done if checkpoint else 0
        docs_written = checkpoint.docs_written or 0 if checkpoint else 0
        errors = checkpoint.errors if checkpoint else 0
        stopped_status: str | None = None
        while True:
            self._assert_lease()
            query: dict[str, Any] = {}
            if last_key:
                query = {"_id": {"$gt": self._decode_mongo_key(last_key[0], root)}}
            documents = list(source_collection.find(query).sort("_id", 1).limit(self.batch_size))
            if not documents:
                break
            batch_started = time.monotonic()
            page_last = self._encode_mongo_key(documents[-1].get("_id"))
            batch_errors_before = self._row_errors
            mapping_entries: list[tuple[str, Any, bytes, bool]] = []
            while True:
                try:
                    if stage == "phase1":
                        committed = self._phase1_batch(root, documents, collection_name, batch_seq + 1, rows_done + len(documents), docs_written, errors, [page_last], mapping_entries)
                    else:
                        committed = self._phase2_batch(root, documents, collection_name, batch_seq + 1, rows_done + len(documents), docs_written, errors, [page_last])
                    break
                except DriftDetected as drift:
                    self._apply_widening(drift)
            docs_written += committed[0]
            errors += committed[1]
            batch_seq += 1
            rows_done += len(documents)
            last_key = [page_last]
            batch_errors = errors - (checkpoint.errors if checkpoint else 0)
            batch_size = max(1, rows_done - (checkpoint.rows_done if checkpoint else 0))
            self._error_window.append((batch_size, max(0, self._row_errors - batch_errors_before)))
            self._error_window = self._error_window[-10:]
            if len(self._error_window) == 10:
                denominator = sum(size for size, _ in self._error_window)
                numerator = sum(count for _, count in self._error_window)
                if denominator and numerator / denominator > self.max_error_rate:
                    raise RuntimeError(f"failed_threshold: recent error rate {numerator / denominator:.4%} exceeds max_error_rate {self.max_error_rate:.4%}")
            if self.options.get("on_progress"):
                self.options["on_progress"]({"unit_id": collection_name, "stage": stage, "rows_done": rows_done,
                                              "docs_written": docs_written, "errors": errors, "last_key": last_key,
                                              "batch_seq": batch_seq, "batch_seconds": time.monotonic() - batch_started})
            if self.options.get("stop_requested") and self.options["stop_requested"]():
                stopped_status = str(self.options.get("stop_status", lambda: "paused")())
                break
            checkpoint = Checkpoint(self.checkpoints.job_id, collection_name, stage, last_key, batch_seq, rows_done,
                                    docs_written, errors, self.plan.plan_hash, self._fence(), utc_iso(), "running")
        final_status = stopped_status or ("completed_with_errors" if errors else "completed")
        final = Checkpoint(self.checkpoints.job_id, collection_name, stage, last_key, batch_seq, rows_done,
                           docs_written, errors, self.plan.plan_hash, self._fence(), utc_iso(), final_status)
        # Final status is itself fenced and persisted, using a transaction even when no data batch was read.
        with self._connect() as connection:
            self.checkpoints.write_reverse_checkpoint(connection, final)
        self.checkpoints.mirror_reverse_after_commit(final)
        return {"unit_id": collection_name, "stage": stage, "rows_done": rows_done,
                "docs_written": docs_written, "errors": errors, "batch_seq": batch_seq,
                "last_key": last_key, "status": final_status}

    def _reverse_source_namespace(self) -> str:
        return str(self.plan.options.get("reverse_idmap_namespace") or self.plan.project_id)

    def _reverse_source_key_hash(self, source_id: Any) -> str:
        encoded = json.dumps(self._encode_mongo_key(source_id), sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def _lookup_reverse_id(self, connection: Any, collection: str, source_id: Any) -> Any | None:
        from sqlalchemy import text
        table_name = str(self.plan.options.get("reverse_idmap_table") or "_synapse_reverse_idmap")
        q = connection.dialect.identifier_preparer.quote
        statement = text(f"SELECT {q('target_id')} FROM {q(table_name)} WHERE {q('source_namespace')}=:namespace AND {q('source_collection')}=:collection AND {q('source_key_hash')}=:key_hash")
        return connection.execute(statement, {"namespace": self._reverse_source_namespace(), "collection": collection,
                                               "key_hash": self._reverse_source_key_hash(source_id)}).scalar()

    def _store_reverse_id(self, connection: Any, root: dict[str, Any], source_id: Any, target_id: Any) -> None:
        from sqlalchemy import text
        table_name = str(self.plan.options.get("reverse_idmap_table") or "_synapse_reverse_idmap")
        q = connection.dialect.identifier_preparer.quote
        statement = text(f"INSERT INTO {q(table_name)} ({q('source_namespace')},{q('source_collection')},{q('source_key_hash')},{q('target_table')},{q('target_id')}) VALUES (:namespace,:collection,:key_hash,:target_table,:target_id)")
        connection.execute(statement, {"namespace": self._reverse_source_namespace(), "collection": root["collection"],
                                        "key_hash": self._reverse_source_key_hash(source_id), "target_table": root["name"],
                                        "target_id": target_id})

    def _insert_root_row(self, connection: Any, root: dict[str, Any], row: dict[str, Any]) -> Any:
        from sqlalchemy import text
        q = connection.dialect.identifier_preparer.quote
        available = {column["name"]: column for column in root.get("columns", [])}
        columns = [name for name in row if name in available and name != root.get("id_column") and not available[name].get("auto_increment")]
        if not columns:
            raise ValueError(f"no insertable columns for target table {root['name']}")
        table_name = q(root["name"])
        qcolumns = ", ".join(q(name) for name in columns)
        binds = {f"v{index}": row.get(name) for index, name in enumerate(columns)}
        placeholders = ", ".join(f":v{index}" for index in range(len(columns)))
        if connection.dialect.name == "postgresql":
            result = connection.execute(text(f"INSERT INTO {table_name} ({qcolumns}) VALUES ({placeholders}) RETURNING {q(root['id_column'])}"), binds)
            return result.scalar_one()
        result = connection.execute(text(f"INSERT INTO {table_name} ({qcolumns}) VALUES ({placeholders})"), binds)
        target_id = getattr(result, "lastrowid", None)
        if target_id is None:
            raise RuntimeError(f"SQL driver did not return the generated id for {root['name']}")
        return target_id

    def _update_root_row(self, connection: Any, root: dict[str, Any], row: dict[str, Any], target_id: Any) -> bool:
        from sqlalchemy import text
        q = connection.dialect.identifier_preparer.quote
        id_column = root["id_column"]
        columns = [name for name in row if name != id_column]
        if not columns:
            return True
        assignments = ", ".join(f"{q(name)}=:v{index}" for index, name in enumerate(columns))
        params = {f"v{index}": row.get(name) for index, name in enumerate(columns)}
        params["target_id"] = target_id
        result = connection.execute(text(f"UPDATE {q(root['name'])} SET {assignments} WHERE {q(id_column)}=:target_id"), params)
        if result.rowcount:
            return True
        # MySQL may report zero rows when an existing row is updated to identical values.
        exists = connection.execute(text(f"SELECT {q(id_column)} FROM {q(root['name'])} WHERE {q(id_column)}=:target_id"),
                                    {"target_id": target_id}).first()
        return exists is not None

    def _delete_reverse_id(self, connection: Any, collection: str, source_id: Any) -> None:
        from sqlalchemy import text
        table_name = str(self.plan.options.get("reverse_idmap_table") or "_synapse_reverse_idmap")
        q = connection.dialect.identifier_preparer.quote
        connection.execute(text(f"DELETE FROM {q(table_name)} WHERE {q('source_namespace')}=:namespace AND {q('source_collection')}=:collection AND {q('source_key_hash')}=:key_hash"),
                           {"namespace": self._reverse_source_namespace(), "collection": collection,
                            "key_hash": self._reverse_source_key_hash(source_id)})

    def _upsert_root_identity(self, connection: Any, root: dict[str, Any], row: dict[str, Any], source_id: Any) -> Any:
        target_id = self._lookup_reverse_id(connection, root["collection"], source_id)
        if target_id is not None and self._update_root_row(connection, root, row, target_id):
            return target_id
        if target_id is not None:
            self._delete_reverse_id(connection, root["collection"], source_id)
        target_id = self._insert_root_row(connection, root, row)
        self._store_reverse_id(connection, root, source_id, target_id)
        return target_id

    def _phase1_batch(self, root: dict[str, Any], documents: list[dict[str, Any]], unit: str, batch_seq: int,
                      rows_done: int, docs_written_so_far: int, errors_so_far: int, last_key: list[Any], mapping_entries: list[tuple[str, Any, bytes, bool]]) -> tuple[int, int]:
        records: list[dict[str, Any]] = []
        for document in documents:
            row = self._build_root_row(root, document)
            records.append({"row": row, "source_key": [self._encode_mongo_key(document.get("_id"))], "payload": document, "document": document})
        batch_errors_before = self._row_errors
        batch_mappings: list[tuple[str, Any, bytes, bool]] = []
        with self._connect() as connection:
            if root.get("legacy_id_column"):
                # Compatibility path for plans frozen before source IDs moved to internal metadata.
                successful = self._safe_upsert(connection, root, records, "phase1")
                ids_by_legacy = self._readback_ids(connection, root, successful)
                for record in successful:
                    sql_id = ids_by_legacy.get(self._legacy_value_key(record["row"].get(root["legacy_id_column"])))
                    if sql_id is None:
                        self.dlq.append(unit=unit, stage="phase1", source_key=record["source_key"], error_code="PARENT_NOT_LOADED",
                                        error="SQL read-back did not return the unique legacy identifier", payload=record["payload"])
                        self._row_errors += 1
                        continue
                    record["target_id"] = sql_id
            else:
                successful = []
                for record in records:
                    try:
                        with connection.begin_nested():
                            record["target_id"] = self._upsert_root_identity(connection, root, record["row"], record["document"].get("_id"))
                        successful.append(record)
                    except Exception as exc:
                        code = error_code_for(exc)
                        self.dlq.append(unit=unit, stage="phase1", source_key=record["source_key"],
                                        error_code=code if code != "UNKNOWN" else "SQL_CONSTRAINT",
                                        error=str(exc), attempt=1, payload=record["payload"])
                        self._row_errors += 1

            for record in successful:
                sql_id = record.get("target_id")
                if sql_id is None:
                    continue
                batch_mappings.append((unit, str(record["document"].get("_id")), self._pack_sql_value(sql_id), True))
                for side in self._side_tables(root):
                    side_record = self._build_side_row(side, record["document"], sql_id)
                    if side_record:
                        side_record["_source_key"] = record["source_key"]
                        side_record["_payload"] = record["payload"]
                        side_rows = self._safe_upsert(connection, side, [side_record], "phase1")
                        if not side_rows:
                            self._row_errors += 1
            checkpoint = Checkpoint(self.checkpoints.job_id, unit, "phase1", last_key, batch_seq, rows_done,
                                    docs_written_so_far + len(successful), errors_so_far + self._row_errors - batch_errors_before,
                                    self.plan.plan_hash, self._fence(), utc_iso(), "running")
            # Root, side-table data and authoritative reverse checkpoint commit atomically. The SQLite
            # ID map is advanced before the SQL commit: if SQL rolls back, replay re-reads the unique
            # legacy key and repairs the mapping; if the SQL commit succeeds, phase 2 can resolve it.
            self.idmap.put_many(batch_mappings)
            self.checkpoints.write_reverse_checkpoint(connection, checkpoint)
        mapping_entries.extend(batch_mappings)
        failed_count = self._row_errors - batch_errors_before
        return len(successful), max(0, failed_count)

    def _phase2_batch(self, root: dict[str, Any], documents: list[dict[str, Any]], unit: str, batch_seq: int,
                      rows_done: int, docs_written_so_far: int, errors_so_far: int, last_key: list[Any]) -> tuple[int, int]:
        batch_errors_before = self._row_errors
        docs_done = 0
        with self._connect() as connection:
            for document in documents:
                self._assert_lease()
                encoded_id = str(document.get("_id"))
                mapped = self.idmap.get(unit, encoded_id)
                if mapped is None:
                    self.dlq.append(unit=unit, stage="phase2", source_key=[self._encode_mongo_key(document.get("_id"))],
                                    error_code="PARENT_NOT_LOADED", error="parent document was not loaded in phase 1", payload=document)
                    self._row_errors += 1
                    continue
                parent_pk = self._unpack_sql_value(mapped, self._column_type(root, root["id_column"]))
                self._update_root_references(connection, root, document, parent_pk, unit)
                self._process_junctions(connection, root, document, parent_pk, unit)
                self._process_child_arrays(connection, root, document, parent_pk, unit, current_path="")
                docs_done += 1
            errors = self._row_errors - batch_errors_before
            checkpoint = Checkpoint(self.checkpoints.job_id, unit, "phase2", last_key, batch_seq, rows_done,
                                    docs_written_so_far + docs_done, errors_so_far + errors, self.plan.plan_hash, self._fence(), utc_iso(), "running")
            # All child/foreign-key writes and this checkpoint share one target SQL transaction.
            self.checkpoints.write_reverse_checkpoint(connection, checkpoint)
        return docs_done, self._row_errors - batch_errors_before

    def _build_root_row(self, root: dict[str, Any], document: dict[str, Any]) -> dict[str, Any]:
        row: dict[str, Any] = {}
        legacy_column = root.get("legacy_id_column")
        source_id = document.get("_id")
        if legacy_column:
            row[legacy_column] = str(source_id) if self._is_object_id(source_id) else source_id
        fk_columns = {column for fk in root.get("foreign_keys", []) for column in fk["columns"]}
        for column in root.get("columns", []):
            name = column["name"]
            if name in {root.get("id_column"), legacy_column} or column.get("auto_increment"):
                continue
            if name == "row_version":
                row[name] = 1
                continue
            if name == "is_stub":
                row[name] = 0
                continue
            source_path = column.get("source_path", "")
            if name in fk_columns:
                row[name] = None
                continue
            if not source_path or source_path.startswith("$"):
                continue
            value = self._get_path(document, source_path)
            row[name] = self._to_sql(value, column.get("sql_type", ""))
        return row

    def _side_tables(self, root: dict[str, Any]) -> list[dict[str, Any]]:
        return [table for table in self.plan.tables if table.get("role") == "side" and table.get("collection") == root.get("collection") and table.get("parent_table") == root.get("name")]

    def _build_side_row(self, table: dict[str, Any], document: dict[str, Any], parent_pk: Any) -> dict[str, Any] | None:
        value = self._get_path(document, table.get("source_path", ""))
        if not isinstance(value, dict):
            return None
        row: dict[str, Any] = {}
        parent_column = table.get("id_column")
        if parent_column:
            row[parent_column] = parent_pk
        prefix = table.get("source_path", "") + "."
        for column in table.get("columns", []):
            name = column["name"]
            if name == parent_column or name in table.get("primary_key", []):
                continue
            source_path = column.get("source_path", "")
            relative = source_path[len(prefix):] if source_path.startswith(prefix) else source_path
            row[name] = self._to_sql(self._get_path(value, relative), column.get("sql_type", ""))
        return row

    def _update_root_references(self, connection: Any, root: dict[str, Any], document: dict[str, Any], parent_pk: Any, unit: str) -> None:
        from sqlalchemy import text
        updates: dict[str, Any] = {}
        for fk in root.get("foreign_keys", []):
            target_table = self.tables.get(fk["target_table"])
            if target_table is None or target_table.get("role") == "system":
                continue
            if target_table.get("name") == root.get("name"):
                continue
            if fk["target_table"] == root.get("name"):
                continue
            for local_col in fk["columns"]:
                colspec = next((column for column in root["columns"] if column["name"] == local_col), None)
                if not colspec:
                    continue
                oid = self._get_path(document, colspec.get("source_path", ""))
                if oid is None:
                    updates[local_col] = None
                    continue
                sql_id = self._resolve_reference(connection, target_table, oid, unit, "phase2", document)
                updates[local_col] = sql_id
        if not updates:
            return
        qtable = connection.dialect.identifier_preparer.quote(root["name"])
        qid = connection.dialect.identifier_preparer.quote(root["id_column"])
        assignments = ", ".join(f"{connection.dialect.identifier_preparer.quote(name)}=:{name}" for name in updates)
        params = dict(updates)
        params["_parent_pk"] = parent_pk
        connection.execute(text(f"UPDATE {qtable} SET {assignments} WHERE {qid} = :_parent_pk"), params)

    def _process_junctions(self, connection: Any, root: dict[str, Any], document: dict[str, Any], parent_pk: Any, unit: str) -> None:
        junctions = [table for table in self.plan.tables if table.get("role") == "junction" and table.get("collection") == root.get("collection") and table.get("parent_table") == root.get("name")]
        for table in junctions:
            array = self._get_path(document, table.get("source_path", ""))
            if not isinstance(array, list):
                continue
            fk = next((item for item in table.get("foreign_keys", []) if item["target_table"] != root["name"]), None)
            if fk is None:
                continue
            target = self.tables.get(fk["target_table"])
            if target is None:
                continue
            parent_local = next((item for item in table.get("foreign_keys", []) if item["target_table"] == root["name"]), None)
            if parent_local is None:
                continue
            parent_column = parent_local["columns"][0]
            target_column = fk["columns"][0]
            rows: list[dict[str, Any]] = []
            seen: set[bytes] = set()
            for position, oid in enumerate(array):
                if oid is None:
                    continue
                encoded = self.idmap.get(target.get("collection", ""), str(oid))
                if encoded is None:
                    self._orphan(target, oid, unit, "phase2", document, "target for junction reference was not loaded")
                    continue
                if encoded in seen:
                    self._dedup_count += 1
                    continue
                seen.add(encoded)
                value = self._unpack_sql_value(encoded, self._column_type(target, target["id_column"]))
                row = {parent_column: parent_pk, target_column: value}
                if any(column["name"] == "position" for column in table["columns"]):
                    row["position"] = position
                rows.append(row)
            self._safe_upsert(connection, table, [{"row": row, "source_key": [self._encode_mongo_key(document.get("_id"))], "payload": document} for row in rows], "phase2")

    def _process_child_arrays(self, connection: Any, owner: dict[str, Any], source_value: Any, owner_pk: Any, unit: str, *, current_path: str) -> None:
        children = [table for table in self.plan.tables if table.get("role") == "child" and table.get("collection") == owner.get("collection") and table.get("parent_table") == owner.get("name")]
        for table in sorted(children, key=lambda item: (item.get("source_path", "").count("."), item.get("source_path", ""))):
            array_path = table.get("source_path", "")
            relative = array_path
            prefix = current_path + "." if current_path else ""
            if prefix and relative.startswith(prefix):
                relative = relative[len(prefix):]
            relative = relative.replace("[]", "")
            array_value = self._get_path(source_value, relative)
            if not isinstance(array_value, list):
                continue
            if len(array_value) > self.max_children:
                self.dlq.append(unit=unit, stage="phase2", source_key=[self._encode_mongo_key(source_value.get("_id"))] if isinstance(source_value, dict) else None,
                                error_code="BSON_TOO_LARGE", error=f"array {array_path} has {len(array_value)} elements; limit is {self.max_children}",
                                hint="reduce embedding/unnesting batch or normalize as an external collection", payload=source_value if isinstance(source_value, dict) else None)
                self._row_errors += 1
                continue
            parent_column = table.get("id_column_parent")
            row_records: list[dict[str, Any]] = []
            owner_id_column = owner.get("id_column")
            child_ids: list[Any] = []
            for position, element in enumerate(array_value):
                row: dict[str, Any] = {}
                for column in table.get("columns", []):
                    name = column["name"]
                    if name == table.get("id_column") or column.get("auto_increment"):
                        continue
                    if name == parent_column:
                        row[name] = owner_pk
                        continue
                    if name == "position":
                        row[name] = position
                        continue
                    source_path = column.get("source_path", "")
                    array_prefix = array_path + "[]."
                    if source_path == array_path + "[]" or name == "value" and source_path == array_path:
                        value = element
                    elif source_path.startswith(array_prefix):
                        value = self._get_path(element, source_path[len(array_prefix):])
                    else:
                        value = self._get_path(element, source_path)
                    row[name] = self._to_sql(value, column.get("sql_type", ""))
                self._resolve_child_references(connection, table, row, element, unit)
                source_key = [self._encode_mongo_key(source_value.get("_id"))] if isinstance(source_value, dict) else None
                row_records.append({"row": row, "source_key": source_key, "payload": element, "position": position})
            succeeded = self._safe_upsert(connection, table, row_records, "phase2")
            child_pk_by_position = self._readback_child_ids(connection, table, succeeded, owner_pk, parent_column)
            nested_tables = [item for item in self.plan.tables if item.get("role") in {"child", "junction"} and item.get("parent_table") == table.get("name")]
            if nested_tables:
                for record in succeeded:
                    child_pk = child_pk_by_position.get(int(record["position"]))
                    if child_pk is None:
                        continue
                    child_source_path = array_path + "[]"
                    self._process_child_arrays(connection, table, record["payload"], child_pk, unit, current_path=child_source_path)
                    # Nested junctions are handled with a synthetic root-like context below.
                    self._process_nested_junctions(connection, table, record["payload"], child_pk, unit)

    def _process_nested_junctions(self, connection: Any, owner: dict[str, Any], source_value: Any, owner_pk: Any, unit: str) -> None:
        for table in self.plan.tables:
            if table.get("role") != "junction" or table.get("collection") != owner.get("collection") or table.get("parent_table") != owner.get("name"):
                continue
            relative = table.get("source_path", "")
            if relative.startswith(owner.get("source_path", "") + "[]."):
                relative = relative[len(owner.get("source_path", "") + "[]."):]
            array = self._get_path(source_value, relative)
            if not isinstance(array, list):
                continue
            parent_fk = next((item for item in table.get("foreign_keys", []) if item["target_table"] == owner["name"]), None)
            target_fk = next((item for item in table.get("foreign_keys", []) if item["target_table"] != owner["name"]), None)
            if not parent_fk or not target_fk:
                continue
            target = self.tables.get(target_fk["target_table"])
            if not target:
                continue
            rows = []
            for value in array:
                encoded = self.idmap.get(target.get("collection", ""), str(value)) if value is not None else None
                if encoded is None:
                    self._orphan(target, value, unit, "phase2", source_value, "nested junction reference was not loaded")
                    continue
                row = {parent_fk["columns"][0]: owner_pk,
                       target_fk["columns"][0]: self._unpack_sql_value(encoded, self._column_type(target, target["id_column"]))}
                rows.append({"row": row, "source_key": None, "payload": source_value})
            self._safe_upsert(connection, table, rows, "phase2")

    def _resolve_child_references(self, connection: Any, table: dict[str, Any], row: dict[str, Any], element: Any, unit: str) -> None:
        for fk in table.get("foreign_keys", []):
            target = self.tables.get(fk["target_table"])
            if target is None or target["name"] == table.get("parent_table"):
                continue
            for local_column in fk["columns"]:
                spec = next((col for col in table["columns"] if col["name"] == local_column), None)
                source_path = spec.get("source_path", "") if spec else ""
                relative = source_path.split("[].", 1)[-1]
                oid = self._get_path(element, relative)
                if oid is None:
                    row[local_column] = None
                    continue
                row[local_column] = self._resolve_reference(connection, target, oid, unit, "phase2", element)

    def _resolve_reference(self, connection: Any, target: dict[str, Any], oid: Any, unit: str, stage: str, payload: Any) -> Any:
        if oid is None:
            return None
        collection = target.get("collection", "")
        encoded = self.idmap.get(collection, str(oid))
        if encoded is not None:
            return self._unpack_sql_value(encoded, self._column_type(target, target["id_column"]))
        if self.plan.options.get("reverse_idmap_table"):
            mapped_id = self._lookup_reverse_id(connection, collection, oid)
            if mapped_id is not None:
                self.idmap.put(collection, str(oid), self._pack_sql_value(mapped_id), materialized=True)
                return mapped_id
        self._orphan(target, oid, unit, stage, payload, "referenced document was not loaded in phase 1")
        if self.on_orphan == "NULL":
            return None
        if self.on_orphan == "STUB":
            return self._insert_stub(connection, target, oid)
        return None

    def _orphan(self, target: dict[str, Any], oid: Any, unit: str, stage: str, payload: Any, message: str) -> None:
        self.dlq.append(unit=unit, stage=stage, source_key=[self._encode_mongo_key(payload.get("_id"))] if isinstance(payload, dict) and "_id" in payload else None,
                        error_code="ORPHAN_REFERENCE", error=f"{message}: {target.get('collection')}/{oid}",
                        hint=f"on_orphan={self.on_orphan}", payload=payload)
        self._row_errors += 1

    def _insert_stub(self, connection: Any, target: dict[str, Any], oid: Any) -> Any | None:
        if not any(column["name"] == "is_stub" for column in target.get("columns", [])):
            return None
        legacy_column = target.get("legacy_id_column")
        row = {legacy_column: str(oid)} if legacy_column else {}
        row["is_stub"] = 1
        for column in target.get("columns", []):
            if column["name"] in row or column.get("nullable") or column["name"] == target.get("id_column"):
                continue
            raw = column.get("sql_type", "").upper()
            if "CHAR" in raw or "TEXT" in raw: row[column["name"]] = ""
            elif "JSON" in raw: row[column["name"]] = "{}"
            elif any(kind in raw for kind in ("INT", "DECIMAL", "NUMERIC", "DOUBLE")): row[column["name"]] = 0
            else: row[column["name"]] = None
        if not legacy_column:
            return self._upsert_root_identity(connection, target, row, oid)
        records = self._safe_upsert(connection, target, [{"row": row, "source_key": None, "payload": None}], "phase2")
        if not records:
            return None
        found = self._readback_ids(connection, target, records)
        return next(iter(found.values()), None)

    def _readback_ids(self, connection: Any, table: dict[str, Any], records: list[dict[str, Any]]) -> dict[str, Any]:
        if not records:
            return {}
        from sqlalchemy import text
        legacy = table.get("legacy_id_column")
        id_col = table.get("id_column")
        if not legacy or not id_col:
            return {}
        legacy_values = [record["row"].get(legacy) for record in records if legacy in record["row"]]
        unique_values = list(dict.fromkeys(legacy_values))
        result: dict[str, Any] = {}
        for offset in range(0, len(unique_values), 500):
            chunk = unique_values[offset:offset + 500]
            binds = {f"v{index}": value for index, value in enumerate(chunk)}
            placeholders = ",".join(f":v{index}" for index in range(len(chunk)))
            qtable = connection.dialect.identifier_preparer.quote(table["name"])
            qid = connection.dialect.identifier_preparer.quote(id_col)
            qlegacy = connection.dialect.identifier_preparer.quote(legacy)
            rows = connection.execute(text(f"SELECT {qid},{qlegacy} FROM {qtable} WHERE {qlegacy} IN ({placeholders})"), binds).fetchall()
            for row in rows:
                result[self._legacy_value_key(row[1])] = row[0]
        return result

    def _readback_child_ids(self, connection: Any, table: dict[str, Any], records: list[dict[str, Any]], owner_pk: Any, parent_column: str) -> dict[int, Any]:
        if not records or not parent_column:
            return {}
        from sqlalchemy import text
        qtable = connection.dialect.identifier_preparer.quote(table["name"])
        qid = connection.dialect.identifier_preparer.quote(table["id_column"])
        qparent = connection.dialect.identifier_preparer.quote(parent_column)
        qpos = connection.dialect.identifier_preparer.quote("position")
        rows = connection.execute(text(f"SELECT {qpos},{qid} FROM {qtable} WHERE {qparent}=:parent ORDER BY {qpos}"), {"parent": owner_pk}).fetchall()
        return {int(row[0]): row[1] for row in rows}

    def _safe_upsert(self, connection: Any, table: dict[str, Any], records: list[dict[str, Any]], stage: str) -> list[dict[str, Any]]:
        if not records:
            return []
        result: list[dict[str, Any]] = []
        savepoint = connection.begin_nested()
        try:
            self._execute_upsert(connection, table, [record["row"] for record in records])
            savepoint.commit()
            return records
        except BaseException as exc:
            try:
                if savepoint.is_active: savepoint.rollback()
            except Exception:
                pass
            if len(records) > 1:
                middle = len(records) // 2
                result.extend(self._safe_upsert(connection, table, records[:middle], stage))
                result.extend(self._safe_upsert(connection, table, records[middle:], stage))
                return result
            record = records[0]
            code = error_code_for(exc)
            if code == "DRIFT_VALUE_TOO_LONG" and self.on_drift == "widen":
                drift = self._detect_widening(table, record["row"], exc)
                if drift is not None:
                    column, next_type, value = drift
                    raise DriftDetected(table, column, next_type, value, exc)
            if code == "DRIFT_VALUE_TOO_LONG" and self.on_drift == "abort":
                raise RuntimeError(f"schema drift aborted migration: {exc}") from exc
            if code == "UNKNOWN": code = "SQL_CONSTRAINT"
            self.dlq.append(unit=table.get("collection", "unknown"), stage=stage, source_key=record.get("source_key"),
                            error_code=code, error=str(exc), attempt=1,
                            hint="batch bisection isolated this source record", payload=record.get("payload"))
            self._row_errors += 1
            return []

    def _execute_upsert(self, connection: Any, table: dict[str, Any], rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        from sqlalchemy import text
        dialect = connection.dialect.name
        table_name = connection.dialect.identifier_preparer.quote(table["name"])
        available = {column["name"]: column for column in table.get("columns", [])}
        columns = [name for name in rows[0] if name in available and not available[name].get("auto_increment") and name != table.get("id_column")]
        if not columns:
            raise ValueError(f"no insertable columns for target table {table['name']}")
        values_sql: list[str] = []
        params: dict[str, Any] = {}
        for row_index, row in enumerate(rows):
            placeholders = []
            for column_index, column in enumerate(columns):
                bind = f"v_{row_index}_{column_index}"
                placeholders.append(f":{bind}")
                params[bind] = row.get(column)
            values_sql.append("(" + ",".join(placeholders) + ")")
        qcolumns = ", ".join(connection.dialect.identifier_preparer.quote(column) for column in columns)
        insert = f"INSERT INTO {table_name} ({qcolumns}) VALUES {', '.join(values_sql)}"
        conflict = self._conflict_columns(table, columns)
        updates = [column for column in columns if column not in conflict]
        if dialect == "mysql":
            if updates:
                update_sql = ", ".join(f"{connection.dialect.identifier_preparer.quote(column)}=VALUES({connection.dialect.identifier_preparer.quote(column)})" for column in updates)
                sql = insert + " ON DUPLICATE KEY UPDATE " + update_sql
            else:
                first = columns[0]
                sql = insert + f" ON DUPLICATE KEY UPDATE {connection.dialect.identifier_preparer.quote(first)}=VALUES({connection.dialect.identifier_preparer.quote(first)})"
        else:
            qconflict = ", ".join(connection.dialect.identifier_preparer.quote(column) for column in conflict)
            if updates:
                update_sql = ", ".join(f"{connection.dialect.identifier_preparer.quote(column)}=EXCLUDED.{connection.dialect.identifier_preparer.quote(column)}" for column in updates)
                sql = insert + f" ON CONFLICT ({qconflict}) DO UPDATE SET {update_sql}"
            else:
                sql = insert + f" ON CONFLICT ({qconflict}) DO NOTHING"
        connection.execute(text(sql), params)

    @staticmethod
    def _conflict_columns(table: dict[str, Any], insert_columns: list[str]) -> list[str]:
        legacy = table.get("legacy_id_column")
        if legacy and legacy in insert_columns:
            return [legacy]
        for unique in table.get("unique_constraints", []):
            columns = list(unique.get("columns", []))
            if columns and set(columns) <= set(insert_columns):
                return columns
        pk = [column for column in table.get("primary_key", []) if column in insert_columns]
        if pk:
            return pk
        return [insert_columns[0]]

    def _build_child_row(self, connection: Any, table: dict[str, Any], element: Any, array_path: str, parent_pk: Any, position: int) -> dict[str, Any]:
        row: dict[str, Any] = {}
        parent_column = table.get("id_column_parent")
        for column in table.get("columns", []):
            name = column["name"]
            if name == table.get("id_column") or column.get("auto_increment"):
                continue
            if name == parent_column:
                row[name] = parent_pk
            elif name == "position":
                row[name] = position
            else:
                source_path = column.get("source_path", "")
                prefix = array_path + "[]."
                if source_path == array_path + "[]" or name == "value" and source_path == array_path:
                    value = element
                elif source_path.startswith(prefix):
                    value = self._get_path(element, source_path[len(prefix):])
                else:
                    value = self._get_path(element, source_path)
                row[name] = self._to_sql(value, column.get("sql_type", ""))
        self._resolve_child_references(connection, table, row, element, table.get("collection", ""))
        return row

    def _column_type(self, table: dict[str, Any], column: str) -> str:
        return next((item.get("sql_type", "") for item in table.get("columns", []) if item.get("name") == column), "")

    @staticmethod
    def _legacy_value_key(value: Any) -> str:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)

    @staticmethod
    def _pack_sql_value(value: Any) -> bytes:
        if isinstance(value, bool): return b"b" + (b"\x01" if value else b"\x00")
        if isinstance(value, int): return b"i" + value.to_bytes(16, "big", signed=True)
        if isinstance(value, str): return b"s" + value.encode("utf-8")
        if isinstance(value, bytes): return b"y" + value
        if isinstance(value, decimal.Decimal): return b"d" + str(value).encode("ascii")
        return b"j" + json.dumps(value, sort_keys=True, default=str).encode("utf-8")

    @staticmethod
    def _unpack_sql_value(value: bytes, sql_type: str) -> Any:
        if not value: return None
        tag, payload = value[:1], value[1:]
        if tag == b"i": return int.from_bytes(payload, "big", signed=True)
        if tag == b"s": return payload.decode("utf-8")
        if tag == b"y": return payload
        if tag == b"d": return decimal.Decimal(payload.decode("ascii"))
        if tag == b"b": return payload == b"\x01"
        if tag == b"j": return json.loads(payload.decode("utf-8"))
        # Compatibility with earlier ID maps that stored raw signed 64-bit integers.
        if len(value) in {4, 8, 16} and "INT" in sql_type.upper():
            return int.from_bytes(value, "big", signed=True)
        return value

    @staticmethod
    def _encode_mongo_key(value: Any) -> Any:
        if value is None: return None
        if ReverseETLRunner._is_object_id(value): return {"$oid": str(value)}
        if isinstance(value, dt.datetime): return {"$date": value.isoformat()}
        if isinstance(value, (bytes, bytearray)): return {"$binary": bytes(value).hex()}
        if isinstance(value, decimal.Decimal): return {"$decimal": str(value)}
        return value

    @staticmethod
    def _decode_mongo_key(value: Any, root: dict[str, Any]) -> Any:
        if isinstance(value, dict):
            if "$oid" in value:
                from bson import ObjectId
                return ObjectId(value["$oid"])
            if "$date" in value:
                return dt.datetime.fromisoformat(value["$date"])
            if "$binary" in value:
                return bytes.fromhex(value["$binary"])
            if "$decimal" in value:
                return decimal.Decimal(value["$decimal"])
        if isinstance(value, str) and any(item.get("bson_type") == "objectId" for item in root.get("id_types", [])):
            from bson import ObjectId
            try: return ObjectId(value)
            except Exception: return value
        return value

    @staticmethod
    def _is_object_id(value: Any) -> bool:
        return type(value).__name__.lower() == "objectid"

    @staticmethod
    def _get_path(value: Any, path: str) -> Any:
        if not path:
            return value
        current = value
        for segment in path.split("."):
            segment = segment.replace("[]", "")
            if isinstance(current, dict):
                current = current.get(segment)
            else:
                return None
        return current

    @staticmethod
    def _to_sql(value: Any, sql_type: str) -> Any:
        if value is None:
            return None
        if type(value).__name__.lower() == "decimal128":
            return value.to_decimal()
        if ReverseETLRunner._is_object_id(value):
            return str(value)
        if isinstance(value, dt.datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=dt.timezone.utc)
            return value.astimezone(dt.timezone.utc).replace(tzinfo=None)
        if isinstance(value, decimal.Decimal):
            return value
        if isinstance(value, (dict, list)):
            return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
        if isinstance(value, (bytes, bytearray, memoryview)):
            return bytes(value)
        if "VARCHAR" in sql_type.upper() or "TEXT" in sql_type.upper():
            return str(value) if not isinstance(value, str) else value
        return value

    def _init_widening_index(self) -> None:
        with self.checkpoints._lock:
            self.checkpoints.db.execute("CREATE TABLE IF NOT EXISTS etl_widening (table_name TEXT NOT NULL, column_name TEXT NOT NULL, widening_count INTEGER NOT NULL, before_type TEXT NOT NULL, after_type TEXT NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY(table_name,column_name,widening_count)) WITHOUT ROWID")
            rows = self.checkpoints.db.execute("SELECT table_name,column_name,widening_count,before_type,after_type FROM etl_widening ORDER BY table_name,column_name,widening_count").fetchall()
        for table_name, column_name, _count, before_type, after_type in rows:
            table = self.tables.get(table_name)
            if table:
                column = next((item for item in table["columns"] if item["name"] == column_name), None)
                if column:
                    column["sql_type"] = after_type
            self._drift_changes.append({"table": table_name, "column": column_name, "from": before_type, "to": after_type})

    def _detect_widening(self, table: dict[str, Any], row: dict[str, Any], exc: BaseException) -> tuple[dict[str, Any], str, Any] | None:
        import re
        message = str(exc)
        match = re.search(r"column\s+['`\"]?([A-Za-z0-9_]+)", message, re.I)
        candidates = [item for item in table.get("columns", []) if item["name"] in row]
        if match:
            selected = next((item for item in candidates if item["name"] == match.group(1)), None)
            candidates = [selected] if selected else candidates
        for column in candidates:
            value = row.get(column["name"])
            target_type = sql_widening(column.get("sql_type", ""), value, dialect="mysql" if self.target.dialect.name == "mysql" else "postgresql")
            if target_type:
                return column, target_type, value
        return None

    def _apply_widening(self, drift: DriftDetected) -> None:
        table_name = drift.table["name"]
        column_name = drift.column["name"]
        with self.checkpoints._lock:
            current = int(self.checkpoints.db.execute("SELECT COALESCE(MAX(widening_count),0) FROM etl_widening WHERE table_name=? AND column_name=?", (table_name, column_name)).fetchone()[0])
        maximum = int(self.options.get("max_widenings", self.plan.thresholds.max_widenings))
        if current >= maximum:
            raise RuntimeError(f"max_widenings={maximum} exceeded for {table_name}.{column_name}") from drift.original
        old_type = drift.column["sql_type"]
        from sqlalchemy import text
        with self._connect() as connection:
            qtable = connection.dialect.identifier_preparer.quote(table_name)
            qcolumn = connection.dialect.identifier_preparer.quote(column_name)
            if connection.dialect.name == "mysql":
                nullability = "NULL" if drift.column.get("nullable", True) else "NOT NULL"
                connection.execute(text(f"ALTER TABLE {qtable} MODIFY COLUMN {qcolumn} {drift.next_type} {nullability}"))
            else:
                connection.execute(text(f"ALTER TABLE {qtable} ALTER COLUMN {qcolumn} TYPE {drift.next_type}"))
        drift.column["sql_type"] = drift.next_type
        change = {"table": table_name, "column": column_name, "from": old_type, "to": drift.next_type}
        self._drift_changes.append(change)
        with self.checkpoints._lock:
            self.checkpoints.db.execute("INSERT INTO etl_widening(table_name,column_name,widening_count,before_type,after_type,created_at) VALUES(?,?,?,?,?,?)",
                                        (table_name, column_name, current + 1, old_type, drift.next_type, utc_iso()))

    def _source_id_types(self, collection_name: str) -> set[str]:
        for item in self.plan.source_ir.get("collections", []):
            if item.get("name") == collection_name:
                return {typ.get("bson_type") for typ in item.get("id_profile", {}).get("types", [])}
        return set()
