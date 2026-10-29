"""Keyset SQL→MongoDB runner with bounded batches, lazy ID allocation, and idempotent upserts."""
from __future__ import annotations

import datetime as dt
import decimal
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Iterable

from synapse.adapters.idmap.base import IdMapStore
from synapse.domain.heuristics.thresholds import Thresholds
from synapse.domain.ir.common import ForwardMapping
from synapse.domain.ir.plan import ApprovedPlan, PlanUnit
from synapse.domain.ir.relational import Column, ForeignKey, Relationship, RelationalIR, Table
from synapse.domain.naming import quote_identifier, quote_qualified
from synapse.domain.typemap.sql_to_bson import to_bson_value
from synapse.etl.checkpoint import Checkpoint, CheckpointManager, LeaseHeartbeat, utc_iso
from synapse.etl.dlq import DeadLetterLog, error_code_for
from synapse.etl.keys import keyset_select_sql
from synapse.etl.runtime import AIMDController


class NoStableKeyError(RuntimeError):
    pass


class ForwardETLRunner:
    def __init__(self, plan: ApprovedPlan, source: Any, target_database: Any, idmap: IdMapStore,
                 checkpoints: CheckpointManager, dlq: DeadLetterLog, *, options: dict[str, Any] | None = None,
                 lease: LeaseHeartbeat | None = None):
        if not plan.verify_hash():
            raise ValueError("Approved Plan hash is invalid")
        self.plan = plan
        self.source = source
        self.target = target_database
        self.idmap = idmap
        self.checkpoints = checkpoints
        self.dlq = dlq
        self.options = {**plan.etl_options, **(options or {})}
        self.lease = lease
        self.thresholds = plan.thresholds
        self.ir = RelationalIR.model_validate({"tables": plan.source_ir.get("tables", []),
                                               "relationships": plan.source_ir.get("relationships", []),
                                               "workload": plan.source_ir.get("workload")})
        self.tables = {table.qualified_name: table for table in self.ir.tables}
        self.tables.update({table.table_name: table for table in self.ir.tables})
        self.relationships = self.ir.relationships
        self.source_tz = str(self.options.get("source_tz", "UTC"))
        self.double_for_decimal = bool(self.options.get("double_for_decimal", False))
        self.batch_size = int(self.options.get("batch_size", 2000))
        self.memory_budget_mb = int(self.options.get("memory_budget_mb", 512))
        self.byte_limit = int(self.options.get("mongo_batch_bytes", 8 * 1024 * 1024))
        self.max_children = int(self.options.get("max_children_per_parent", self.thresholds.max_children_per_parent))

    def run(self) -> dict[str, Any]:
        if self.lease:
            self.lease.assert_current()
        units = list(self.plan.units)
        if not units:
            units = self._derive_units()
        concurrency = max(1, int(self.options.get("unit_concurrency", 1)))
        if concurrency == 1 or not hasattr(self.source, "connect"):
            results = [self.run_unit(unit) for unit in units]
        else:
            results = []
            with ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix="synapse-forward-unit") as pool:
                futures = {pool.submit(self.run_unit, unit): unit for unit in units}
                for future in as_completed(futures):
                    results.append(future.result())
        final_status = next((item["status"] for item in results if item["status"] in {"paused", "cancelled"}), None)
        return {"status": final_status or ("completed_with_errors" if any(item["errors"] for item in results) else "completed"),
                "units": sorted(results, key=lambda item: item["unit_id"]),
                "rows_done": sum(item["rows_done"] for item in results),
                "docs_written": sum(item["docs_written"] for item in results),
                "errors": sum(item["errors"] for item in results)}

    def _derive_units(self) -> list[PlanUnit]:
        embedded_children = {rel.child_table for rel in self.relationships if rel.effective_mapping in {ForwardMapping.EMBED, ForwardMapping.EMBED_ARRAY}}
        junction_children = {rel.child_table for rel in self.relationships if rel.rule_id == "H2" and rel.effective_mapping == ForwardMapping.REFERENCE_ARRAY}
        roots = [table for table in self.ir.tables if table.table_name not in embedded_children and table.qualified_name not in embedded_children and table.table_name not in junction_children and table.qualified_name not in junction_children]
        return [PlanUnit(unit_id=table.table_name, root=table.qualified_name, key_columns=tuple(table.primary_key.columns) if table.primary_key else ()) for table in roots]

    def _resolve_table(self, name: str) -> Table:
        table = self.tables.get(name)
        if table is None:
            table = self.tables.get(name.split(".")[-1])
        if table is None:
            raise KeyError(f"source table {name!r} not found in immutable plan")
        return table

    def _connect(self):
        from contextlib import nullcontext
        if hasattr(self.source, "connect"):
            return self.source.connect()
        return nullcontext(self.source)

    def run_unit(self, unit: PlanUnit | dict[str, Any]) -> dict[str, Any]:
        unit = unit if isinstance(unit, PlanUnit) else PlanUnit.model_validate(unit)
        table = self._resolve_table(unit.root)
        key_columns = list(unit.key_columns or (table.primary_key.columns if table.primary_key else ()))
        if not key_columns:
            raise NoStableKeyError(f"NO_STABLE_KEY: {table.qualified_name} has no primary key or key_hint")
        missing = set(key_columns) - {column.name for column in table.columns}
        if missing:
            raise ValueError(f"key_hint references missing columns: {sorted(missing)}")
        stage = "forward"
        checkpoint = self.checkpoints.load(unit.unit_id, stage, plan_hash=self.plan.plan_hash)
        last_key = checkpoint.last_key if checkpoint else None
        batch_seq = checkpoint.batch_seq if checkpoint else 0
        rows_done = checkpoint.rows_done if checkpoint else 0
        docs_written = checkpoint.docs_written or 0 if checkpoint else 0
        errors = checkpoint.errors if checkpoint else 0
        controller = AIMDController(self.batch_size, memory_budget_bytes=self.memory_budget_mb * 1024 * 1024,
                                    pipeline_depth=int(self.options.get("max_inflight_batches", 2)))
        stopped_status: str | None = None
        with self._connect() as connection:
            while True:
                if self.lease: self.lease.assert_current()
                page, query_last = self._fetch_page(connection, table, key_columns, last_key, controller.batch_size)
                if not page:
                    break
                batch_started = time.monotonic()
                operations: list[Any] = []
                source_keys: list[list[Any]] = []
                key_aliases: list[list[tuple[str, tuple[Any, ...]]]] = []
                documents: list[dict[str, Any]] = []
                for row in page:
                    source_key = [row[column] for column in key_columns]
                    try:
                        doc = self._build_document(connection, table, row, source_key, 0, set(), key_columns=key_columns)
                        if doc is None:
                            errors += 1
                            continue
                        size = self._bson_size(doc)
                        if size > self.thresholds.max_document_bytes:
                            self.dlq.append(unit=unit.unit_id, stage=stage, source_key=source_key, error_code="BSON_TOO_LARGE",
                                            error=f"document {size} bytes exceeds MongoDB 16 MiB limit", hint="switch the oversized relationship to REFERENCE")
                            errors += 1
                            continue
                        documents.append(doc)
                        source_keys.append(source_key)
                        aliases = []
                        for relation in self.relationships:
                            if relation.parent_table not in {table.table_name, table.qualified_name}: continue
                            alias = tuple(row.get(column) for column in relation.referenced_columns)
                            if alias and all(value is not None for value in alias) and alias != tuple(source_key):
                                aliases.append((table.qualified_name, alias))
                        key_aliases.append(aliases)
                    except Exception as exc:
                        self.dlq.append(unit=unit.unit_id, stage=stage, source_key=source_key, error_code=error_code_for(exc),
                                        error=str(exc), hint="inspect the source value and frozen field mapping", payload={"row": dict(row)})
                        errors += 1
                written = self._write_documents(unit.unit_id, documents, source_keys, table, key_columns, stage, key_aliases=key_aliases)
                docs_written += written
                rows_done += len(page)
                errors += len(documents) - written
                batch_seq += 1
                last_key = [page[-1][column] for column in key_columns]
                cp = Checkpoint(self.checkpoints.job_id, unit.unit_id, stage, last_key, batch_seq, rows_done,
                                docs_written, errors, self.plan.plan_hash, self.lease.lease.fence if self.lease else 0,
                                utc_iso(), "running")
                # Target writes are committed before this forward checkpoint; replay is safe via stable _id upserts.
                self.checkpoints.commit_forward(cp)
                estimated_bytes = sum(self._bson_size(doc) for doc in documents)
                controller.observe(estimated_bytes)
                if self.options.get("on_progress"):
                    self.options["on_progress"]({"unit_id": unit.unit_id, "stage": stage, "rows_done": rows_done,
                                                  "docs_written": docs_written, "errors": errors,
                                                  "last_key": last_key, "batch_seq": batch_seq,
                                                  "batch_seconds": time.monotonic() - batch_started,
                                                  "batch_size": controller.batch_size})
                if self.options.get("stop_requested") and self.options["stop_requested"]():
                    stopped_status = str(self.options.get("stop_status", lambda: "paused")())
                    cp.status = stopped_status
                    self.checkpoints.commit_forward(cp)
                    break
        final_status = stopped_status or ("completed_with_errors" if errors else "completed")
        final = Checkpoint(self.checkpoints.job_id, unit.unit_id, stage, last_key, batch_seq, rows_done,
                           docs_written, errors, self.plan.plan_hash, self.lease.lease.fence if self.lease else 0,
                           utc_iso(), final_status)
        self.checkpoints.commit_forward(final)
        return {"unit_id": unit.unit_id, "rows_done": rows_done, "docs_written": docs_written,
                "errors": errors, "batch_seq": batch_seq, "last_key": last_key, "status": final_status}

    def _fetch_page(self, connection: Any, table: Table, key_columns: list[str], last_key: list[Any] | None, batch_size: int) -> tuple[list[dict[str, Any]], list[Any] | None]:
        from sqlalchemy import text
        dialect = connection.dialect.name
        sql, params = keyset_select_sql(table.qualified_name, key_columns, batch_size, last_values=last_key, dialect=dialect)
        result = connection.execute(text(sql), params).mappings()
        page = [dict(row) for row in result]
        return page, last_key

    def _build_document(self, connection: Any, table: Table, row: dict[str, Any], source_key: list[Any], depth: int, ancestors: set[str], *, embedded: bool = False, key_columns: list[str] | None = None) -> dict[str, Any] | None:
        if depth > self.thresholds.max_depth:
            raise ValueError("maximum relational embedding depth exceeded")
        table_name = table.qualified_name
        if table_name in ancestors:
            raise ValueError("embedding cycle escaped H8 cycle breaking")
        self._assert_lease()
        stable_columns = tuple(key_columns or (table.primary_key.columns if table.primary_key else ()))
        if not stable_columns and not embedded:
            raise NoStableKeyError(f"NO_STABLE_KEY: {table_name} has no primary key or key_hint")
        own_key = tuple(row[column] for column in stable_columns) if stable_columns else ()
        bson_id = self.idmap.get_or_allocate(table_name, own_key) if not embedded else None
        if bson_id is not None:
            # A key hint can differ from a referenced unique key. Store every declared FK target
            # tuple as an alias for this root's stable ObjectId before dependent rows are emitted.
            for incoming in self.relationships:
                if incoming.parent_table not in {table.table_name, table.qualified_name}:
                    continue
                alias = tuple(row.get(column) for column in incoming.referenced_columns)
                if any(value is None for value in alias) or alias == own_key:
                    continue
                prior = self.idmap.get(table_name, alias)
                if prior is not None and prior != bson_id:
                    raise ValueError(f"referenced unique key alias collision in {table_name}: {incoming.referenced_columns}")
                self.idmap.put(table_name, alias, bson_id, materialized=False)
        object_id = self._object_id(bson_id) if bson_id is not None else None
        outgoing = [rel for rel in self.relationships if rel.child_table in {table.table_name, table.qualified_name}]
        embedded_fk_columns = {column for rel in outgoing if rel.effective_mapping in {ForwardMapping.EMBED, ForwardMapping.EMBED_ARRAY} for column in rel.foreign_key}
        document: dict[str, Any] = {} if embedded else {"_id": object_id}
        for column in table.columns:
            if column.name in embedded_fk_columns:
                continue
            value = row.get(column.name)
            reference_rel = next((rel for rel in outgoing if rel.effective_mapping == ForwardMapping.REFERENCE and column.name in rel.foreign_key), None)
            reference_array_rel = next((rel for rel in outgoing if rel.effective_mapping == ForwardMapping.REFERENCE_ARRAY and column.name in rel.foreign_key), None)
            if reference_rel or reference_array_rel:
                continue
            document[column.name] = to_bson_value(value, column, source_tz=self.source_tz, double_for_decimal=self.double_for_decimal)
        # Store each non-embedded parent relation as a deterministic ObjectId reference.
        for rel in outgoing:
            if rel.effective_mapping not in {ForwardMapping.REFERENCE, ForwardMapping.REFERENCE_ARRAY}:
                continue
            fk_values = [row.get(column) for column in rel.foreign_key]
            if any(value is None for value in fk_values):
                continue
            parent_table = self._resolve_table(rel.parent_table)
            target_key = tuple(fk_values)
            target_id = self.idmap.get_or_allocate(parent_table.qualified_name, target_key)
            field_name = rel.foreign_key[0] if len(fk_values) == 1 else f"{parent_table.table_name}_ref"
            document[field_name] = self._object_id(target_id)
        if not embedded:
            document["_legacy"] = {"table": table.table_name, "pk": list(own_key)}
        elif self.options.get("preserve_child_legacy_ids", False) and own_key:
            document["_legacy"] = {"table": table.table_name, "pk": list(own_key)}

        # A pure M:N junction is represented as an array of stable target ObjectIds.
        for rel in sorted(self.relationships, key=lambda item: (item.child_table, item.rel_id)):
            if rel.parent_table not in {table.table_name, table.qualified_name} or rel.effective_mapping != ForwardMapping.REFERENCE_ARRAY:
                continue
            junction_table = self._resolve_table(rel.child_table)
            junction_rows = self._fetch_children(connection, junction_table, rel, row)
            other_edge = next((candidate for candidate in self.relationships
                               if candidate.child_table in {junction_table.table_name, junction_table.qualified_name}
                               and candidate.rel_id != rel.rel_id and candidate.rule_id == "H2"), None)
            if other_edge is None:
                continue
            target_table = self._resolve_table(other_edge.parent_table)
            references: list[Any] = []
            seen_ids: set[bytes] = set()
            for junction_row in junction_rows:
                target_key = tuple(junction_row.get(column) for column in other_edge.foreign_key)
                if any(value is None for value in target_key):
                    continue
                target_id = self.idmap.get_or_allocate(target_table.qualified_name, target_key)
                if target_id not in seen_ids:
                    references.append(self._object_id(target_id))
                    seen_ids.add(target_id)
            document[target_table.table_name] = references

        for rel in sorted(self.relationships, key=lambda item: (item.child_table, item.rel_id)):
            if rel.parent_table not in {table.table_name, table.qualified_name} or rel.effective_mapping not in {ForwardMapping.EMBED, ForwardMapping.EMBED_ARRAY}:
                continue
            child_table = self._resolve_table(rel.child_table)
            children = self._fetch_children(connection, child_table, rel, row)
            if len(children) > self.max_children:
                raise ValueError(f"BSON_TOO_LARGE: parent has more than max_children_per_parent={self.max_children}")
            child_docs: list[dict[str, Any]] = []
            for child in children:
                child_key = list(child_table.primary_key.columns) if child_table.primary_key else list(rel.foreign_key)
                child_source_key = [child.get(key) for key in child_key]
                child_doc = self._build_document(connection, child_table, child, child_source_key, depth + 1, ancestors | {table_name}, embedded=True)
                child_doc.pop("_id", None)
                child_doc.pop("_legacy", None)
                child_docs.append(child_doc)
            field_name = child_table.table_name
            if rel.effective_mapping == ForwardMapping.EMBED_ARRAY:
                document[field_name] = child_docs
            elif child_docs:
                if len(child_docs) > 1:
                    raise ValueError(f"1:1 EMBED relationship {rel.rel_id} returned multiple child rows")
                document[field_name] = child_docs[0]
        return document

    def _fetch_children(self, connection: Any, table: Table, relationship: Relationship, parent_row: dict[str, Any]) -> list[dict[str, Any]]:
        from sqlalchemy import text
        dialect = connection.dialect.name
        qtable = quote_qualified(table.schema_name, table.table_name, dialect)
        terms: list[str] = []
        params: dict[str, Any] = {"child_limit": self.max_children + 1}
        for index, (child_col, parent_col) in enumerate(zip(relationship.foreign_key, relationship.referenced_columns, strict=True)):
            key = f"fk_{index}"
            terms.append(f"{quote_identifier(child_col, dialect)} = :{key}")
            params[key] = parent_row.get(parent_col)
        if any(params[f"fk_{index}"] is None for index in range(len(relationship.foreign_key))):
            return []
        order_columns = list(table.primary_key.columns) if table.primary_key else list(relationship.foreign_key)
        order = ", ".join(quote_identifier(column, dialect) for column in order_columns)
        select_cols = ", ".join(quote_identifier(column.name, dialect) for column in table.columns) or "*"
        sql = f"SELECT {select_cols} FROM {qtable} WHERE {' AND '.join(terms)} ORDER BY {order} LIMIT :child_limit"
        result = connection.execute(text(sql), params).mappings()
        return [dict(row) for row in result.fetchmany(self.max_children + 1)]

    def _write_documents(self, unit_id: str, docs: list[dict[str, Any]], source_keys: list[list[Any]], table: Table, key_columns: list[str], stage: str,
                         *, key_aliases: list[list[tuple[str, tuple[Any, ...]]]] | None = None) -> int:
        if not docs:
            return 0
        if self.lease: self.lease.assert_current()
        try:
            from pymongo import ReplaceOne
        except ImportError as exc:
            raise RuntimeError("PyMongo is required for the forward ETL writer") from exc
        collection = self.target[table.table_name]
        operations = [ReplaceOne({"_id": doc["_id"]}, doc, upsert=True) for doc in docs]
        max_attempts = int(self.options.get("max_retries", 6))
        delay = 1.0
        for attempt in range(1, max_attempts + 1):
            try:
                collection.bulk_write(operations, ordered=False, bypass_document_validation=False)
                for index, key in enumerate(source_keys):
                    self.idmap.materialize(table.qualified_name, tuple(key))
                    for namespace, alias in (key_aliases[index] if key_aliases and index < len(key_aliases) else []):
                        self.idmap.materialize(namespace, alias)
                return len(docs)
            except Exception as exc:
                details = getattr(exc, "details", None)
                if isinstance(details, dict) and details.get("writeErrors") is not None:
                    bad_indexes = {int(item.get("index", -1)): item for item in details["writeErrors"]}
                    for index, item in enumerate(docs):
                        if index in bad_indexes:
                            write_error = bad_indexes[index]
                            code = error_code_for(type("MongoWriteError", (Exception,), {"code": write_error.get("code")})())
                            self.dlq.append(unit=unit_id, stage=stage, source_key=source_keys[index], error_code=code,
                                            error=write_error.get("errmsg", str(write_error)), attempt=attempt,
                                            hint="repair the source row or adjust the generated MongoDB validator")
                        else:
                            self.idmap.materialize(table.qualified_name, tuple(source_keys[index]))
                            for namespace, alias in (key_aliases[index] if key_aliases and index < len(key_aliases) else []):
                                self.idmap.materialize(namespace, alias)
                    return len(docs) - len(bad_indexes)
                code = error_code_for(exc)
                if code == "TRANSIENT_EXHAUSTED" and attempt < max_attempts:
                    if self.lease: self.lease.assert_current()
                    time.sleep(min(30.0, delay) * (0.75 + 0.5 * __import__("random").random()))
                    delay = min(30.0, delay * 2)
                    continue
                if code == "TRANSIENT_EXHAUSTED":
                    code = "TRANSIENT_EXHAUSTED"
                for key in source_keys:
                    self.dlq.append(unit=unit_id, stage=stage, source_key=key, error_code=code, error=str(exc), attempt=attempt,
                                    retryable=code == "TRANSIENT_EXHAUSTED")
                return 0
        return 0

    def _assert_lease(self) -> None:
        if self.lease: self.lease.assert_current()

    @staticmethod
    def _object_id(value: bytes) -> Any:
        try:
            from bson import ObjectId
            return ObjectId(value)
        except ImportError as exc:
            raise RuntimeError("PyMongo's bson package is required to create stable ObjectIds") from exc

    @staticmethod
    def _bson_size(document: dict[str, Any]) -> int:
        try:
            from bson import BSON
            return len(BSON.encode(document))
        except Exception:
            import json
            return len(json.dumps(document, default=str, separators=(",", ":")).encode("utf-8"))


ForwardRunner = ForwardETLRunner
