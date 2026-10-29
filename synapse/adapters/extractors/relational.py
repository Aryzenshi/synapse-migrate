"""SQLAlchemy Inspector extractor with composite keys and bounded catalog probes."""
from __future__ import annotations

import fnmatch
import math
import re
from typing import Any

from synapse.adapters.extractors.base import Extractor
from synapse.domain.heuristics.forward import estimate_fanout
from synapse.domain.heuristics.thresholds import Thresholds
from synapse.domain.ir.common import CanonicalType, WorkloadProfile
from synapse.domain.ir.relational import CheckConstraint, Column, FanoutStats, ForeignKey, Index, PrimaryKey, RelationalIR, Table, TableStats, UniqueConstraint
from synapse.domain.typemap.sql_to_bson import map_sql_type, parse_sql_type


def _canonical(raw_type: str, mapped: str | list[str]) -> CanonicalType:
    value = mapped[0] if isinstance(mapped, list) else mapped
    reverse = {
        "int": CanonicalType.INT, "long": CanonicalType.LONG, "decimal": CanonicalType.DECIMAL,
        "double": CanonicalType.DOUBLE, "string": CanonicalType.STRING, "bool": CanonicalType.BOOL,
        "date": CanonicalType.DATETIME, "object": CanonicalType.JSON, "array": CanonicalType.JSON,
        "binData": CanonicalType.BINARY,
    }
    name, _, _ = parse_sql_type(raw_type)
    if name in {"text", "tinytext", "mediumtext", "longtext", "clob"}:
        return CanonicalType.TEXT
    if name == "time":
        return CanonicalType.TIME
    if name in {"enum", "set"}:
        return CanonicalType.ENUM
    if name == "uuid":
        return CanonicalType.UUID
    return reverse.get(value, CanonicalType.UNKNOWN)


def _sql_type_metadata(raw_type: str) -> tuple[int | None, int | None, int | None, bool]:
    name, args, unsigned = parse_sql_type(raw_type)
    length = args[0] if name in {"char", "varchar", "binary", "varbinary"} and args else None
    precision = args[0] if name in {"decimal", "numeric", "number"} and args else None
    scale = args[1] if name in {"decimal", "numeric", "number"} and len(args) > 1 else None
    return length, precision, scale, unsigned


def _estimate_column_bytes(column: Column) -> int:
    if column.length is not None:
        return min(column.length, 16_383)
    if column.canonical_type == CanonicalType.BINARY:
        return column.length or 32
    if column.canonical_type in {CanonicalType.INT, CanonicalType.BOOL}:
        return 4
    if column.canonical_type in {CanonicalType.LONG, CanonicalType.DOUBLE, CanonicalType.DATETIME, CanonicalType.DATE}:
        return 8
    if column.canonical_type == CanonicalType.DECIMAL:
        return 16
    return 256


class RelationalExtractor(Extractor[RelationalIR]):
    def __init__(self, *, probe_limit: int = 5_000_000):
        self.probe_limit = probe_limit

    def extract(self, conn: Any, options: dict[str, Any] | None = None) -> RelationalIR:
        try:
            from sqlalchemy import inspect, text
        except ImportError as exc:
            raise RuntimeError("SQLAlchemy 2.x is required for relational introspection") from exc
        opts = options or {}
        inspector = inspect(conn)
        dialect = getattr(getattr(conn, "dialect", None), "name", "mysql")
        requested_schemas = opts.get("schemas") or [inspector.default_schema_name]
        if requested_schemas == [None]:
            requested_schemas = [None]
        include = opts.get("include_tables") or []
        exclude = opts.get("exclude_tables") or []
        table_specs: list[tuple[str | None, str]] = []
        for schema in requested_schemas:
            try:
                names = inspector.get_table_names(schema=schema)
            except Exception:
                if schema is None:
                    names = inspector.get_table_names()
                else:
                    raise
            for name in names:
                if include and not any(fnmatch.fnmatch(name, pattern) for pattern in include):
                    continue
                if any(fnmatch.fnmatch(name, pattern) for pattern in exclude):
                    continue
                if name.startswith("_synapse_"):
                    continue
                table_specs.append((schema, name))
        table_specs.sort(key=lambda item: ((item[0] or ""), item[1]))

        tables: list[Table] = []
        for schema, name in table_specs:
            raw_columns = inspector.get_columns(name, schema=schema)
            columns: list[Column] = []
            for ordinal, item in enumerate(raw_columns, 1):
                raw_type = str(item.get("type", "UNKNOWN"))
                mapped = map_sql_type(raw_type)
                length, precision, scale, unsigned = _sql_type_metadata(raw_type)
                enum_values = getattr(item.get("type"), "enums", None)
                columns.append(Column(
                    name=item["name"], sql_type=raw_type,
                    canonical_type=_canonical(raw_type, mapped.bson_type),
                    nullable=bool(item.get("nullable", True)), length=length, precision=precision, scale=scale,
                    unsigned=unsigned, auto_increment=bool(item.get("autoincrement", False) is True or item.get("identity")),
                    default=None if item.get("default") is None else str(item.get("default")),
                    enum_values=list(enum_values) if enum_values else None,
                    generated=bool(item.get("computed") or item.get("identity")), ordinal=ordinal,
                    comment=item.get("comment"),
                ))
            pk_info = inspector.get_pk_constraint(name, schema=schema) or {}
            pk_cols = list(pk_info.get("constrained_columns") or [])
            primary_key = PrimaryKey(name=pk_info.get("name"), columns=pk_cols) if pk_cols else None
            foreign_keys: list[ForeignKey] = []
            for fk in inspector.get_foreign_keys(name, schema=schema) or []:
                constrained = list(fk.get("constrained_columns") or [])
                referred = list(fk.get("referred_columns") or [])
                if not constrained or len(constrained) != len(referred):
                    continue
                ref_schema = fk.get("referred_schema")
                ref_table = f"{ref_schema}.{fk['referred_table']}" if ref_schema and ref_schema != schema else fk["referred_table"]
                options_fk = fk.get("options") or {}
                foreign_keys.append(ForeignKey(
                    name=fk.get("name"), columns=constrained, ref_table=ref_table, ref_columns=referred,
                    on_delete=str(options_fk.get("ondelete", "NO ACTION")).upper(),
                    on_update=str(options_fk.get("onupdate", "NO ACTION")).upper(),
                ))
            uniques = [UniqueConstraint(name=item.get("name"), columns=list(item.get("column_names") or []))
                       for item in (inspector.get_unique_constraints(name, schema=schema) or []) if item.get("column_names")]
            checks = [CheckConstraint(name=item.get("name"), sqltext=str(item.get("sqltext", "")))
                      for item in (inspector.get_check_constraints(name, schema=schema) or []) if item.get("sqltext")]
            indexes: list[Index] = []
            for item in inspector.get_indexes(name, schema=schema) or []:
                col_names = list(item.get("column_names") or [])
                if col_names and all(col is not None for col in col_names):
                    indexes.append(Index(name=item.get("name"), columns=col_names, unique=bool(item.get("unique", False)),
                                         method=item.get("dialect_options", {}).get("using"),
                                         where=item.get("dialect_options", {}).get("where"),
                                         dialect_options=dict(item.get("dialect_options", {}))))
            row_estimate, avg_row_bytes = self._table_estimates(conn, dialect, schema, name, columns, opts)
            tables.append(Table(schema_name=schema, table_name=name, primary_key=primary_key, columns=columns,
                                foreign_keys=foreign_keys, uniques=uniques, checks=checks, indexes=indexes,
                                stats=TableStats(row_estimate=row_estimate, avg_row_bytes=avg_row_bytes)))

        threshold = Thresholds.from_mapping(opts.get("thresholds"))
        tables_by_name = {table.qualified_name: table for table in tables}
        tables_by_name.update({table.table_name: table for table in tables})
        for child in tables:
            fanout_map: dict[str, FanoutStats] = {}
            for fk in child.foreign_keys:
                parent = tables_by_name.get(fk.ref_table)
                if parent is None:
                    continue
                key = fk.name or ",".join(fk.columns)
                if (opts.get("fanout") or {}).get("mode", "estimate") == "exact":
                    exact = self._exact_fanout(conn, dialect, child, fk)
                    if exact is not None:
                        fanout_map[key] = exact
                        continue
                fanout_map[key] = estimate_fanout(child, parent, threshold)
            child.stats = child.stats.model_copy(update={"fanout_by_fk": fanout_map})

        workload_opts = opts.get("workload") or {}
        profile_data = workload_opts.get("profile")
        workload = WorkloadProfile.model_validate({
            "tables": (profile_data or {}).get("tables", {}),
            "source": workload_opts.get("source", "profile"),
            "window_seconds": workload_opts.get("window_seconds"),
        }) if profile_data else None
        return RelationalIR(tables=tables, workload=workload)

    def _table_estimates(self, conn: Any, dialect: str, schema: str | None, name: str, columns: list[Column], options: dict[str, Any]) -> tuple[int, int]:
        try:
            from sqlalchemy import text
            prep = conn.dialect.identifier_preparer
            if dialect == "mysql":
                result = conn.execute(text("SELECT COALESCE(TABLE_ROWS,0), COALESCE(AVG_ROW_LENGTH,0) FROM information_schema.tables WHERE table_schema=:schema AND table_name=:table"),
                                      {"schema": schema or conn.engine.url.database, "table": name}).first()
                if result:
                    return max(0, int(result[0] or 0)), max(0, int(result[1] or 0)) or sum(_estimate_column_bytes(c) for c in columns)
            elif dialect == "postgresql":
                query = text("SELECT GREATEST(c.reltuples,0)::bigint, COALESCE(pg_relation_size(c.oid)/NULLIF(c.reltuples,0),0)::bigint FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relname=:table AND n.nspname=:schema")
                result = conn.execute(query, {"table": name, "schema": schema or "public"}).first()
                if result:
                    return max(0, int(result[0] or 0)), max(0, int(result[1] or 0)) or sum(_estimate_column_bytes(c) for c in columns)
            if options.get("exact_row_counts", False):
                qtable = prep.quote(name)
                if schema:
                    qtable = prep.quote_schema(schema) + "." + qtable
                count = conn.execute(text(f"SELECT COUNT(*) FROM {qtable}")).scalar_one()
                return int(count), sum(_estimate_column_bytes(c) for c in columns)
        except Exception:
            pass
        return 0, sum(_estimate_column_bytes(column) for column in columns)

    def _exact_fanout(self, conn: Any, dialect: str, child: Table, fk: ForeignKey) -> FanoutStats | None:
        try:
            from sqlalchemy import text
            prep = conn.dialect.identifier_preparer
            qtable = prep.quote(child.table_name)
            if child.schema_name:
                qtable = prep.quote_schema(child.schema_name) + "." + qtable
            qcols = ", ".join(prep.quote(column) for column in fk.columns)
            group_count_sql = f"SELECT COUNT(*) FROM (SELECT 1 FROM {qtable} GROUP BY {qcols}) AS _synapse_groups"
            group_count = int(conn.execute(text(group_count_sql)).scalar_one())
            if group_count == 0:
                return FanoutStats(avg=0, p50=0, p95=0, p99=0, max=0, source="exact")
            query = text(f"SELECT COUNT(*) AS _synapse_c FROM {qtable} GROUP BY {qcols} ORDER BY _synapse_c")
            result = conn.execute(query)
            target_ranks = {max(1, math.ceil(group_count * q / 100)): q for q in (50, 95, 99)}
            quantiles: dict[int, int] = {}
            total = maximum = 0
            for rank, row in enumerate(result, 1):
                count = int(row[0])
                total += count
                maximum = count
                if rank in target_ranks:
                    quantiles[target_ranks[rank]] = count
            return FanoutStats(avg=total / group_count, p50=quantiles.get(50, maximum), p95=quantiles.get(95, maximum),
                               p99=quantiles.get(99, maximum), max=maximum, source="exact")
        except Exception:
            return None
