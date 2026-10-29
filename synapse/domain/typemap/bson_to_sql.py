"""Conservative BSON→SQL widening and sample-aware type sizing."""
from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Iterable

from synapse.domain.ir.document import FieldNode
from synapse.domain.typemap.sql_to_bson import BsonTypeMapping

LENGTH_BUCKETS = (32, 64, 128, 255, 512, 1024, 4096, 16383)


@dataclass(frozen=True)
class SqlTypeSpec:
    sql_type: str
    nullable: bool = True
    warning: str | None = None
    lossy: bool = False
    charset: str | None = None
    collation: str | None = None


def wilson_interval(successes: int | float, n: int, z: float = 1.959963984540054) -> tuple[float, float]:
    from synapse.domain.heuristics.reverse import wilson_interval as _wilson
    return _wilson(successes, n, z)


def _quoted(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _bucket_length(node: FieldNode) -> tuple[int | None, str | None]:
    declared = node.declared.max_length if node.declared else None
    p99 = node.string_stats.p99_len if node.string_stats else 0
    observed_max = node.string_stats.max_len if node.string_stats else 0
    requested = max(declared or 0, math.ceil(p99 * 1.5), math.ceil(observed_max * 1.25))
    if requested <= 0:
        requested = 255
    for bucket in LENGTH_BUCKETS:
        if requested <= bucket:
            return bucket, None
    if requested <= 65_535:
        return None, "TEXT"
    if requested <= 16_777_215:
        return None, "MEDIUMTEXT"
    return None, "LONGTEXT"


def _numeric_sql(types: set[str], node: FieldNode, dialect: str, prefer_int: bool) -> SqlTypeSpec:
    mysql = dialect.lower().startswith("mysql")
    if "decimal" in types:
        precision = node.numeric_stats.precision if node.numeric_stats else None
        scale = node.numeric_stats.scale if node.numeric_stats else None
        precision = min(65 if mysql else 1000, max(1, precision or 38))
        scale = min(30 if mysql else precision, max(0, scale if scale is not None else 10))
        precision = max(precision, scale)
        return SqlTypeSpec(f"{'DECIMAL' if mysql else 'NUMERIC'}({precision},{scale})")
    if "double" in types:
        if prefer_int and node.numeric_stats and node.numeric_stats.all_integer_valued:
            return SqlTypeSpec("BIGINT")
        return SqlTypeSpec("DOUBLE" if mysql else "DOUBLE PRECISION")
    if "long" in types:
        return SqlTypeSpec("BIGINT")
    if "int" in types or "number" in types:
        return SqlTypeSpec("INT" if mysql else "INTEGER")
    return SqlTypeSpec("JSON" if mysql else "JSONB", warning="numeric profile has no recognized scalar type")


def bson_to_sql(node_or_types: FieldNode | Iterable[str], *, dialect: str = "mysql", context: str = "scalar", prefer_int: bool = False, nullable: bool | None = None) -> SqlTypeSpec:
    """Map observed BSON type(s) to a conservative SQL type, respecting RH context."""
    if isinstance(node_or_types, FieldNode):
        node = node_or_types
        types = {item.bson_type for item in node.types if item.bson_type not in {"null", "undefined"} and (item.count > 0 or item.rate > 0)}
        if node.kind.value in {"object", "array", "mixed"}:
            types.add(node.kind.value)
        if nullable is None:
            is_required = bool(node.declared and node.declared.required)
            # Sample-only NOT NULL requires both no empties and a conservative Wilson lower bound.
            empty = node.empty_rate
            lower_presence = wilson_interval(round(node.presence_rate * node.sample_size), node.sample_size)[0]
            nullable = not (is_required or (empty == 0 and lower_presence >= 0.995))
    else:
        node = None
        types = set(node_or_types)
        if nullable is None:
            nullable = True
    nullable = bool(nullable)
    mysql = dialect.lower().startswith("mysql")
    if not types:
        return SqlTypeSpec("VARCHAR(1)" if mysql else "TEXT", nullable=True, warning="no non-null BSON type was observed")
    numeric = types & {"int", "long", "double", "decimal", "number"}
    if numeric and types <= {"int", "long", "double", "decimal", "number"}:
        if node is None:
            class _Numeric:
                numeric_stats = None
            node = _Numeric()  # type: ignore[assignment]
        return _numeric_sql(numeric, node, dialect, prefer_int)

    if context == "root_id" and "objectId" in types:
        return SqlTypeSpec("VARCHAR(24) CHARACTER SET ascii COLLATE ascii_bin" if mysql else "CHAR(24)")
    if context in {"reference", "junction_reference"} and "objectId" in types:
        return SqlTypeSpec("BIGINT UNSIGNED" if mysql else "BIGINT")
    if "objectId" in types:
        return SqlTypeSpec("VARCHAR(24) CHARACTER SET ascii COLLATE ascii_bin" if mysql else "CHAR(24)", warning="ObjectId stored as its 24-character hex form")
    if "string" in types:
        if node is None:
            return SqlTypeSpec("VARCHAR(255)" if mysql else "VARCHAR(255)")
        length, large_type = _bucket_length(node)
        if large_type:
            return SqlTypeSpec(large_type if mysql else "TEXT")
        return SqlTypeSpec(f"VARCHAR({length})", charset="utf8mb4" if mysql else None)
    if types <= {"bool"}:
        return SqlTypeSpec("TINYINT(1)" if mysql else "BOOLEAN")
    if types <= {"date"}:
        return SqlTypeSpec("DATETIME(3)" if mysql else "TIMESTAMPTZ(3)")
    if types <= {"timestamp"}:
        return SqlTypeSpec("BIGINT", warning="BSON internal timestamp retained as integer")
    if "binData" in types:
        return SqlTypeSpec("VARBINARY(4096)" if mysql else "BYTEA")
    if types <= {"null"}:
        return SqlTypeSpec("VARCHAR(1)" if mysql else "TEXT", nullable=True, warning="null-only sample has no type information")
    if types & {"object", "array", "mixed"}:
        return SqlTypeSpec("JSON" if mysql else "JSONB")
    if types & {"regex", "javascript", "symbol", "dbPointer", "minKey", "maxKey"}:
        return SqlTypeSpec("TEXT", warning="BSON special type serialized to text", lossy=True)
    return SqlTypeSpec("JSON" if mysql else "JSONB", warning=f"unrecognized BSON type(s): {sorted(types)}")


def serialize_demoted(value: Any) -> str | None:
    """Canonical, deterministic text representation for RH4 scalar demotion."""
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool, Decimal)):
        return str(value)
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def sql_widening(current: str, value: Any, *, dialect: str = "mysql") -> str | None:
    """Return a bounded next type for a detected drift value, or None if unchanged."""
    raw = current.upper()
    if isinstance(value, str):
        match = re.fullmatch(r"VARCHAR\((\d+)\)", raw)
        if match:
            size = int(match.group(1))
            required = len(value)
            if required <= size:
                return None
            for bucket in LENGTH_BUCKETS:
                if bucket > max(size, required):
                    return f"VARCHAR({bucket})"
            return "TEXT" if dialect.lower().startswith("mysql") else "TEXT"
        if raw in {"TEXT", "MEDIUMTEXT"} and len(value) > 65_535:
            return "MEDIUMTEXT" if raw == "TEXT" else "LONGTEXT"
    if isinstance(value, int) and raw in {"INT", "INTEGER", "INT UNSIGNED"}:
        if not -(2**31) <= value < 2**31:
            return "BIGINT"
    if isinstance(value, Decimal):
        match = re.fullmatch(r"(?:DECIMAL|NUMERIC)\((\d+),(\d+)\)", raw)
        if match:
            precision, scale = map(int, match.groups())
            value_scale = max(0, -value.as_tuple().exponent)
            value_precision = len(value.as_tuple().digits) + max(0, value_scale - scale)
            if value_scale > scale or value_precision > precision:
                new_scale = min(30 if dialect.lower().startswith("mysql") else value_scale, max(scale, value_scale))
                new_precision = min(65 if dialect.lower().startswith("mysql") else max(precision, value_precision), max(precision + 1, value_precision + 1))
                if new_precision > precision or new_scale > scale:
                    return f"{'DECIMAL' if dialect.lower().startswith('mysql') else 'NUMERIC'}({new_precision},{new_scale})"
    if isinstance(value, (dict, list)) and raw not in {"JSON", "JSONB"}:
        return "JSON" if dialect.lower().startswith("mysql") else "JSONB"
    return None
