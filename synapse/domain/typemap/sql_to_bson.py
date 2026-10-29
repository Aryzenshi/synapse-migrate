"""Relational SQL type/value translation to BSON and MongoDB JSON Schema."""
from __future__ import annotations

import datetime as dt
import decimal
import json
import re
import uuid
from dataclasses import dataclass, field
from typing import Any
from zoneinfo import ZoneInfo

from synapse.domain.ir.common import CanonicalType
from synapse.domain.ir.relational import Column


@dataclass(frozen=True)
class BsonTypeMapping:
    bson_type: str | list[str]
    schema: dict[str, Any] = field(default_factory=dict)
    warning: str | None = None
    lossy: bool = False


def _type_text(value: str | Column) -> str:
    return value.sql_type if isinstance(value, Column) else str(value)


def parse_sql_type(sql_type: str) -> tuple[str, tuple[int, ...], bool]:
    raw = sql_type.strip()
    unsigned = "unsigned" in raw.lower()
    base = re.sub(r"\bunsigned\b", "", raw, flags=re.I).strip().lower()
    match = re.match(r"([a-zA-Z ]+)\s*\((.*?)\)", base)
    if match:
        name = re.sub(r"\s+", " ", match.group(1).strip())
        args = tuple(int(x.strip()) for x in re.findall(r"\d+", match.group(2)))
    else:
        name = re.sub(r"\s+", " ", base.split()[0]) if base else "unknown"
        args = ()
    return name, args, unsigned


def map_sql_type(column_or_type: Column | str, *, double_for_decimal: bool = False, geometry_as_geojson: bool = False) -> BsonTypeMapping:
    raw = _type_text(column_or_type)
    name, args, unsigned = parse_sql_type(raw)
    if isinstance(column_or_type, Column):
        unsigned = unsigned or column_or_type.unsigned
        enum_values = column_or_type.enum_values or []
    else:
        enum_values = []
    if name == "tinyint" and args == (1,):
        return BsonTypeMapping("bool")
    if name in {"tinyint", "smallint", "mediumint", "int", "integer", "year", "serial"}:
        if unsigned and name in {"int", "integer"}:
            return BsonTypeMapping("long", {"minimum": 0})
        return BsonTypeMapping("int", {"minimum": 0} if unsigned else {})
    if name in {"bigint", "bigserial"}:
        if unsigned:
            return BsonTypeMapping("decimal", {}, "BIGINT UNSIGNED can exceed BSON signed int64; Decimal128 conversion is required")
        return BsonTypeMapping("long")
    if name in {"decimal", "numeric", "number"}:
        if double_for_decimal:
            return BsonTypeMapping("double", {}, "DECIMAL/NUMERIC converted to double; precision may be lost", True)
        return BsonTypeMapping("decimal")
    if name in {"float", "double", "real", "double precision"}:
        return BsonTypeMapping("double")
    if name in {"varchar", "char", "character", "character varying", "nvarchar", "nchar"}:
        schema = {"maxLength": args[0]} if args else {}
        return BsonTypeMapping("string", schema)
    if name in {"text", "tinytext", "mediumtext", "longtext", "clob"}:
        return BsonTypeMapping("string")
    if name in {"enum", "set"}:
        values = list(enum_values)
        if not values and "(" in raw:
            inner = raw[raw.find("(") + 1:raw.rfind(")")]
            values = re.findall(r"'((?:[^']|'')*)'", inner)
            values = [value.replace("''", "'") for value in values]
        if name == "set":
            return BsonTypeMapping("array", {"items": {"bsonType": "string", **({"enum": values} if values else {})}})
        return BsonTypeMapping("string", {"enum": values} if values else {})
    if name in {"date", "datetime", "timestamp", "timestamp with time zone", "timestamp without time zone"}:
        return BsonTypeMapping("date")
    if name == "time":
        return BsonTypeMapping("string", {"pattern": r"^([01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d+)?$"}, "BSON has no native time-only scalar")
    if name in {"bool", "boolean"} or (name == "tinyint" and args == (1,)):
        return BsonTypeMapping("bool")
    if name in {"json", "jsonb"}:
        return BsonTypeMapping(["object", "array"], {})
    if name in {"blob", "tinyblob", "mediumblob", "longblob", "binary", "varbinary", "bytea"}:
        if name == "binary" and args == (16,):
            return BsonTypeMapping("binData", {"subType": 4})
        return BsonTypeMapping("binData")
    if name in {"uuid"}:
        return BsonTypeMapping("binData", {"subType": 4})
    if name in {"geometry", "point", "linestring", "polygon"}:
        if geometry_as_geojson:
            return BsonTypeMapping("object", {"description": "GeoJSON geometry"})
        return BsonTypeMapping("object", {}, "Spatial type not converted; enable geometry_as_geojson or review manually", True)
    return BsonTypeMapping("string", {}, f"Unsupported SQL type {raw!r} serialized as text", True)


def column_json_schema(column: Column, *, nullable_as_union: bool = True, double_for_decimal: bool = False) -> dict[str, Any]:
    mapped = map_sql_type(column, double_for_decimal=double_for_decimal)
    bson_type = mapped.bson_type
    schema: dict[str, Any] = {}
    if isinstance(bson_type, list):
        schema["bsonType"] = bson_type
    else:
        schema["bsonType"] = [bson_type, "null"] if column.nullable and nullable_as_union else bson_type
    schema.update(mapped.schema)
    if mapped.warning:
        schema["description"] = mapped.warning
    return schema


def to_bson_value(value: Any, column: Column | None = None, *, source_tz: str = "UTC", double_for_decimal: bool = False) -> Any:
    if value is None:
        return None
    raw = column.sql_type if column else ""
    name, _, _ = parse_sql_type(raw) if raw else ("", (), False)
    if isinstance(value, decimal.Decimal) or (name in {"decimal", "numeric", "number"} and isinstance(value, (int, float, str)) and not isinstance(value, bool)):
        decimal_value = value if isinstance(value, decimal.Decimal) else decimal.Decimal(str(value))
        if double_for_decimal:
            return float(decimal_value)
        try:
            from bson.decimal128 import Decimal128
            return Decimal128(decimal_value)
        except ImportError:
            return decimal_value
    if name in {"json", "jsonb"} and isinstance(value, (str, bytes, bytearray)):
        parsed = json.loads(value)
        return to_bson_value(parsed, None, source_tz=source_tz, double_for_decimal=double_for_decimal)
    if isinstance(value, str) and name in {"date", "datetime", "timestamp", "timestamp with time zone", "timestamp without time zone"}:
        normalized = value.strip()
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"
        parsed = dt.datetime.fromisoformat(normalized)
        return to_bson_value(parsed, column, source_tz=source_tz, double_for_decimal=double_for_decimal)
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, (bytes, bytearray)):
        try:
            if name == "binary" and len(value) == 16:
                from bson.binary import Binary, UUID_SUBTYPE
                return Binary(bytes(value), subtype=UUID_SUBTYPE)
            from bson.binary import Binary
            return Binary(bytes(value))
        except ImportError:
            return bytes(value)
    if isinstance(value, uuid.UUID):
        try:
            from bson.binary import Binary, UUID_SUBTYPE
            return Binary(value.bytes, subtype=UUID_SUBTYPE)
        except ImportError:
            return value.bytes
    if isinstance(value, dt.datetime):
        if value.year == 0:
            return None
        if value.tzinfo is None:
            value = value.replace(tzinfo=ZoneInfo(source_tz))
        value = value.astimezone(dt.timezone.utc)
        return value
    if isinstance(value, dt.date):
        return dt.datetime.combine(value, dt.time.min, tzinfo=dt.timezone.utc)
    if isinstance(value, dt.time):
        return value.isoformat(timespec="auto")
    if name in {"tinyint", "boolean", "bool"} and (isinstance(value, bool) or raw.lower().startswith("tinyint(1)")):
        return bool(value)
    if isinstance(value, dict):
        return {str(key): to_bson_value(item, None, source_tz=source_tz, double_for_decimal=double_for_decimal) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_bson_value(item, None, source_tz=source_tz, double_for_decimal=double_for_decimal) for item in value]
    return value


def type_warning(column: Column) -> str | None:
    return map_sql_type(column).warning
