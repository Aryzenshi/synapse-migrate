"""Canonical order-preserving key tuples and ANSI row-value keyset predicates."""
from __future__ import annotations

import base64
import datetime as dt
import decimal
import json
import math
import struct
import uuid
from typing import Any, Sequence

from synapse.domain.naming import quote_identifier, quote_qualified

_INT_BIAS = 1 << 127
_INT_LIMIT = 1 << 128


def _sortable_int(value: int) -> bytes:
    biased = value + _INT_BIAS
    if not 0 <= biased < _INT_LIMIT:
        raise OverflowError("canonical integer key must fit signed 128-bit range")
    return biased.to_bytes(16, "big", signed=False)


def _encode_decimal(value: decimal.Decimal) -> bytes:
    if not value.is_finite():
        raise ValueError("NaN and infinite Decimal values cannot be stable key components")
    if value.is_zero():
        return b"\x01"
    normalized = value.normalize()
    negative = normalized.is_signed()
    absolute = normalized.copy_abs()
    digits = "".join(str(digit) for digit in absolute.as_tuple().digits).encode("ascii")
    exponent = _sortable_int(absolute.adjusted())
    positive_payload = exponent + digits + b"\x00"
    if negative:
        return b"\x00" + bytes(255 - byte for byte in positive_payload)
    return b"\x02" + positive_payload


def _encode_component(value: Any) -> bytes:
    if value is None:
        raise ValueError("NULL is not a valid keyset/ID-map key component")
    if isinstance(value, bool):
        return b"b" + (b"\x01" if value else b"\x00")
    if isinstance(value, int):
        return b"i" + _sortable_int(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("NaN and infinite float keys are not supported")
        raw = struct.unpack(">Q", struct.pack(">d", value))[0]
        sortable = (~raw & 0xFFFFFFFFFFFFFFFF) if raw >> 63 else (raw ^ 0x8000000000000000)
        return b"f" + sortable.to_bytes(8, "big")
    if isinstance(value, decimal.Decimal):
        return b"d" + _encode_decimal(value)
    if isinstance(value, str):
        payload = value.encode("utf-8")
        return b"s" + payload.replace(b"\x00", b"\x00\xff") + b"\x00\x00"
    if isinstance(value, (bytes, bytearray, memoryview)):
        payload = bytes(value)
        return b"y" + payload.replace(b"\x00", b"\x00\xff") + b"\x00\x00"
    if isinstance(value, uuid.UUID):
        return b"u" + value.bytes
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.timezone.utc)
        micros = int(value.astimezone(dt.timezone.utc).timestamp() * 1_000_000)
        return b"t" + _sortable_int(micros)
    if isinstance(value, dt.date):
        days = (value - dt.date(1970, 1, 1)).days
        return b"D" + _sortable_int(days)
    raise TypeError(f"unsupported key component type: {type(value).__name__}")


def encode_tuple(values: Sequence[Any]) -> bytes:
    """Encode an ordered composite key; byte ordering preserves same-typed SQL tuple order."""
    if not isinstance(values, (list, tuple)) or len(values) == 0:
        raise ValueError("composite key must be a non-empty list or tuple")
    return b"".join(_encode_component(value) + b"\x00\x01" for value in values)


def encode_key_tuple(values: Sequence[Any]) -> bytes:
    return encode_tuple(values)


def _read_escaped(data: bytes, offset: int) -> tuple[bytes, int]:
    out = bytearray()
    while offset < len(data):
        byte = data[offset]
        offset += 1
        if byte != 0:
            out.append(byte)
            continue
        if offset >= len(data):
            raise ValueError("truncated escaped byte sequence")
        following = data[offset]
        offset += 1
        if following == 0:
            return bytes(out), offset
        if following == 0xFF:
            out.append(0)
        else:
            raise ValueError("invalid escaped byte sequence")
    raise ValueError("unterminated escaped byte sequence")


def decode_tuple(data: bytes) -> tuple[Any, ...]:
    """Decode keys produced by :func:`encode_tuple` (for checkpoint diagnostics)."""
    values: list[Any] = []
    pos = 0
    while pos < len(data):
        tag = data[pos:pos + 1]
        pos += 1
        if tag == b"b":
            values.append(data[pos] == 1)
            pos += 1
        elif tag == b"i":
            values.append(int.from_bytes(data[pos:pos + 16], "big") - _INT_BIAS)
            pos += 16
        elif tag == b"f":
            sortable = int.from_bytes(data[pos:pos + 8], "big")
            raw = (sortable ^ 0x8000000000000000) if sortable >> 63 else (~sortable & 0xFFFFFFFFFFFFFFFF)
            values.append(struct.unpack(">d", raw.to_bytes(8, "big"))[0])
            pos += 8
        elif tag == b"s" or tag == b"y":
            raw, pos = _read_escaped(data, pos)
            values.append(raw.decode("utf-8") if tag == b"s" else raw)
        elif tag == b"u":
            values.append(uuid.UUID(bytes=data[pos:pos + 16]))
            pos += 16
        elif tag == b"t" or tag == b"D":
            number = int.from_bytes(data[pos:pos + 16], "big") - _INT_BIAS
            pos += 16
            if tag == b"t":
                values.append(dt.datetime.fromtimestamp(number / 1_000_000, tz=dt.timezone.utc))
            else:
                values.append(dt.date(1970, 1, 1) + dt.timedelta(days=number))
        elif tag == b"d":
            marker = data[pos]
            pos += 1
            if marker == 1:
                values.append(decimal.Decimal(0))
            elif marker in {0, 2}:
                negative = marker == 0
                encoded_exponent = bytearray(data[pos:pos + 16])
                if len(encoded_exponent) != 16:
                    raise ValueError("truncated decimal exponent")
                pos += 16
                if negative:
                    encoded_exponent = bytearray(255 - byte for byte in encoded_exponent)
                exponent = int.from_bytes(encoded_exponent, "big") - _INT_BIAS
                encoded_digits = bytearray()
                while pos < len(data):
                    byte = data[pos]
                    pos += 1
                    if negative:
                        byte = 255 - byte
                    if byte == 0:
                        break
                    encoded_digits.append(byte)
                else:
                    raise ValueError("unterminated decimal digits")
                digits = encoded_digits.decode("ascii")
                if not digits or not digits.isdigit():
                    raise ValueError("invalid decimal digit sequence")
                magnitude = decimal.Decimal((0, tuple(int(char) for char in digits), exponent - len(digits) + 1))
                values.append(-magnitude if negative else magnitude)
            else:
                raise ValueError("invalid decimal sign marker")
        else:
            raise ValueError(f"unknown canonical key tag {tag!r}")
        if data[pos:pos + 2] != b"\x00\x01":
            raise ValueError("invalid composite-key component delimiter")
        pos += 2
    return tuple(values)


def keyset_predicate(columns: Sequence[str], last_values: Sequence[Any], *, dialect: str = "mysql", row_value: bool = True, quote: bool = True) -> tuple[str, dict[str, Any]]:
    """Return `(a,b) > (:last_0,:last_1)` or its portable expanded disjunction."""
    if len(columns) != len(last_values) or not columns:
        raise ValueError("key columns and last-key values must have equal non-zero arity")
    if any(value is None for value in last_values):
        raise ValueError("keyset cursor values cannot be NULL")
    qcols = [quote_identifier(column, dialect) if quote else column for column in columns]
    params = {f"last_{index}": value for index, value in enumerate(last_values)}
    if len(columns) == 1:
        return f"{qcols[0]} > :last_0", params
    if row_value:
        lhs = "(" + ", ".join(qcols) + ")"
        rhs = "(" + ", ".join(f":last_{index}" for index in range(len(columns))) + ")"
        return f"{lhs} > {rhs}", params
    parts: list[str] = []
    for index, column in enumerate(qcols):
        equal_prefix = [f"{qcols[prior]} = :last_{prior}" for prior in range(index)]
        greater = f"{column} > :last_{index}"
        parts.append("(" + " AND ".join([*equal_prefix, greater]) + ")")
    return "(" + " OR ".join(parts) + ")", params


def tuple_range_predicate(columns: Sequence[str], first_values: Sequence[Any], last_values: Sequence[Any], *, dialect: str = "mysql", quote: bool = True) -> tuple[str, dict[str, Any]]:
    """Inclusive lexicographic lower/upper key tuple range for bounded child reads."""
    if len(columns) != len(first_values) or len(columns) != len(last_values) or not columns:
        raise ValueError("range columns and endpoint tuples must have equal non-zero arity")
    if len(columns) == 1:
        col = quote_identifier(columns[0], dialect) if quote else columns[0]
        return f"{col} >= :first_0 AND {col} <= :last_0", {"first_0": first_values[0], "last_0": last_values[0]}
    qcols = [quote_identifier(column, dialect) if quote else column for column in columns]
    lhs = "(" + ", ".join(qcols) + ")"
    first = "(" + ", ".join(f":first_{i}" for i in range(len(columns))) + ")"
    last = "(" + ", ".join(f":last_{i}" for i in range(len(columns))) + ")"
    params = {**{f"first_{i}": v for i, v in enumerate(first_values)}, **{f"last_{i}": v for i, v in enumerate(last_values)}}
    return f"{lhs} >= {first} AND {lhs} <= {last}", params


def order_by(columns: Sequence[str], dialect: str = "mysql") -> str:
    if not columns:
        raise ValueError("ORDER BY requires at least one stable key column")
    return ", ".join(quote_identifier(column, dialect) for column in columns)


def keyset_select_sql(table: str, columns: Sequence[str], batch_size: int, *, last_values: Sequence[Any] | None = None, dialect: str = "mysql", row_value: bool = True, select: str = "*") -> tuple[str, dict[str, Any]]:
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    qtable = quote_qualified(*table.split(".", 1), dialect) if "." in table else quote_identifier(table, dialect)
    qselect = select if select == "*" else ", ".join(quote_identifier(part.strip(), dialect) for part in select.split(","))
    sql = f"SELECT {qselect} FROM {qtable}"
    params: dict[str, Any] = {"batch": int(batch_size)}
    if last_values is not None:
        predicate, key_params = keyset_predicate(columns, last_values, dialect=dialect, row_value=row_value)
        sql += " WHERE " + predicate
        params.update(key_params)
    return sql + f" ORDER BY {order_by(columns, dialect)} LIMIT :batch", params


def _cursor_pack(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, decimal.Decimal):
        return {"$decimal": str(value)}
    if isinstance(value, dt.datetime):
        return {"$datetime": value.isoformat(), "$fold": value.fold}
    if isinstance(value, dt.date):
        return {"$date": value.isoformat()}
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {"$bytes": base64.urlsafe_b64encode(bytes(value)).decode("ascii")}
    if isinstance(value, uuid.UUID):
        return {"$uuid": str(value)}
    if type(value).__name__.lower() == "objectid":
        return {"$oid": str(value)}
    raise TypeError(f"cursor value type {type(value).__name__} is not supported")


def _cursor_unpack(value: Any) -> Any:
    if isinstance(value, dict):
        if "$decimal" in value: return decimal.Decimal(value["$decimal"])
        if "$datetime" in value: return dt.datetime.fromisoformat(value["$datetime"]).replace(fold=int(value.get("$fold", 0)))
        if "$date" in value: return dt.date.fromisoformat(value["$date"])
        if "$bytes" in value: return base64.urlsafe_b64decode(value["$bytes"])
        if "$uuid" in value: return uuid.UUID(value["$uuid"])
        if "$oid" in value:
            from bson import ObjectId
            return ObjectId(value["$oid"])
        raise ValueError("unknown typed cursor component")
    return value


def encode_cursor(last_key: Sequence[Any]) -> str:
    if not isinstance(last_key, (list, tuple)) or not last_key:
        raise ValueError("cursor must contain a non-empty ordered key")
    payload = json.dumps([_cursor_pack(value) for value in last_key], separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return base64.urlsafe_b64encode(payload).rstrip(b"=").decode("ascii")


def decode_cursor(cursor: str) -> list[Any]:
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        value = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")))
    except Exception as exc:
        raise ValueError("invalid cursor") from exc
    if not isinstance(value, list) or not value:
        raise ValueError("cursor must encode a non-empty ordered key array")
    return [_cursor_unpack(item) for item in value]


# Explicit alias used by older call sites and tests.
composite_keyset_predicate = keyset_predicate
