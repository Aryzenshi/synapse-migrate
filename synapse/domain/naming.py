"""Stable, collision-safe SQL identifier generation and quoting."""
from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import dataclass, field
from typing import Iterable

MYSQL_RESERVED = frozenset("accessible add all alter analyze and as asc asensitive before between bigint binary blob both by call cascade case change char character check collate column condition constraint continue convert create cross current_date current_time current_timestamp current_user database databases day_hour day_microsecond day_minute day_second dec decimal declare default delayed delete desc describe deterministic distinct distinctrow div double drop dual each else elseif enclosed escaped exists exit explain false fetch float for force foreign from fulltext grant group having high_priority hour_microsecond hour_minute hour_second if ignore in index infile inner inout insensitive insert int integer interval into is iterate join key keys kill leading leave left like limit linear lines load localtime localtimestamp lock long longblob longtext loop low_priority master_ssl_verify_server_cert match mediumblob mediumint mediumtext middleint minute_microsecond minute_second mod modifies natural not no_write_to_bin numeric on optimize option optionally or order out outer outfile partition precision primary procedure purge range read reads real references regexp release rename repeat replace require restrict return revoke right rlike schema schemas second_microsecond select sensitive separator set show smallint spatial specific sql sqlexception sqlstate sqlwarning sql_big_result sql_calc_found_rows sql_small_result ssl starting straight_join table terminated then tinyblob tinyint tinytext to trailing trigger true undo union unique unlock unsigned update usage use using utc_date utc_time utc_timestamp values varbinary varchar varcharacter varying virtual when where while with write xor year_month zerofill".split())
POSTGRES_RESERVED = frozenset("all analyse analyze and any array as asc asymmetric authorization binary both case cast check collate collation column concurrently constraint create cross current_catalog current_date current_role current_time current_timestamp current_user default deferrable desc distinct do else end except false fetch for foreign freeze from full grant group having ilike in initially inner intersect into is isnull join lateral leading left like limit localtime localtimestamp natural not notnull null offset on only or order outer overlaps overlay placing primary references returning right select session_user similar some symmetric table tablesample then to trailing trigger true union unique user using variadic verbose when where window with".split())


def snake_case(value: str) -> str:
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    value = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", value)
    value = re.sub(r"[^A-Za-z0-9]+", "_", value).strip("_").lower()
    if not value:
        raise ValueError("identifier path must contain at least one alphanumeric character")
    if value[0].isdigit():
        value = "_" + value
    return value


def _hash6(path: str) -> str:
    return hashlib.sha256(path.encode("utf-8")).hexdigest()[:6]


def shorten_identifier(identifier: str, full_path: str, max_length: int = 64) -> str:
    if max_length < 9:
        raise ValueError("max identifier length must leave room for a hash suffix")
    if len(identifier) <= max_length:
        return identifier
    suffix = "_" + _hash6(full_path)
    return identifier[: max_length - len(suffix)].rstrip("_") + suffix


def flatten_path(path: str | Iterable[str], *, max_length: int = 64) -> str:
    segments = path.split(".") if isinstance(path, str) else list(path)
    cleaned = [snake_case(re.sub(r"\[\]$", "", str(part))) for part in segments if str(part)]
    full_path = ".".join(segments)
    return shorten_identifier("_".join(cleaned), full_path, max_length)


def child_table_name(parent: str, array_field: str, *, singularize: bool = False, max_length: int = 64) -> str:
    tail = snake_case(array_field.replace("[]", ""))
    if singularize:
        if tail.endswith("ies") and len(tail) > 3:
            tail = tail[:-3] + "y"
        elif tail.endswith("s") and not tail.endswith("ss"):
            tail = tail[:-1]
    parent_name = snake_case(parent.split(".")[-1])
    joined = f"{parent_name}_{tail}"
    return shorten_identifier(joined, f"{parent}.{array_field}", max_length)


def quote_identifier(identifier: str, dialect: str = "mysql", *, quote_reserved: bool = True) -> str:
    """Quote one identifier, rejecting dotted/raw SQL fragments."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", identifier):
        raise ValueError(f"unsafe SQL identifier {identifier!r}")
    reserved = MYSQL_RESERVED if dialect.lower().startswith("mysql") else POSTGRES_RESERVED
    is_reserved = identifier.lower() in reserved
    if not quote_reserved:
        return identifier + "_" if is_reserved else identifier
    if dialect.lower().startswith("mysql"):
        return "`" + identifier.replace("`", "``") + "`"
    return '"' + identifier.replace('"', '""') + '"'


def quote_qualified(schema: str | None, name: str, dialect: str = "mysql", *, quote_reserved: bool = True) -> str:
    qname = quote_identifier(name, dialect, quote_reserved=quote_reserved)
    if schema:
        return quote_identifier(schema, dialect, quote_reserved=quote_reserved) + "." + qname
    return qname


@dataclass
class NameRegistry:
    """Deterministic path→identifier map with `_2`, `_3` collision suffixes."""
    max_length: int = 64
    used: set[str] = field(default_factory=set)
    mapping: dict[str, str] = field(default_factory=dict)

    def register(self, path: str, candidate: str | None = None) -> str:
        if path in self.mapping:
            return self.mapping[path]
        base = shorten_identifier(snake_case(candidate or path.replace(".", "_")), path, self.max_length)
        result = base
        index = 2
        while result.casefold() in {item.casefold() for item in self.used}:
            suffix = f"_{index}"
            result = base[: self.max_length - len(suffix)].rstrip("_") + suffix
            index += 1
        self.mapping[path] = result
        self.used.add(result)
        return result

    def seed(self, names: Iterable[str]) -> None:
        for name in names:
            self.used.add(snake_case(name))

    def to_dict(self) -> dict[str, str]:
        return dict(sorted(self.mapping.items()))
