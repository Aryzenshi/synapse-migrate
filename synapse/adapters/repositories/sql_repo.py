"""Verified SQL row-aggregate CRUD for generated reverse-normalization plans."""
from __future__ import annotations

import copy
import json
from contextlib import nullcontext
from typing import Any

from synapse.domain.ir.plan import ApprovedPlan
from synapse.etl.keys import decode_cursor, encode_cursor, keyset_predicate


class SqlVersionConflict(RuntimeError):
    pass


class SqlAggregateNotFound(LookupError):
    pass


class SqlValidationError(ValueError):
    def __init__(self, errors: list[dict[str, Any]]):
        super().__init__("SQL aggregate failed API-level validation")
        self.errors = errors


class VerifiedSqlAggregateRepository:
    def __init__(self, engine: Any, plan: ApprovedPlan, root_table: str):
        self.engine = engine
        self.plan = plan
        self.tables = {item["name"]: item for item in plan.tables}
        self.root = self.tables.get(root_table)
        if self.root is None or self.root.get("role") != "root":
            raise KeyError(f"{root_table!r} is not a root table in the approved plan")
        self.collection = self.root["collection"]
        self.children = [item for item in plan.tables if item.get("collection") == self.collection and item.get("role") in {"side", "child", "junction"}]
        self._metadata = None
        self._sql_tables: dict[str, Any] = {}

    def _connect(self):
        if hasattr(self.engine, "begin"):
            return self.engine.begin()
        return nullcontext(self.engine)

    def _reflect(self, connection: Any) -> None:
        from sqlalchemy import MetaData, Table
        if self._metadata is None:
            self._metadata = MetaData()
            for name, plan_table in self.tables.items():
                try:
                    self._sql_tables[name] = Table(name, self._metadata, autoload_with=connection)
                except Exception:
                    continue

    def validate(self, payload: dict[str, Any]) -> None:
        errors: list[dict[str, Any]] = []
        source_paths = [column.get("source_path", "") for column in self.root.get("columns", []) if column.get("source_path") and not column["source_path"].startswith("$")]
        source_paths.extend(table.get("source_path", "") for table in self.children)
        known_columns = {column["name"] for table in [self.root, *self.children] for column in table.get("columns", [])}
        top_level = {path.split(".")[0].replace("[]", "") for path in source_paths if path}
        reserved = {"_id", "id", "legacy_mongo_id", "legacy_id", "row_version", "is_stub", "position"}
        for key in payload:
            if key not in top_level and key not in reserved and key not in known_columns:
                errors.append({"path": f"$.{key}", "code": "additional_property", "message": "field is not present in the approved SQL aggregate schema", "source": "api"})
        for column in self.root.get("columns", []):
            name = column["name"]
            source_path = column.get("source_path", "")
            if name == self.root.get("id_column") or name == self.root.get("legacy_id_column") or name == "row_version" or column.get("auto_increment"):
                continue
            if source_path and not source_path.startswith("$"):
                value = self._get_path(payload, source_path)
                if value is None and column.get("nullable") is False:
                    errors.append({"path": f"$.{source_path}", "code": "not_null", "message": "field is required", "source": "api"})
                elif value is not None:
                    type_error = self._value_type_error(value, column.get("sql_type", ""))
                    if type_error:
                        errors.append({"path": f"$.{source_path}", "code": "type", "message": type_error, "source": "api"})
        for table in self.children:
            if table.get("role") == "side":
                value = self._get_path(payload, table.get("source_path", ""))
                if isinstance(value, dict):
                    prefix = table["source_path"] + "."
                    for column in table.get("columns", []):
                        path = column.get("source_path", "")
                        if column["name"] == table.get("id_column") or not path.startswith(prefix): continue
                        relative = path[len(prefix):]
                        item = self._get_path(value, relative)
                        if item is None and not column.get("nullable", True):
                            errors.append({"path": f"$.{table['source_path']}.{relative}", "code": "not_null", "message": "field is required", "source": "api"})
                        elif item is not None and (type_error := self._value_type_error(item, column.get("sql_type", ""))):
                            errors.append({"path": f"$.{table['source_path']}.{relative}", "code": "type", "message": type_error, "source": "api"})
            elif table.get("role") == "child":
                values = self._get_path(payload, table.get("source_path", ""))
                if not isinstance(values, list): continue
                prefix = table["source_path"] + "[]."
                for index, value in enumerate(values):
                    if not isinstance(value, dict):
                        errors.append({"path": f"$.{table['source_path']}[{index}]", "code": "type", "message": "array elements must be objects", "source": "api"})
                        continue
                    for column in table.get("columns", []):
                        path = column.get("source_path", "")
                        if not path.startswith(prefix) or column["name"] in {table.get("id_column"), table.get("id_column_parent"), "position"}: continue
                        relative = path[len(prefix):]
                        item = self._get_path(value, relative)
                        field_path = f"$.{table['source_path']}[{index}].{relative}"
                        if item is None and not column.get("nullable", True):
                            errors.append({"path": field_path, "code": "not_null", "message": "field is required", "source": "api"})
                        elif item is not None and (type_error := self._value_type_error(item, column.get("sql_type", ""))):
                            errors.append({"path": field_path, "code": "type", "message": type_error, "source": "api"})
        if errors:
            raise SqlValidationError(errors)

    def create(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.validate(payload)
        with self._connect() as connection:
            self._reflect(connection)
            root_row = self._root_row(payload)
            table = self._sql_tables[self.root["name"]]
            result = connection.execute(table.insert().values(**root_row))
            root_pk = self._inserted_pk(result, connection, table, root_row)
            self._write_children(connection, payload, root_pk)
            assembled = self._assemble(connection, root_pk)
        return assembled

    def get(self, identifier: Any, *, by: str | None = None, expand: list[str] | None = None) -> dict[str, Any]:
        from sqlalchemy import select
        with self._connect() as connection:
            self._reflect(connection)
            table = self._sql_tables[self.root["name"]]
            id_column = by or self.root.get("id_column")
            if id_column == "legacy_mongo_id" and self.root.get("legacy_id_column") != "legacy_mongo_id":
                id_column = self.root.get("legacy_id_column")
            column = table.c.get(id_column)
            if column is None:
                raise ValueError(f"unknown root key column {id_column!r}")
            row = connection.execute(select(table).where(column == identifier)).mappings().first()
            if row is None:
                raise SqlAggregateNotFound(str(identifier))
            return self._assemble(connection, row[self.root["id_column"]], root_row=dict(row))

    def replace(self, identifier: Any, payload: dict[str, Any], *, if_match: int | None = None) -> dict[str, Any]:
        self.validate(payload)
        from sqlalchemy import select, update
        with self._connect() as connection:
            self._reflect(connection)
            table = self._sql_tables[self.root["name"]]
            row = connection.execute(select(table).where(table.c[self.root["id_column"]] == identifier)).mappings().first()
            if row is None:
                raise SqlAggregateNotFound(str(identifier))
            version = int(row.get("row_version", 1))
            if if_match is not None and version != if_match:
                raise SqlVersionConflict(f"expected version {if_match}, current version is {version}")
            values = self._root_row(payload)
            values.pop(self.root.get("id_column"), None)
            values["row_version"] = version + 1
            changed = connection.execute(update(table).where(table.c[self.root["id_column"]] == identifier, table.c["row_version"] == version).values(**values))
            if changed.rowcount != 1:
                raise SqlVersionConflict("aggregate changed during replacement")
            self._replace_children(connection, payload, identifier)
            return self._assemble(connection, identifier)

    def patch(self, identifier: Any, patch: dict[str, Any], *, if_match: int | None = None) -> dict[str, Any]:
        current = self.get(identifier)
        version = int(current.get("row_version", 1))
        if if_match is not None and if_match != version:
            raise SqlVersionConflict(f"expected version {if_match}, current version is {version}")
        merged = self._merge_patch(current, patch)
        return self.replace(identifier, merged, if_match=version)

    def delete(self, identifier: Any, *, if_match: int | None = None, restrict: bool = True) -> bool:
        from sqlalchemy import delete, select
        with self._connect() as connection:
            self._reflect(connection)
            root = self._sql_tables[self.root["name"]]
            row = connection.execute(select(root).where(root.c[self.root["id_column"]] == identifier)).mappings().first()
            if row is None:
                raise SqlAggregateNotFound(str(identifier))
            version = int(row.get("row_version", 1))
            if if_match is not None and version != if_match:
                raise SqlVersionConflict(f"expected version {if_match}, current version is {version}")
            if restrict:
                for table_spec in self.tables.values():
                    for fk in table_spec.get("foreign_keys", []):
                        if fk["target_table"] == self.root["name"] and table_spec["name"] != self.root["name"]:
                            child_table = self._sql_tables.get(table_spec["name"])
                            if child_table is not None and connection.execute(select(child_table).where(child_table.c[fk["columns"][0]] == identifier).limit(1)).first():
                                if fk.get("on_delete") != "CASCADE":
                                    raise RuntimeError("referenced: root row has RESTRICT references")
            connection.execute(delete(root).where(root.c[self.root["id_column"]] == identifier, root.c["row_version"] == version))
        return True

    def list(self, *, limit: int = 50, cursor: str | None = None, filters: dict[str, Any] | None = None) -> tuple[list[dict[str, Any]], str | None, bool]:
        from sqlalchemy import select
        if not 1 <= limit <= 500:
            raise ValueError("limit must be 1..500")
        with self._connect() as connection:
            self._reflect(connection)
            table = self._sql_tables[self.root["name"]]
            statement = select(table).order_by(*(table.c[name] for name in self.root["primary_key"]))
            for field, value in (filters or {}).items():
                if field not in table.c:
                    raise ValueError(f"unknown filter field {field!r}")
                column = table.c[field]
                if isinstance(value, dict):
                    for operator, operand in value.items():
                        if operator == "$in": statement = statement.where(column.in_(operand))
                        elif operator == "gte": statement = statement.where(column >= operand)
                        elif operator == "gt": statement = statement.where(column > operand)
                        elif operator == "lte": statement = statement.where(column <= operand)
                        elif operator == "lt": statement = statement.where(column < operand)
                        elif operator == "ne": statement = statement.where(column != operand)
                        else: raise ValueError(f"unsupported SQL filter operator {operator!r}")
                else:
                    statement = statement.where(column == value)
            if cursor:
                values = decode_cursor(cursor)
                columns = self.root["primary_key"]
                if len(values) != len(columns):
                    raise ValueError("cursor arity does not match the composite primary key")
                # SQLAlchemy tuple comparison is used where supported; the helper builds the portable disjunction.
                predicate, params = keyset_predicate(columns, values, dialect=connection.dialect.name, row_value=False)
                from sqlalchemy import text
                statement = statement.where(text(predicate)).params(**params)
            rows = connection.execute(statement.limit(limit + 1)).mappings().all()
            has_more = len(rows) > limit
            rows = rows[:limit]
            results = [self._assemble(connection, row[self.root["id_column"]], root_row=dict(row)) for row in rows]
            next_cursor = encode_cursor([rows[-1][key] for key in self.root["primary_key"]]) if has_more and rows else None
            return results, next_cursor, has_more

    @staticmethod
    def _value_type_error(value: Any, sql_type: str) -> str | None:
        raw = sql_type.upper()
        if any(token in raw for token in ("INT", "SERIAL")):
            if isinstance(value, bool) or not isinstance(value, int): return "must be an integer"
        elif any(token in raw for token in ("DECIMAL", "NUMERIC", "NUMBER")):
            try:
                import decimal
                decimal.Decimal(str(value))
            except decimal.InvalidOperation:
                return "must be a decimal number or decimal string"
        elif any(token in raw for token in ("FLOAT", "DOUBLE", "REAL")):
            if isinstance(value, bool) or not isinstance(value, (int, float)): return "must be a number"
        elif "BOOL" in raw or raw == "BIT":
            if not isinstance(value, bool): return "must be a boolean"
        elif any(token in raw for token in ("CHAR", "TEXT", "CLOB", "ENUM")):
            if not isinstance(value, str): return "must be a string"
        elif any(token in raw for token in ("DATE", "TIME")):
            if not isinstance(value, str) and not hasattr(value, "isoformat"): return "must be an ISO date/time string"
        elif "JSON" in raw:
            if not isinstance(value, (dict, list)): return "must be an object or array"
        return None

    def _root_row(self, payload: dict[str, Any]) -> dict[str, Any]:
        row: dict[str, Any] = {}
        foreign_columns = {column for fk in self.root.get("foreign_keys", []) for column in fk["columns"]}
        for column in self.root.get("columns", []):
            name = column["name"]
            source_path = column.get("source_path", "")
            if name == self.root.get("id_column") or column.get("auto_increment") or name == "row_version":
                if name == "row_version": row[name] = 1
                continue
            if name == self.root.get("legacy_id_column"):
                row[name] = payload.get("legacy_mongo_id")
                continue
            if name in foreign_columns:
                row[name] = payload.get(name)
                continue
            if not source_path or source_path.startswith("$"):
                continue
            value = self._get_path(payload, source_path)
            if value is None and source_path in payload:
                value = payload[source_path]
            row[name] = self._to_sql(value, column.get("sql_type", ""))
        return row

    def _write_children(self, connection: Any, payload: dict[str, Any], root_pk: Any) -> None:
        from sqlalchemy import insert
        for spec in self.children:
            table = self._sql_tables.get(spec["name"])
            if table is None:
                continue
            if spec["role"] == "side":
                value = self._get_path(payload, spec.get("source_path", ""))
                if not isinstance(value, dict): continue
                row = self._side_row(spec, value, root_pk)
                if row: connection.execute(insert(table).values(**row))
            elif spec["role"] == "child":
                values = self._get_path(payload, spec.get("source_path", ""))
                if not isinstance(values, list): continue
                for position, value in enumerate(values):
                    row = self._child_row(spec, value, root_pk, position)
                    connection.execute(insert(table).values(**row))
            elif spec["role"] == "junction":
                values = self._get_path(payload, spec.get("source_path", ""))
                if not isinstance(values, list): continue
                self._write_junction(connection, spec, table, values, root_pk)

    def _replace_children(self, connection: Any, payload: dict[str, Any], root_pk: Any) -> None:
        from sqlalchemy import delete, insert, select, update
        for spec in self.children:
            table = self._sql_tables.get(spec["name"])
            if table is None: continue
            parent_col = spec.get("id_column_parent") or spec.get("primary_key", [None])[0]
            if spec["role"] == "side":
                value = self._get_path(payload, spec.get("source_path", ""))
                existing = connection.execute(select(table).where(table.c[parent_col] == root_pk)).mappings().first()
                if not isinstance(value, dict):
                    if existing: connection.execute(delete(table).where(table.c[parent_col] == root_pk))
                    continue
                row = self._side_row(spec, value, root_pk)
                if existing:
                    connection.execute(update(table).where(table.c[parent_col] == root_pk).values(**{k: v for k, v in row.items() if k != parent_col}))
                else:
                    connection.execute(insert(table).values(**row))
            elif spec["role"] == "child":
                values = self._get_path(payload, spec.get("source_path", ""))
                values = values if isinstance(values, list) else []
                for position, value in enumerate(values):
                    row = self._child_row(spec, value, root_pk, position)
                    existing = connection.execute(select(table).where(table.c[parent_col] == root_pk, table.c.position == position)).mappings().first()
                    if existing:
                        connection.execute(update(table).where(table.c[parent_col] == root_pk, table.c.position == position).values(**{k: v for k, v in row.items() if k not in {parent_col, "position", spec.get("id_column")}}))
                    else:
                        connection.execute(insert(table).values(**row))
                if parent_col and "position" in table.c:
                    connection.execute(delete(table).where(table.c[parent_col] == root_pk, table.c.position >= len(values)))
            elif spec["role"] == "junction":
                connection.execute(delete(table).where(table.c[spec["primary_key"][0]] == root_pk))
                values = self._get_path(payload, spec.get("source_path", ""))
                if isinstance(values, list): self._write_junction(connection, spec, table, values, root_pk)

    def _assemble(self, connection: Any, root_pk: Any, root_row: dict[str, Any] | None = None) -> dict[str, Any]:
        from sqlalchemy import select
        self._reflect(connection)
        root_table = self._sql_tables[self.root["name"]]
        if root_row is None:
            root_row = dict(connection.execute(select(root_table).where(root_table.c[self.root["id_column"]] == root_pk)).mappings().one())
        result: dict[str, Any] = {}
        for column in self.root.get("columns", []):
            name = column["name"]
            if name == self.root.get("id_column"):
                result[name] = root_row.get(name)
                continue
            if name == self.root.get("legacy_id_column"):
                result["legacy_mongo_id"] = root_row.get(name)
                continue
            if name in {"row_version", "is_stub"}:
                result[name] = root_row.get(name)
                continue
            source_path = column.get("source_path", "")
            if source_path and not source_path.startswith("$"):
                self._set_path(result, source_path, self._from_sql(root_row.get(name)))
        for spec in self.children:
            table = self._sql_tables.get(spec["name"])
            if table is None: continue
            if spec["role"] == "side":
                parent_col = spec.get("id_column")
                row = connection.execute(select(table).where(table.c[parent_col] == root_pk)).mappings().first()
                if row:
                    nested: dict[str, Any] = {}
                    for column in spec.get("columns", []):
                        source_path = column.get("source_path", "")
                        if column["name"] != parent_col and source_path:
                            relative = source_path[len(spec["source_path"]) + 1:] if source_path.startswith(spec["source_path"] + ".") else source_path
                            self._set_path(nested, relative, self._from_sql(row[column["name"]]))
                    self._set_path(result, spec["source_path"], nested)
            elif spec["role"] == "child":
                parent_col = spec.get("id_column_parent")
                rows = connection.execute(select(table).where(table.c[parent_col] == root_pk).order_by(table.c.position)).mappings().all()
                array: list[Any] = []
                for row in rows:
                    element: dict[str, Any] = {}
                    for column in spec.get("columns", []):
                        if column["name"] in {spec.get("id_column"), parent_col, "position"}:
                            if column["name"] in {spec.get("id_column"), "position"}:
                                element[column["name"]] = row.get(column["name"])
                            continue
                        source_path = column.get("source_path", "")
                        prefix = spec["source_path"] + "[]."
                        relative = source_path[len(prefix):] if source_path.startswith(prefix) else source_path
                        self._set_path(element, relative, self._from_sql(row.get(column["name"])))
                    array.append(element)
                self._set_path(result, spec["source_path"], array)
            elif spec["role"] == "junction":
                parent_fk = next((fk for fk in spec.get("foreign_keys", []) if fk["target_table"] == self.root["name"]), None)
                target_fk = next((fk for fk in spec.get("foreign_keys", []) if fk["target_table"] != self.root["name"]), None)
                if not parent_fk or not target_fk: continue
                rows = connection.execute(select(table).where(table.c[parent_fk["columns"][0]] == root_pk)).mappings().all()
                self._set_path(result, spec["source_path"], [row[target_fk["columns"][0]] for row in rows])
        return result

    def _side_row(self, spec: dict[str, Any], value: dict[str, Any], root_pk: Any) -> dict[str, Any]:
        row = {spec.get("id_column"): root_pk}
        prefix = spec.get("source_path", "") + "."
        for column in spec.get("columns", []):
            if column["name"] == spec.get("id_column"): continue
            source = column.get("source_path", "")
            relative = source[len(prefix):] if source.startswith(prefix) else source
            row[column["name"]] = self._to_sql(self._get_path(value, relative))
        return row

    def _child_row(self, spec: dict[str, Any], value: Any, root_pk: Any, position: int) -> dict[str, Any]:
        row: dict[str, Any] = {}
        parent_col = spec.get("id_column_parent")
        for column in spec.get("columns", []):
            name = column["name"]
            if name == parent_col: row[name] = root_pk
            elif name == "position": row[name] = position
            elif name == spec.get("id_column") or column.get("auto_increment"): continue
            else:
                path = column.get("source_path", "")
                prefix = spec["source_path"] + "[]."
                relative = path[len(prefix):] if path.startswith(prefix) else path
                row[name] = self._to_sql(value if name == "value" and path == spec["source_path"] else self._get_path(value, relative))
        return row

    def _write_junction(self, connection: Any, spec: dict[str, Any], table: Any, values: list[Any], root_pk: Any) -> None:
        from sqlalchemy import insert
        parent_fk = next((fk for fk in spec.get("foreign_keys", []) if fk["target_table"] == self.root["name"]), None)
        target_fk = next((fk for fk in spec.get("foreign_keys", []) if fk["target_table"] != self.root["name"]), None)
        if not parent_fk or not target_fk: return
        parent_col, target_col = parent_fk["columns"][0], target_fk["columns"][0]
        seen: set[str] = set()
        for position, value in enumerate(values):
            if value is None: continue
            if str(value) in seen: continue
            seen.add(str(value))
            row = {parent_col: root_pk, target_col: value}
            if "position" in table.c: row["position"] = position
            connection.execute(insert(table).values(**row))

    @staticmethod
    def _inserted_pk(result: Any, connection: Any, table: Any, row: dict[str, Any]) -> Any:
        if result.inserted_primary_key and result.inserted_primary_key[0] is not None:
            return result.inserted_primary_key[0]
        pk = list(table.primary_key.columns)
        if len(pk) == 1 and pk[0].name in row:
            return row[pk[0].name]
        if len(pk) == 1:
            return connection.execute(table.select().with_only_columns(pk[0]).order_by(pk[0].desc()).limit(1)).scalar_one()
        return tuple(row.get(column.name) for column in pk)

    @staticmethod
    def _get_path(value: Any, path: str) -> Any:
        current = value
        if not path: return current
        for part in path.split("."):
            part = part.replace("[]", "")
            if not isinstance(current, dict): return None
            current = current.get(part)
        return current

    @staticmethod
    def _set_path(target: dict[str, Any], path: str, value: Any) -> None:
        if not path: return
        parts = path.split(".")
        current = target
        for part in parts[:-1]:
            current = current.setdefault(part, {})
        current[parts[-1]] = value

    @staticmethod
    def _to_sql(value: Any, sql_type: str = "") -> Any:
        if isinstance(value, dict) or isinstance(value, list): return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
        if type(value).__name__.lower() == "decimal128": return value.to_decimal()
        if type(value).__name__.lower() == "objectid": return str(value)
        return value

    @staticmethod
    def _from_sql(value: Any) -> Any:
        if isinstance(value, (bytes, bytearray, memoryview)): return bytes(value)
        if isinstance(value, str) and value.startswith(("{", "[")):
            try: return json.loads(value)
            except json.JSONDecodeError: return value
        return value

    @staticmethod
    def _merge_patch(target: Any, patch: Any) -> Any:
        if not isinstance(patch, dict): return copy.deepcopy(patch)
        result = copy.deepcopy(target) if isinstance(target, dict) else {}
        for key, value in patch.items():
            if value is None: result.pop(key, None)
            else: result[key] = VerifiedSqlAggregateRepository._merge_patch(result.get(key), value) if isinstance(value, dict) else copy.deepcopy(value)
        return result


SqlRepository = VerifiedSqlAggregateRepository
