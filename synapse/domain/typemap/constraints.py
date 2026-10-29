"""Native validator/index and SQL constraint translation helpers."""
from __future__ import annotations

import re
from typing import Any, Iterable

from synapse.domain.ir.document import Collection, MongoIndex
from synapse.domain.ir.relational import CheckConstraint, Relationship, Table
from synapse.domain.typemap.sql_to_bson import column_json_schema

_SIMPLE_CHECK = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(>=|<=|<>|!=|=|>|<)\s*(-?\d+(?:\.\d+)?)\s*$", re.I)
_SIMPLE_REGEXP = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s+(?:REGEXP|RLIKE)\s+(['\"])(.*?)\2\s*$", re.I)


def _column_by_name(table: Table) -> dict[str, Any]:
    return {column.name: column for column in table.columns}


def build_mongo_validator(table: Table, relationships: Iterable[Relationship] = (), tables: dict[str, Table] | None = None) -> dict[str, Any]:
    """Compile one root table and EMBED edges to MongoDB's `$jsonSchema` validator."""
    columns = _column_by_name(table)
    properties: dict[str, Any] = {"_id": {"bsonType": "objectId"}}
    required: list[str] = []
    child_tables = tables or {}
    edges = [rel for rel in relationships if rel.parent_table in {table.table_name, table.qualified_name}]
    embedded_fk_cols: set[str] = set()
    for edge in edges:
        if edge.effective_mapping.value in {"EMBED", "EMBED_ARRAY"}:
            embedded_fk_cols.update(edge.foreign_key)
    for column in table.columns:
        if column.name in embedded_fk_cols:
            continue
        schema = column_json_schema(column)
        properties[column.name] = schema
        if not column.nullable:
            required.append(column.name)
    for edge in sorted(edges, key=lambda relation: (relation.child_table, relation.rel_id)):
        mapping = edge.effective_mapping.value
        if mapping not in {"EMBED", "EMBED_ARRAY"}:
            for col_name, ref_name in zip(edge.foreign_key, edge.referenced_columns, strict=True):
                if col_name not in properties:
                    properties[f"{edge.parent_table}_{ref_name}"] = {"bsonType": "objectId", "description": f"reference to {edge.parent_table}"}
            continue
        child = child_tables.get(edge.child_table)
        if child is None:
            child = next((candidate for candidate in child_tables.values() if candidate.table_name == edge.child_table), None)
        if child is None:
            continue
        child_props = {column.name: column_json_schema(column) for column in child.columns if column.name not in edge.foreign_key}
        child_required = [column.name for column in child.columns if not column.nullable and column.name not in edge.foreign_key]
        child_schema = {"bsonType": "object", "properties": child_props}
        if child_required:
            child_schema["required"] = sorted(set(child_required))
        field_name = child.table_name
        if mapping == "EMBED_ARRAY":
            properties[field_name] = {"bsonType": "array", "items": child_schema}
        else:
            properties[field_name] = child_schema
        # An embedded field is required only where the relational relationship is 1:1 and source FK is NOT NULL.
    schema: dict[str, Any] = {"bsonType": "object", "properties": properties, "additionalProperties": False}
    if required:
        schema["required"] = sorted(set(required))
    return {"$jsonSchema": schema}


def relational_indexes(table: Table, relationships: Iterable[Relationship] = ()) -> list[dict[str, Any]]:
    """Return MongoDB createIndex specifications, preserving compound key order."""
    indexes: list[dict[str, Any]] = []
    for index in table.indexes:
        indexes.append({"keys": {column: 1 for column in index.columns}, "options": {"name": index.name or f"ix_{table.table_name}_{'_'.join(index.columns)}", "unique": index.unique}})
    for unique in table.uniques:
        indexes.append({"keys": {column: 1 for column in unique.columns}, "options": {"name": unique.name or f"uq_{table.table_name}_{'_'.join(unique.columns)}", "unique": True}})
    for rel in relationships:
        if rel.child_table == table.table_name or rel.child_table == table.qualified_name:
            keys = {column: 1 for column in rel.foreign_key}
            if keys and not any(item["keys"] == keys for item in indexes):
                indexes.append({"keys": keys, "options": {"name": f"ix_{table.table_name}_{'_'.join(rel.foreign_key)}", "unique": False}})
    if table.primary_key:
        legacy = {"_legacy.pk": 1}
        indexes.append({"keys": legacy, "options": {"name": f"uq_{table.table_name}_legacy_pk", "unique": True}})
    return indexes


def translate_check(constraint: CheckConstraint, dialect: str = "mysql") -> tuple[dict[str, Any] | None, str | None]:
    expression = constraint.sqltext.strip().removeprefix("CHECK").strip().strip("()")
    match = _SIMPLE_CHECK.match(expression)
    if match:
        field, operator, value = match.groups()
        number: int | float = float(value) if "." in value else int(value)
        if operator == ">=": return {"field": field, "minimum": number}, None
        if operator == ">": return {"field": field, "exclusiveMinimum": number}, None
        if operator == "<=": return {"field": field, "maximum": number}, None
        if operator == "<": return {"field": field, "exclusiveMaximum": number}, None
        if operator in {"=", "=="}: return {"field": field, "enum": [number]}, None
        return None, f"CHECK expression {constraint.sqltext!r} is not expressible in MongoDB JSON Schema"
    match = _SIMPLE_REGEXP.match(expression)
    if match:
        field, _, pattern = match.groups()
        return {"field": field, "pattern": pattern}, None
    return None, f"CHECK expression {constraint.sqltext!r} was not translated"


def translate_relational_constraints(table: Table) -> tuple[dict[str, Any], list[dict[str, str]], list[dict[str, Any]]]:
    """Return validator, Mongo index plan, and loss report for one relational table."""
    validator = build_mongo_validator(table)
    indexes = relational_indexes(table)
    not_translated: list[dict[str, str]] = []
    schema = validator["$jsonSchema"]
    for check in table.checks:
        translated, warning = translate_check(check)
        if translated:
            field = translated.pop("field")
            prop = schema["properties"].setdefault(field, {"bsonType": ["int", "long", "double", "decimal"]})
            prop.update(translated)
        elif warning:
            not_translated.append({"source": check.sqltext, "reason": warning})
    for fk in table.foreign_keys:
        not_translated.append({"source": f"FOREIGN KEY {fk.name or fk.columns}", "reason": "MongoDB validators cannot enforce foreign-key existence; ETL verifies the reference"})
    return validator, indexes, not_translated


def mongo_indexes_to_sql(collection: Collection, *, dialect: str, naming: dict[str, str] | None = None) -> tuple[list[str], list[dict[str, str]]]:
    naming = naming or {}
    mysql = dialect.lower().startswith("mysql")
    statements: list[str] = []
    not_translated: list[dict[str, str]] = []
    q = "`" if mysql else '"'
    for index in collection.indexes:
        keys = index.keys
        kind = index.index_type.lower()
        unsupported_kind = kind in {"text", "2dsphere", "hashed", "wildcard", "ttl"} or index.expire_after_seconds is not None
        if unsupported_kind:
            alternative = "scheduled purge or generated column / FULLTEXT / spatial index" if kind in {"ttl", "text", "2dsphere"} else "manual review"
            not_translated.append({"source": f"{collection.name}.{index.name} ({kind})", "reason": f"index type has no direct {dialect} equivalent; consider {alternative}"})
            continue
        if index.partial_filter:
            if mysql:
                not_translated.append({"source": f"{collection.name}.{index.name} (partial)", "reason": "partial indexes are unsupported by MySQL"})
                continue
            where = " WHERE " + " AND ".join(f"{q}{field}{q} IS NOT NULL" for field in index.partial_filter)
        else:
            where = ""
        cols: list[str] = []
        for field, direction in keys:
            mapped = naming.get(field, field.replace(".", "_"))
            order = "DESC" if direction == -1 else "ASC"
            cols.append(f"{q}{mapped}{q} {order}")
        if not cols:
            continue
        unique = "UNIQUE " if index.unique else ""
        name = re.sub(r"[^A-Za-z0-9_]", "_", index.name)
        table = re.sub(r"[^A-Za-z0-9_]", "_", collection.name)
        statements.append(f"CREATE {unique}INDEX {q}{name}{q} ON {q}{table}{q} ({', '.join(cols)}){where};")
    return statements, not_translated


def bson_schema_to_json_schema(schema: dict[str, Any]) -> dict[str, Any]:
    """Convert the useful Mongo `$jsonSchema` subset for API-side validation."""
    bson_to_json = {
        "objectId": "string", "int": "integer", "long": "integer", "double": "number", "decimal": "number",
        "bool": "boolean", "date": "string", "binData": "string", "object": "object", "array": "array", "string": "string",
    }
    result: dict[str, Any] = {}
    bson_type = schema.get("bsonType")
    if isinstance(bson_type, list):
        result["type"] = [bson_to_json.get(item, item) for item in bson_type]
    elif bson_type:
        result["type"] = bson_to_json.get(bson_type, bson_type)
    for key in ("required", "enum", "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "pattern", "minLength", "maxLength", "minItems", "maxItems"):
        if key in schema:
            mapped = {"minLength": "minLength", "maxLength": "maxLength"}.get(key, key)
            result[mapped] = schema[key]
    if "properties" in schema:
        result["properties"] = {key: bson_schema_to_json_schema(value) for key, value in schema["properties"].items()}
    if "items" in schema:
        result["items"] = bson_schema_to_json_schema(schema["items"])
    if "additionalProperties" in schema:
        result["additionalProperties"] = schema["additionalProperties"]
    return result


def validate_against_bson_schema(instance: Any, schema: dict[str, Any], path: str = "$", source: str = "api") -> list[dict[str, Any]]:
    """Return stable field-level errors for the BSON JSON Schema subset."""
    errors: list[dict[str, Any]] = []
    expected = schema.get("bsonType")
    accepted = expected if isinstance(expected, list) else [expected] if expected else []
    def bson_kind(value: Any) -> str:
        if value is None: return "null"
        if isinstance(value, bool): return "bool"
        if isinstance(value, int): return "int" if -(2**31) <= value < 2**31 else "long"
        if isinstance(value, float): return "double"
        if isinstance(value, str): return "string"
        if isinstance(value, list): return "array"
        if isinstance(value, dict): return "object"
        name = type(value).__name__.lower()
        if name == "objectid": return "objectId"
        if name == "decimal128": return "decimal"
        if name == "datetime": return "date"
        return name
    actual = bson_kind(instance)
    if accepted and actual not in accepted and not (actual in {"int", "long"} and any(item in accepted for item in ("int", "long"))):
        return [{"path": path, "code": "bson_type", "message": f"expected {accepted}, got {actual}", "source": source}]
    if isinstance(instance, dict):
        for key in schema.get("required", []):
            if key not in instance:
                errors.append({"path": f"{path}.{key}", "code": "required", "message": "field is required", "source": source})
        for key, value in instance.items():
            child_schema = schema.get("properties", {}).get(key)
            if child_schema is None:
                if schema.get("additionalProperties") is False:
                    errors.append({"path": f"{path}.{key}", "code": "additional_property", "message": "additional property is not allowed", "source": source})
                continue
            errors.extend(validate_against_bson_schema(value, child_schema, f"{path}.{key}", source))
    if isinstance(instance, list) and "items" in schema:
        for index, value in enumerate(instance):
            errors.extend(validate_against_bson_schema(value, schema["items"], f"{path}[{index}]", source))
    if isinstance(instance, str):
        if schema.get("maxLength") is not None and len(instance) > schema["maxLength"]:
            errors.append({"path": path, "code": "maxLength", "message": f"must be at most {schema['maxLength']} characters", "source": source})
        if schema.get("enum") is not None and instance not in schema["enum"]:
            errors.append({"path": path, "code": "enum", "message": "value is not in the allowed enum", "source": source})
        if schema.get("pattern") and not re.search(schema["pattern"], instance):
            errors.append({"path": path, "code": "pattern", "message": "value does not match the required pattern", "source": source})
    if isinstance(instance, (int, float)) and not isinstance(instance, bool):
        if "minimum" in schema and instance < schema["minimum"]: errors.append({"path": path, "code": "minimum", "message": f"must be >= {schema['minimum']}", "source": source})
        if "maximum" in schema and instance > schema["maximum"]: errors.append({"path": path, "code": "maximum", "message": f"must be <= {schema['maximum']}", "source": source})
    return errors
