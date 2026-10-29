"""Forward validator, collection/index, and artifact generation for MongoDB targets."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader

from synapse.domain.ir.common import ForwardMapping
from synapse.domain.ir.relational import Relationship, RelationalIR, Table
from synapse.domain.typemap.constraints import relational_indexes
from synapse.domain.typemap.sql_to_bson import column_json_schema

_TEMPLATE_DIR = Path(__file__).with_name("templates")
_ENV = Environment(loader=FileSystemLoader(_TEMPLATE_DIR), autoescape=False, trim_blocks=True, lstrip_blocks=True)


@dataclass
class DocumentArtifacts:
    create_collections_js: str
    indexes_js: str
    validators_json: str
    etl_script: str
    report: str
    not_translated: list[dict[str, str]]
    validators: dict[str, Any]
    indexes: dict[str, list[dict[str, Any]]]


class DocumentEmitter:
    def __init__(self, *, options: dict[str, Any] | None = None):
        self.options = options or {}

    def emit(self, relational_ir: RelationalIR | dict[str, Any], relationships: list[Relationship] | list[dict[str, Any]], *, project_id: str = "") -> DocumentArtifacts:
        ir = relational_ir if isinstance(relational_ir, RelationalIR) else RelationalIR.model_validate(relational_ir)
        rels = [item if isinstance(item, Relationship) else Relationship.model_validate(item) for item in relationships]
        table_by_name = {table.qualified_name: table for table in ir.tables}
        table_by_name.update({table.table_name: table for table in ir.tables})
        collections: dict[str, Table] = {}
        embedded_children = {rel.child_table for rel in rels if rel.effective_mapping in {ForwardMapping.EMBED, ForwardMapping.EMBED_ARRAY}}
        junction_tables = {rel.child_table for rel in rels if rel.rule_id == "H2"}
        for table in ir.tables:
            if table.table_name not in embedded_children and table.qualified_name not in embedded_children and table.table_name not in junction_tables:
                collections[table.table_name] = table
        if not collections:
            collections = {table.table_name: table for table in ir.tables}
        not_translated: list[dict[str, str]] = []
        validators: dict[str, Any] = {}
        index_map: dict[str, list[dict[str, Any]]] = {}
        for name, table in sorted(collections.items()):
            validator = self._validator(table, rels, table_by_name, set())
            validators[name] = validator
            indexes = relational_indexes(table, rels)
            index_map[name] = indexes
            for check in table.checks:
                from synapse.domain.typemap.constraints import translate_check
                translated, warning = translate_check(check)
                if warning:
                    not_translated.append({"source": f"{name}.{check.name or check.sqltext}", "reason": warning})
            for fk in table.foreign_keys:
                not_translated.append({"source": f"{name}.{fk.name or fk.columns}", "reason": "MongoDB does not enforce foreign keys; runtime verification and application checks are required."})
            for column in table.columns:
                mapping = __import__("synapse.domain.typemap.sql_to_bson", fromlist=["map_sql_type"]).map_sql_type(column)
                if mapping.lossy:
                    not_translated.append({"source": f"{name}.{column.name} ({column.sql_type})", "reason": mapping.warning or "conversion may be lossy"})
        create_fragments: list[str] = []
        index_fragments: list[str] = []
        validator_template = _ENV.get_template("validator.js.j2")
        for name in sorted(collections):
            create_fragments.append(validator_template.render(collection=name, validator=validators[name], indexes=[]).strip())
            for index in index_map[name]:
                index_fragments.append(f"db[{json.dumps(name)}].createIndex({json.dumps(index['keys'], separators=(',', ':'))}, {json.dumps(index['options'], separators=(',', ':'))});")
        # Attach deterministic legacy-key and FK reference indexes after ordinary source indexes.
        for name, table in collections.items():
            if table.primary_key:
                index_fragments.append(f"db[{json.dumps(name)}].createIndex({{\"_legacy.pk\":1}}, {{unique:true,name:{json.dumps('uq_' + name + '_legacy_pk')}}});")
        report_lines = ["# SynapseMigrate forward generation report", "", f"Project: `{project_id or 'unassigned'}`", "", "## Collections", ""]
        report_lines.extend(f"- `{name}` — validator and `{len(index_map[name])}` source index plans" for name in sorted(collections))
        report_lines.extend(["", "## Not translated / lossy", ""])
        report_lines.extend([f"- **{item['source']}** — {item['reason']}" for item in not_translated] or ["- None identified."])
        return DocumentArtifacts(
            create_collections_js="\n\n".join(create_fragments) + "\n",
            indexes_js="\n".join(index_fragments) + "\n",
            validators_json=json.dumps(validators, sort_keys=True, indent=2, default=str) + "\n",
            etl_script=_ENV.get_template("etl_script.py.j2").render(),
            report="\n".join(report_lines) + "\n",
            not_translated=not_translated, validators=validators, indexes=index_map,
        )

    def _validator(self, table: Table, relationships: list[Relationship], table_map: dict[str, Table], seen: set[str]) -> dict[str, Any]:
        identity = table.qualified_name
        if identity in seen:
            return {"bsonType": "object", "properties": {"_id": {"bsonType": "objectId"}}}
        seen = set(seen) | {identity}
        properties: dict[str, Any] = {"_id": {"bsonType": "objectId"},
                                      "_version": {"bsonType": "int", "minimum": 1},
                                      "_legacy": {"bsonType": "object", "required": ["table", "pk"],
                                                   "properties": {"table": {"bsonType": "string"}, "pk": {"bsonType": "array"}}}}
        required: list[str] = []
        child_edges = [rel for rel in relationships if rel.child_table in {table.table_name, table.qualified_name}]
        omitted_fk_columns = {column for rel in child_edges if rel.effective_mapping in {ForwardMapping.EMBED, ForwardMapping.EMBED_ARRAY} for column in rel.foreign_key}
        for column in table.columns:
            if column.name in omitted_fk_columns:
                continue
            properties[column.name] = column_json_schema(column)
            if not column.nullable:
                required.append(column.name)
        # Standalone referenced documents carry one deterministic reference field per parent key group.
        for relation in child_edges:
            if relation.effective_mapping in {ForwardMapping.EMBED, ForwardMapping.EMBED_ARRAY, ForwardMapping.REFERENCE_ARRAY}:
                continue
            reference_name = relation.foreign_key[0] if len(relation.foreign_key) == 1 else f"{relation.parent_table.split('.')[-1]}_ref"
            properties[reference_name] = {"bsonType": "objectId", "description": f"reference to {relation.parent_table}; not enforced by MongoDB"}
        parent_edges = [rel for rel in relationships if rel.parent_table in {table.table_name, table.qualified_name}]
        for relation in sorted(parent_edges, key=lambda item: (item.child_table, item.rel_id)):
            mapping = relation.effective_mapping
            child_table = table_map.get(relation.child_table)
            if mapping == ForwardMapping.REFERENCE_ARRAY:
                junction = child_table
                other_edge = next((candidate for candidate in relationships
                                   if candidate.child_table in {junction.table_name, junction.qualified_name}
                                   and candidate.rel_id != relation.rel_id and candidate.rule_id == "H2"), None) if junction else None
                target_name = other_edge.parent_table.split(".")[-1] if other_edge else relation.child_table.split(".")[-1]
                properties[target_name] = {"bsonType": "array", "items": {"bsonType": "objectId"},
                                           "description": "references for a pure many-to-many junction"}
                continue
            if mapping not in {ForwardMapping.EMBED, ForwardMapping.EMBED_ARRAY} or child_table is None:
                continue
            child_schema = self._validator(child_table, relationships, table_map, seen)
            # `$jsonSchema` is a query-validator wrapper and is valid only at the root.
            # Embedded `properties`/`items` need the inner BSON schema directly.
            child_schema = child_schema.get("$jsonSchema", child_schema)
            # Remove root-only metadata from embedded objects and omit their parent FK.
            child_schema = {**child_schema, "properties": dict(child_schema.get("properties", {}))}
            child_schema["properties"].pop("_id", None)
            child_schema["properties"].pop("_legacy", None)
            for fk_column in relation.foreign_key:
                child_schema["properties"].pop(fk_column, None)
            child_schema["required"] = [field for field in child_schema.get("required", []) if field not in relation.foreign_key]
            field_name = child_table.table_name
            if mapping == ForwardMapping.EMBED_ARRAY:
                properties[field_name] = {"bsonType": "array", "items": child_schema}
            else:
                properties[field_name] = child_schema
        if required:
            required = sorted(set(required))
        result: dict[str, Any] = {"bsonType": "object", "properties": properties, "additionalProperties": False}
        if required:
            result["required"] = required
        return {"$jsonSchema": result}


def emit_document(relational_ir: RelationalIR | dict[str, Any], relationships: list[Relationship] | list[dict[str, Any]], *, options: dict[str, Any] | None = None, project_id: str = "") -> DocumentArtifacts:
    return DocumentEmitter(options=options).emit(relational_ir, relationships, project_id=project_id)
