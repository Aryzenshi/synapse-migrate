from __future__ import annotations

from synapse.adapters.emitters.document_emitter import DocumentEmitter
from synapse.domain.ir.relational import RelationalIR


def test_embedded_validator_uses_raw_child_schema_not_nested_jsonschema_wrapper() -> None:
    ir = RelationalIR.model_validate({
        "tables": [
            {"table_name": "orders", "columns": [
                {"name": "id", "sql_type": "INTEGER", "canonical_type": "int", "nullable": False},
                {"name": "customer", "sql_type": "VARCHAR", "canonical_type": "string", "nullable": True},
            ]},
            {"table_name": "items", "columns": [
                {"name": "id", "sql_type": "INTEGER", "canonical_type": "int", "nullable": False},
                {"name": "order_id", "sql_type": "INTEGER", "canonical_type": "int", "nullable": False},
                {"name": "sku", "sql_type": "VARCHAR", "canonical_type": "string", "nullable": False},
            ]},
        ],
        "relationships": [{
            "rel_id": "orders_items", "parent_table": "orders", "child_table": "items",
            "foreign_key": ["order_id"], "referenced_columns": ["id"], "cardinality": "1:N",
            "proposed_mapping": "EMBED_ARRAY", "rule_id": "H4", "rationale": "bounded child collection",
        }],
    })

    artifacts = DocumentEmitter().emit(ir, ir.relationships)
    validator = artifacts.validators["orders"]
    embedded_item = validator["$jsonSchema"]["properties"]["items"]["items"]

    assert embedded_item["bsonType"] == "object"
    assert "$jsonSchema" not in embedded_item
    assert embedded_item["required"] == ["id", "sku"]
    assert "order_id" not in embedded_item["properties"]
    _assert_only_root_schema_wrapper(validator)


def test_nested_object_and_array_embeddings_have_no_nested_schema_wrappers() -> None:
    ir = RelationalIR.model_validate({
        "tables": [
            {"table_name": "orders", "columns": [
                {"name": "id", "sql_type": "INTEGER", "canonical_type": "int", "nullable": False},
            ]},
            {"table_name": "items", "columns": [
                {"name": "id", "sql_type": "INTEGER", "canonical_type": "int", "nullable": False},
                {"name": "order_id", "sql_type": "INTEGER", "canonical_type": "int", "nullable": False},
                {"name": "sku", "sql_type": "VARCHAR", "canonical_type": "string", "nullable": False},
            ]},
            {"table_name": "parts", "columns": [
                {"name": "id", "sql_type": "INTEGER", "canonical_type": "int", "nullable": False},
                {"name": "item_id", "sql_type": "INTEGER", "canonical_type": "int", "nullable": False},
                {"name": "part_no", "sql_type": "VARCHAR", "canonical_type": "string", "nullable": False},
            ]},
        ],
        "relationships": [
            {"rel_id": "orders_items", "parent_table": "orders", "child_table": "items",
             "foreign_key": ["order_id"], "referenced_columns": ["id"], "cardinality": "1:1",
             "proposed_mapping": "EMBED", "rule_id": "H1", "rationale": "single embedded child"},
            {"rel_id": "items_parts", "parent_table": "items", "child_table": "parts",
             "foreign_key": ["item_id"], "referenced_columns": ["id"], "cardinality": "1:N",
             "proposed_mapping": "EMBED_ARRAY", "rule_id": "H4", "rationale": "nested embedded parts"},
        ],
    })

    artifacts = DocumentEmitter().emit(ir, ir.relationships)
    validator = artifacts.validators["orders"]
    root_schema = validator["$jsonSchema"]
    embedded_item = root_schema["properties"]["items"]
    embedded_parts = embedded_item["properties"]["parts"]["items"]

    assert embedded_item["bsonType"] == "object"
    assert embedded_parts["bsonType"] == "object"
    assert "item_id" not in embedded_parts["properties"]
    _assert_only_root_schema_wrapper(validator)


def _assert_only_root_schema_wrapper(validator: dict) -> None:
    """MongoDB permits `$jsonSchema` as the validator wrapper only at the root."""
    def walk(value):
        if isinstance(value, dict):
            assert "$jsonSchema" not in value or value is validator
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)
    walk(validator)
