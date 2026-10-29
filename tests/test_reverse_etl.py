"""Reverse ETL regression tests for BSON source-key serialization."""
from __future__ import annotations

from contextlib import nullcontext
from types import SimpleNamespace

from sqlalchemy import create_engine, text

from synapse.etl.reverse import ReverseETLRunner


class ObjectId:
    def __str__(self) -> str:
        return "0123456789abcdef01234567"


def test_phase1_accepts_objectid_encoded_as_dictionary_source_key() -> None:
    runner = ReverseETLRunner.__new__(ReverseETLRunner)
    runner._row_errors = 0
    runner.plan = SimpleNamespace(plan_hash="plan-hash")
    runner.checkpoints = SimpleNamespace(job_id="job-1", write_reverse_checkpoint=lambda *_: None)
    runner.idmap = SimpleNamespace(put_many=lambda *_: None)
    runner._connect = lambda: nullcontext(object())
    runner._build_root_row = lambda _root, document: {"legacy_id": str(document["_id"])}
    runner._safe_upsert = lambda _connection, _root, records, _stage: records
    runner._readback_ids = lambda _connection, _root, records: {
        runner._legacy_value_key(record["row"]["legacy_id"]): 42 for record in records
    }
    runner._side_tables = lambda _root: []
    runner._fence = lambda: 0

    mapping_entries: list[tuple[str, object, bytes, bool]] = []
    root = {"legacy_id_column": "legacy_id", "collection": "orders", "name": "orders"}
    committed, errors = runner._phase1_batch(
        root, [{"_id": ObjectId()}], "orders", batch_seq=1, rows_done=1,
        docs_written_so_far=0, errors_so_far=0, last_key=[], mapping_entries=mapping_entries,
    )

    assert (committed, errors) == (1, 0)
    assert len(mapping_entries) == 1
    assert mapping_entries[0][0] == "orders"


def test_internal_reverse_idmap_keeps_mongo_id_out_of_root_table_and_makes_upserts_idempotent() -> None:
    runner = ReverseETLRunner.__new__(ReverseETLRunner)
    runner.plan = SimpleNamespace(project_id="project-1", options={
        "reverse_idmap_table": "_synapse_reverse_idmap",
        "reverse_idmap_namespace": "source-connection-1",
    })
    root = {
        "name": "orders", "collection": "orders", "id_column": "id",
        "columns": [
            {"name": "id", "sql_type": "BIGINT", "auto_increment": True},
            {"name": "name", "sql_type": "VARCHAR(255)"},
        ],
    }
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE orders (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)"))
        connection.execute(text("""
            CREATE TABLE _synapse_reverse_idmap (
                source_namespace TEXT NOT NULL,
                source_collection TEXT NOT NULL,
                source_key_hash TEXT NOT NULL,
                target_table TEXT NOT NULL,
                target_id INTEGER NOT NULL,
                PRIMARY KEY (source_namespace, source_collection, source_key_hash)
            )
        """))
        source_id = ObjectId()
        first_id = runner._upsert_root_identity(connection, root, {"name": "before"}, source_id)
        same_id = runner._upsert_root_identity(connection, root, {"name": "after"}, source_id)
        rows = connection.execute(text("SELECT id, name FROM orders")).all()

    engine.dispose()
    assert first_id == same_id
    assert rows == [(first_id, "after")]
