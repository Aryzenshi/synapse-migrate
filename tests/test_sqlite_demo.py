from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from synapse.adapters.connections import create_sql_engine, verify_connection
from synapse.adapters.extractors.relational import RelationalExtractor
from synapse.adapters.state.file_store import FileStateStore
from synapse.api.main import create_app
from synapse.api.store import ApiRepository


DEMO_DB = Path(__file__).resolve().parents[1] / "demo" / "synapsemigrate_demo.db"


def test_bundled_sqlite_file_connects_and_exposes_seeded_relational_schema() -> None:
    assert DEMO_DB.is_file()
    result = verify_connection("sqlite", {"database": str(DEMO_DB)})
    assert result["server_version"]

    engine = create_sql_engine("sqlite", {"database": str(DEMO_DB)})
    try:
        with engine.connect() as connection:
            ir = RelationalExtractor().extract(connection, {"exact_row_counts": True})
    finally:
        engine.dispose()

    tables = {table.table_name: table for table in ir.tables}
    assert set(tables) == {"users", "orders", "order_items", "tags", "order_tags"}
    assert tables["users"].stats.row_estimate == 2
    assert tables["orders"].stats.row_estimate == 2
    assert sum(table.stats.row_estimate for table in tables.values()) == 10
    assert tables["order_items"].primary_key.columns == ["order_id", "line_no"]
    assert [fk.columns for fk in tables["order_tags"].foreign_keys] == [["order_id"], ["tag_id"]]


def test_sqlite_demo_connection_can_be_introspected_through_the_api(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("SYNAPSE_API_KEYS", "sqlite-demo-token:admin")
    monkeypatch.setenv("SYNAPSE_MASTER_KEY", "unit-test-demo-master-key")
    repository = ApiRepository(FileStateStore(tmp_path / "state"), tmp_path / "jobs")
    app = create_app(repository=repository)

    with TestClient(app) as client:
        headers = {"Authorization": "Bearer sqlite-demo-token"}
        connected = client.post("/api/connections", headers=headers, json={
            "engine": "sqlite", "role": "source", "name": "Bundled SQLite demo",
            "database": str(DEMO_DB), "ttl_seconds": 3600,
        })
        assert connected.status_code == 201, connected.text

        analysis = client.post("/api/schema/introspect", headers=headers, json={
            "source_type": "sqlite",
            "connection_id": connected.json()["connection_id"],
            "options": {"exact_row_counts": True, "infer_implicit_fks": True},
            "wait": True,
        })
        assert analysis.status_code == 200, analysis.text
        assert analysis.json()["ir_kind"] == "relational"
        assert analysis.json()["summary"]["tables"] == 5
        assert analysis.json()["tables"]
