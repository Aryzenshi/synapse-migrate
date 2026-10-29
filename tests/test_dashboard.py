"""Serve the browser dashboard and its read-only guided walkthrough from the API."""
from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from synapse.adapters.state.file_store import FileStateStore
from synapse.api.main import create_app
from synapse.api.store import ApiRepository

DEMO_DB = Path(__file__).resolve().parents[1] / "demo" / "synapsemigrate_demo.db"


def make_client(tmp_path: Path, monkeypatch) -> TestClient:
    monkeypatch.setenv("SYNAPSE_API_KEYS", "dashboard-token:admin")
    monkeypatch.setenv("SYNAPSE_MASTER_KEY", "dashboard-test-master-key")
    repository = ApiRepository(FileStateStore(tmp_path / "state"), tmp_path / "jobs")
    return TestClient(create_app(repository=repository))


def test_dashboard_shell_assets_and_sqlite_demo_endpoint(tmp_path, monkeypatch) -> None:
    with make_client(tmp_path, monkeypatch) as client:
        page = client.get("/")
        assert page.status_code == 200
        assert "SynapseMigrate · Workspace" in page.text
        assert "/dashboard/js/app.js" in page.text
        assert 'id="plan-target-connection"' in page.text
        # Database content browsing is its own sidebar tab and its own view section.
        assert 'data-view="database-browser"' in page.text
        assert 'data-view="connections"' in page.text and 'data-view="projects"' in page.text
        assert '<section id="view-database-browser" class="view">' in page.text
        assert 'Browse tables, views, and collections' in page.text
        app_js = client.get("/dashboard/js/app.js")
        styles = client.get("/dashboard/styles.css")
        assert app_js.status_code == 200 and "history.replaceState" in app_js.text
        assert "synapsemigrate.current-location" in app_js.text and "savedLocation.projectId" in app_js.text
        assert "savedLocation.view" in app_js.text and "savedObject" in app_js.text
        assert styles.status_code == 200 and ".browser-sample pre" in styles.text and "overflow-wrap:anywhere" in styles.text
        assert client.get("/dashboard/js/api.js").status_code == 200

        response = client.get("/api/demo/sqlite", headers={"Authorization": "Bearer dashboard-token"})
        assert response.status_code == 200
        assert Path(response.json()["database"]).is_file()


def test_compose_mongodb_container_has_no_persistent_data_volume() -> None:
    compose = Path(__file__).resolve().parents[1] / "docker-compose.yml"
    text = compose.read_text(encoding="utf-8")
    mongo_service = text.split("  mongo:\n", 1)[1].split("  mongo-init:\n", 1)[0]
    assert "volumes:" not in mongo_service
    assert "mongo_data:" not in text


def test_duplicate_endpoint_role_is_rejected_and_can_be_added_after_removal(tmp_path, monkeypatch) -> None:
    import synapse.api.main as api_main

    monkeypatch.setattr(api_main, "verify_connection", lambda engine, credentials: {"server_version": "test"})
    headers = {"Authorization": "Bearer dashboard-token"}
    with make_client(tmp_path, monkeypatch) as client:
        payload = {"engine": "sqlite", "role": "source", "name": "Sample A",
                   "database": str(DEMO_DB), "ttl_seconds": 3600}
        first = client.post("/api/connections", headers=headers, json=payload)
        assert first.status_code == 201, first.text
        repeated = client.post("/api/connections", headers=headers, json={**payload, "name": "Sample renamed"})
        assert repeated.status_code == 409
        assert repeated.json()["code"] == "connection_exists"

        removed = client.delete(f"/api/connections/{first.json()['connection_id']}", headers=headers)
        assert removed.status_code == 204
        allowed_again = client.post("/api/connections", headers=headers, json=payload)
        assert allowed_again.status_code == 201, allowed_again.text


def test_database_browser_deduplicates_source_and_target_and_reads_sqlite_rows(tmp_path, monkeypatch) -> None:
    import synapse.api.main as api_main

    monkeypatch.setattr(api_main, "verify_connection", lambda engine, credentials: {"server_version": "test"})
    headers = {"Authorization": "Bearer dashboard-token"}
    with make_client(tmp_path, monkeypatch) as client:
        uri = "mongodb://127.0.0.1:27017/?directConnection=true"
        for role in ("source", "target"):
            response = client.post("/api/connections", headers=headers, json={
                "engine": "mongodb", "role": role, "name": f"Mongo {role}", "database": "shop",
                "uri": uri, "ttl_seconds": 3600,
            })
            assert response.status_code == 201, response.text
        grouped = client.get("/api/database-browser", headers=headers)
        assert grouped.status_code == 200
        mongo_groups = [item for item in grouped.json()["items"] if item["engine"] == "mongodb"]
        assert len(mongo_groups) == 1
        assert set(mongo_groups[0]["roles"]) == {"source", "target"}
        assert len(mongo_groups[0]["connection_ids"]) == 2

    # Separately exercise actual, read-only object discovery and row preview on the bundled SQLite file.
    with make_client(tmp_path / "sqlite", monkeypatch) as client:
        connected = client.post("/api/connections", headers=headers, json={
            "engine": "sqlite", "role": "source", "name": "SQLite demo", "database": str(DEMO_DB), "ttl_seconds": 3600,
        })
        assert connected.status_code == 201, connected.text
        connection_id = connected.json()["connection_id"]
        objects = client.get(f"/api/database-browser/{connection_id}/objects", headers=headers)
        assert objects.status_code == 200
        assert {item["name"] for item in objects.json()["items"]} >= {"users", "orders"}
        rows = client.get(f"/api/database-browser/{connection_id}/sample", headers=headers,
                          params={"object_name": "users", "limit": 5})
        assert rows.status_code == 200
        assert len(rows.json()["items"]) == 2
        assert rows.json()["items"][0]["email"]


def test_mongodb_connection_requires_an_explicit_database_name(tmp_path, monkeypatch) -> None:
    import synapse.api.main as api_main

    monkeypatch.setattr(api_main, "verify_connection", lambda engine, credentials: {"server_version": "test"})
    headers = {"Authorization": "Bearer dashboard-token"}
    with make_client(tmp_path, monkeypatch) as client:
        missing = client.post("/api/connections", headers=headers, json={
            "engine": "mongodb", "role": "target", "name": "Mongo target",
            "uri": "mongodb://127.0.0.1:27017/?directConnection=true", "ttl_seconds": 3600,
        })
        assert missing.status_code == 400
        assert missing.json()["title"] == "MongoDB database name required"

        supplied = client.post("/api/connections", headers=headers, json={
            "engine": "mongodb", "role": "target", "name": "Mongo target",
            "uri": "mongodb://127.0.0.1:27017/?directConnection=true",
            "database": "synapsemigrate_demo", "ttl_seconds": 3600,
        })
        assert supplied.status_code == 201, supplied.text
        assert supplied.json()["database"] == "synapsemigrate_demo"


def test_connection_live_check_changes_from_successful_to_failed(tmp_path, monkeypatch) -> None:
    import synapse.api.main as api_main

    reachable = {"value": True}
    def fake_verify(engine, credentials):
        if not reachable["value"]:
            raise OSError("database stopped")
        return {"server_version": "test"}

    monkeypatch.setattr(api_main, "verify_connection", fake_verify)
    headers = {"Authorization": "Bearer dashboard-token"}
    with make_client(tmp_path, monkeypatch) as client:
        connected = client.post("/api/connections", headers=headers, json={
            "engine": "sqlite", "role": "source", "name": "Sample",
            "database": str(DEMO_DB), "ttl_seconds": 3600,
        })
        assert connected.status_code == 201
        connection_id = connected.json()["connection_id"]
        assert connected.json()["connection_status"] == "successful"

        reachable["value"] = False
        checked = client.post(f"/api/connections/{connection_id}/test", headers=headers, json={})
        assert checked.status_code == 200
        assert checked.json()["connection_status"] == "failed"
        listed = client.get("/api/connections", headers=headers)
        assert listed.json()["items"][0]["connection_status"] == "failed"


def test_failed_migration_exposes_only_sanitized_error_location(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("SYNAPSE_API_KEYS", "dashboard-token:admin")
    repository = ApiRepository(FileStateStore(tmp_path / "state"), tmp_path / "jobs")
    repository.save_job({
        "job_id": "job_failed", "job_type": "migration", "status": "failed",
        "error": "list indices must be integers or slices, not str", "error_code": "worker_error",
        "traceback": 'Traceback (most recent call last):\n  File "C:/app/synapse/workers/runtime.py", line 234, in _apply_mongo_schema\n    collection.create_index(index["keys"])\nTypeError: list indices must be integers or slices, not str',
    })
    with TestClient(create_app(repository=repository)) as client:
        response = client.get("/api/migrations/job_failed", headers={"Authorization": "Bearer dashboard-token"})
    assert response.status_code == 200
    assert response.json()["error_location"] == "runtime.py:234 (_apply_mongo_schema)"
    assert "traceback" not in response.json()


def test_guided_demo_uses_real_sqlite_rows_but_never_writes_to_a_target(tmp_path, monkeypatch) -> None:
    headers = {"Authorization": "Bearer dashboard-token"}
    with make_client(tmp_path, monkeypatch) as client:
        demo = client.get("/api/demo/sqlite", headers=headers).json()
        connected = client.post("/api/connections", headers=headers, json={
            "engine": "sqlite", "role": "source", "name": "Demo source",
            "database": demo["database"], "ttl_seconds": 3600,
        })
        assert connected.status_code == 201, connected.text

        analyzed = client.post("/api/schema/introspect", headers=headers, json={
            "source_type": "sqlite", "connection_id": connected.json()["connection_id"],
            "options": {"exact_row_counts": True, "infer_implicit_fks": True}, "wait": True,
        })
        assert analyzed.status_code == 200, analyzed.text
        project = analyzed.json()
        sample_head = client.get(f"/api/schema/ir/{project['project_id']}/sample?limit=5", headers=headers)
        assert sample_head.status_code == 200, sample_head.text
        sample_groups = sample_head.json()["items"]
        assert len(sample_groups) == 5
        assert {item["name"] for item in sample_groups} == {"users", "orders", "order_items", "tags", "order_tags"}
        assert all(len(item["rows"]) <= 5 for item in sample_groups)
        generated = client.post("/api/schema/generate", headers=headers, json={
            "project_id": project["project_id"], "target_engine": "mongodb", "if_revision": project["revision"],
            "options": {"etl": {"batch_size": 2}}, "acknowledge_advisories": [],
        })
        assert generated.status_code == 201, generated.text

        walkthrough = client.post("/api/demo/visualize", headers=headers, json={"plan_id": generated.json()["plan_id"]})
        assert walkthrough.status_code == 200, walkthrough.text
        result = walkthrough.json()
        assert result["mode"] == "visualization_only"
        assert result["writes_performed"] is False
        assert result["summary"]["source_rows"] == 10
        assert result["summary"]["target_writes"] == 0
        assert any(row["sample_documents"] for row in result["tables"])
        assert result["stages"][-1]["status"] == "simulated"
