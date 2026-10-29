from __future__ import annotations

from fastapi.testclient import TestClient

from synapse.adapters.state.file_store import FileStateStore
from synapse.api.main import create_app
from synapse.api.store import ApiRepository


def _relationship_project() -> dict:
    return {
        "project_id": "prj_composite",
        "revision": 1,
        "ir_kind": "relational",
        "source_engine": "mysql",
        "direction": "RELATIONAL_TO_DOCUMENT",
        "status": "ready",
        "ir": {
            "tables": [],
            "relationships": [
                {
                    "rel_id": "rel_orders_items",
                    "parent_table": "orders",
                    "child_table": "items",
                    "foreign_key": ["tenant_id", "order_id"],
                    "referenced_columns": ["tenant_id", "id"],
                    "cardinality": "1:N",
                    "proposed_mapping": "EMBED_ARRAY",
                    "user_override": None,
                    "rule_id": "H4",
                    "rationale": "bounded fan-out",
                    "inferred": False,
                    "advisory_ids": [],
                    "fk_name": "fk_items_orders",
                    "fanout": {"avg": 2.0, "p50": 2, "p95": 3, "p99": 5, "max": 8, "source": "exact"},
                    "estimated_embedded_bytes": 500,
                    "override_reason": None,
                }
            ],
        },
        "advisories": [],
        "thresholds": {},
        "history": [],
        "author": "test",
        "created_at": "2026-10-21T00:00:00Z",
        "updated_at": "2026-10-21T00:00:00Z",
    }


def test_api_authenticates_and_ir_mutations_use_if_match_revision_cas(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("SYNAPSE_API_KEYS", "test-admin-token:admin")
    store = FileStateStore(tmp_path / "state")
    repository = ApiRepository(store, tmp_path / "jobs")
    project = _relationship_project()
    assert repository.save_project(project, expected_revision=None)
    app = create_app(repository=repository)

    with TestClient(app) as client:
        unauthenticated = client.get("/api/version")
        assert unauthenticated.status_code == 401

        headers = {"Authorization": "Bearer test-admin-token"}
        response = client.get("/api/schema/ir/prj_composite", headers=headers)
        assert response.status_code == 200
        original_etag = response.json()["etag"]
        assert response.headers["etag"] == f'"{original_etag}"'
        assert response.json()["relationships"][0]["foreign_key"] == ["tenant_id", "order_id"]

        missing_precondition = client.patch(
            "/api/schema/ir/prj_composite/relationship/rel_orders_items",
            headers=headers,
            json={"user_override": "REFERENCE"},
        )
        assert missing_precondition.status_code == 428

        stale_precondition = client.patch(
            "/api/schema/ir/prj_composite/relationship/rel_orders_items",
            headers={**headers, "If-Match": '"stale-etag"'},
            json={"user_override": "REFERENCE"},
        )
        assert stale_precondition.status_code == 412

        updated = client.patch(
            "/api/schema/ir/prj_composite/relationship/rel_orders_items",
            headers={**headers, "If-Match": f'"{original_etag}"'},
            json={"user_override": "REFERENCE", "reason": "Keep a shared child separate"},
        )
        assert updated.status_code == 200, updated.text
        assert updated.json()["revision"] == 2
        assert updated.json()["relationship"]["effective_mapping"] == "REFERENCE"
        assert updated.headers["etag"] == f'"{updated.json()["etag"]}"'

        stale_retry = client.patch(
            "/api/schema/ir/prj_composite/relationship/rel_orders_items",
            headers={**headers, "If-Match": f'"{original_etag}"'},
            json={"user_override": "REFERENCE"},
        )
        assert stale_retry.status_code == 412
        history = client.get("/api/schema/ir/prj_composite/revisions", headers=headers)
        assert history.status_code == 200
        assert [item["revision"] for item in history.json()["items"]] == [1, 2]
