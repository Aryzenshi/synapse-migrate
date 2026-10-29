"""Stateless API repository on the Redis/file StateStore plus immutable artifact files."""
from __future__ import annotations

import hashlib
import json
import os
import secrets
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from synapse.adapters.state.base import StateStore, encode


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def opaque_id(prefix: str) -> str:
    return f"{prefix}_{datetime.now(timezone.utc).strftime('%y%m%d%H%M%S')}{secrets.token_hex(5)}"


def etag_for(value: Any) -> str:
    body = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str).encode("utf-8")
    return hashlib.sha256(body).hexdigest()[:16]


class ApiRepository:
    def __init__(self, store: StateStore, data_dir: str | Path):
        self.store = store
        self.data_dir = Path(data_dir).expanduser().resolve()
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.artifact_dir = self.data_dir / "artifacts"
        self.artifact_dir.mkdir(parents=True, exist_ok=True)
        self.job_dir = self.data_dir / "jobs"
        self.job_dir.mkdir(parents=True, exist_ok=True)

    def save_project(self, project: dict[str, Any], *, expected_revision: int | None = None, expected_etag: str | None = None) -> bool:
        project["etag"] = etag_for({"ir": project.get("ir", {}), "advisories": project.get("advisories", []), "thresholds": project.get("thresholds", {})})
        return self.store.compare_and_set(f"project:{project['project_id']}", expected_revision, expected_etag, project)

    def get_project(self, project_id: str) -> dict[str, Any] | None:
        return self.store.get(f"project:{project_id}")

    def list_projects(self) -> list[dict[str, Any]]:
        return [value for _, value in self.store.list_prefix("project:") if isinstance(value, dict)]

    def delete_project(self, project_id: str) -> bool:
        return self.store.delete(f"project:{project_id}")

    def save_connection(self, metadata: dict[str, Any]) -> None:
        self.store.set(f"connection:{metadata['connection_id']}", metadata)

    def get_connection(self, connection_id: str) -> dict[str, Any] | None:
        return self.store.get(f"connection:{connection_id}")

    def list_connections(self) -> list[dict[str, Any]]:
        return [value for _, value in self.store.list_prefix("connection:") if isinstance(value, dict)]

    def delete_connection(self, connection_id: str) -> bool:
        return self.store.delete(f"connection:{connection_id}")

    def save_plan(self, plan: dict[str, Any]) -> None:
        self.store.set(f"plan:{plan['plan_id']}", plan)

    def get_plan(self, plan_id: str) -> dict[str, Any] | None:
        return self.store.get(f"plan:{plan_id}")

    def list_plans(self) -> list[dict[str, Any]]:
        return [value for _, value in self.store.list_prefix("plan:") if isinstance(value, dict)]

    def save_job(self, job: dict[str, Any]) -> None:
        job.setdefault("updated_at", utc_now())
        self.store.set(f"job:{job['job_id']}", job)
        self.append_event(job["job_id"], {"event": "state", "status": job.get("status"), "updated_at": job["updated_at"]})

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        return self.store.get(f"job:{job_id}")

    def list_jobs(self) -> list[dict[str, Any]]:
        return [value for _, value in self.store.list_prefix("job:") if isinstance(value, dict)]

    def append_event(self, job_id: str, event: dict[str, Any]) -> None:
        sequence_key = f"job-event-seq:{job_id}"
        sequence = int(self.store.get(sequence_key) or 0) + 1
        event = {**event, "id": sequence, "job_id": job_id, "ts": event.get("ts", utc_now())}
        self.store.set(sequence_key, sequence)
        self.store.set(f"job-event:{job_id}:{sequence:012d}", event, ttl_seconds=7 * 24 * 3600)

    def list_events(self, job_id: str, after_id: int = 0) -> list[dict[str, Any]]:
        prefix = f"job-event:{job_id}:"
        items = [value for _, value in self.store.list_prefix(prefix) if isinstance(value, dict) and int(value.get("id", 0)) > after_id]
        return sorted(items, key=lambda item: int(item.get("id", 0)))

    def save_artifact(self, *, artifact_id: str, plan_id: str, name: str, kind: str, content: str | bytes) -> dict[str, Any]:
        data = content.encode("utf-8") if isinstance(content, str) else content
        digest = hashlib.sha256(data).hexdigest()
        safe_name = Path(name).name
        path = self.artifact_dir / f"{artifact_id}.bin"
        tmp = self.artifact_dir / f".{artifact_id}.{secrets.token_hex(3)}.tmp"
        with tmp.open("wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
        metadata = {"artifact_id": artifact_id, "plan_id": plan_id, "name": safe_name, "kind": kind,
                    "bytes": len(data), "sha256": digest, "path": str(path), "url": f"/api/artifacts/{artifact_id}"}
        self.store.set(f"artifact:{artifact_id}", metadata)
        return metadata

    def get_artifact(self, artifact_id: str) -> tuple[dict[str, Any], bytes] | None:
        metadata = self.store.get(f"artifact:{artifact_id}")
        if not metadata:
            return None
        try:
            data = Path(metadata["path"]).read_bytes()
        except OSError:
            return None
        return metadata, data

    def artifacts_for_plan(self, plan_id: str) -> list[dict[str, Any]]:
        return sorted([value for _, value in self.store.list_prefix("artifact:") if isinstance(value, dict) and value.get("plan_id") == plan_id], key=lambda item: item["name"])

    def idempotency_get(self, principal: str, key: str) -> dict[str, Any] | None:
        return self.store.get(f"idempotency:{principal}:{key}")

    def idempotency_set(self, principal: str, key: str, body_hash: str, response: dict[str, Any]) -> None:
        self.store.set(f"idempotency:{principal}:{key}", {"body_hash": body_hash, "response": response}, ttl_seconds=24 * 3600)

    def save_connection_job_marker(self, connection_id: str, job_id: str, active: bool) -> None:
        key = f"connection-jobs:{connection_id}"
        current = set(self.store.get(key) or [])
        current.add(job_id) if active else current.discard(job_id)
        self.store.set(key, sorted(current))
