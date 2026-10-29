"""SynapseMigrate stateless FastAPI service."""
from __future__ import annotations

import hashlib
import hmac
import io
import json
import logging
import os
import re
import secrets
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlsplit, urlunsplit, quote, unquote

from fastapi import Body, Depends, FastAPI, Header, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.staticfiles import StaticFiles

from synapse.adapters.connections import create_mongo_client, create_sql_engine, verify_connection
from synapse.adapters.repositories.mongo_repo import MongoDocumentNotFound, MongoValidationError, MongoVersionConflict, VerifiedMongoRepository
from synapse.adapters.repositories.sql_repo import SqlAggregateNotFound, SqlValidationError, SqlVersionConflict, VerifiedSqlAggregateRepository
from synapse.adapters.state.base import ResilientStateStore
from synapse.adapters.state.file_store import FileStateStore
from synapse.adapters.vault import CredentialExpiredError
from synapse.api.runtime import build_repository, build_state_store, build_vault
from synapse.api.schema_service import compile_plan, run_introspection
from synapse.api.store import ApiRepository, etag_for, opaque_id, utc_now
from synapse.domain.heuristics.forward import apply_forward_overrides, propose_forward
from synapse.domain.heuristics.reverse import allowed_reverse_overrides, apply_reverse_overrides, propose_reverse
from synapse.domain.heuristics.thresholds import Thresholds
from synapse.domain.heuristics.workload import analyze_workload
from synapse.domain.ir.common import ForwardMapping
from synapse.domain.ir.document import DocumentIR, FieldKind, ReverseAction
from synapse.domain.ir.plan import ApprovedPlan
from synapse.domain.ir.relational import RelationalIR
from synapse.etl.checkpoint import CheckpointManager
from synapse.etl.dlq import DeadLetterLog
from synapse.etl.keys import decode_cursor, encode_cursor
from synapse.workers.runtime import JobWorker


logger = logging.getLogger(__name__)


def _connection_fingerprint(engine: str, role: str, database: str | None, credentials: dict[str, Any]) -> str:
    """Build a non-secret identity so repeated server/database/role registrations collapse."""
    engine = engine.lower()
    if engine == "sqlite":
        endpoint = os.path.normcase(str(Path(str(credentials["database"])).expanduser().resolve()))
    elif engine == "mongodb":
        parsed = urlsplit(str(credentials.get("uri", "")))
        # Strip any URI username/password before comparing or storing identity metadata.
        hosts = parsed.netloc.rsplit("@", 1)[-1].lower()
        endpoint = {"scheme": parsed.scheme.lower(), "hosts": sorted(hosts.split(","))}
    else:
        default_port = 3306 if engine == "mysql" else 5432
        endpoint = {"host": str(credentials.get("host", "")).strip().casefold(),
                    "port": int(credentials.get("port") or default_port)}
    identity = {"engine": engine, "role": role.lower(),
                "database": str(database or "").strip().casefold(), "endpoint": endpoint}
    return hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class ApiProblem(Exception):
    def __init__(self, status: int, code: str, title: str, detail: str = "", *, errors: list[dict[str, Any]] | None = None,
                 headers: dict[str, str] | None = None):
        super().__init__(detail or title)
        self.status = status
        self.code = code
        self.title = title
        self.detail = detail
        self.errors = errors or []
        self.headers = headers or {}


def _safe(value: Any) -> Any:
    if isinstance(value, dict): return {str(key): _safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)): return [_safe(item) for item in value]
    if isinstance(value, (bytes, bytearray, memoryview)): return {"$binary": bytes(value).hex()}
    if isinstance(value, datetime): return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z") if value.tzinfo else value.isoformat()
    if type(value).__name__.lower() == "objectid": return str(value)
    if type(value).__name__.lower() == "decimal128": return format(value.to_decimal(), "f")
    try:
        import decimal
        if isinstance(value, decimal.Decimal): return format(value, "f")
    except Exception:
        pass
    if hasattr(value, "model_dump"): return _safe(value.model_dump(mode="json"))
    return value


class RequestIdMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable):
        request_id = request.headers.get("X-Request-Id") or opaque_id("req")
        request.state.request_id = request_id
        response = await call_next(request)
        response.headers["X-Request-Id"] = request_id
        return response


def _normal_etag(value: str | None) -> str | None:
    if value is None: return None
    return value.strip().removeprefix("W/").strip().strip('"')


def _problem(request: Request, exc: ApiProblem) -> JSONResponse:
    request_id = getattr(request.state, "request_id", opaque_id("req"))
    payload: dict[str, Any] = {
        "type": f"https://synapsemigrate.dev/errors/{exc.code.replace('_', '-')}",
        "title": exc.title, "status": exc.status, "code": exc.code,
        "detail": exc.detail or exc.title, "instance": request.url.path,
        "request_id": request_id,
    }
    if exc.errors: payload["errors"] = exc.errors
    return JSONResponse(payload, status_code=exc.status, headers=exc.headers, media_type="application/problem+json")


def _parse_api_keys() -> dict[str, str]:
    raw = os.getenv("SYNAPSE_API_KEYS", "")
    keys: dict[str, str] = {}
    for item in raw.split(","):
        if not item.strip(): continue
        token, sep, role = item.partition(":")
        if sep and token.strip() and role.strip(): keys[token.strip()] = role.strip().lower()
    return keys


def _get_user(request: Request) -> dict[str, str]:
    authorization = request.headers.get("Authorization", "")
    if not authorization.startswith("Bearer "):
        raise ApiProblem(401, "unauthorized", "Authentication required", "Send Authorization: Bearer <token>.", headers={"WWW-Authenticate": "Bearer"})
    token = authorization[7:].strip()
    keys = _parse_api_keys()
    role = next((candidate for secret, candidate in keys.items() if hmac.compare_digest(token, secret)), None)
    if role is None and os.getenv("SYNAPSE_DEV_MODE", "false").lower() in {"1", "true", "yes"} and hmac.compare_digest(token, "dev-token"):
        role = "admin"
    if role not in {"viewer", "editor", "operator", "admin"}:
        raise ApiProblem(401, "unauthorized", "Invalid bearer token", "The supplied bearer token is not configured.", headers={"WWW-Authenticate": "Bearer"})
    return {"sub": "api-key", "role": role}


def _require(user: dict[str, str], minimum: str) -> None:
    rank = {"viewer": 0, "editor": 1, "operator": 2, "admin": 3}
    if rank.get(user.get("role", ""), -1) < rank[minimum]:
        raise ApiProblem(403, "forbidden", "Insufficient role", f"This operation requires the {minimum} role.")


def _need_project(repository: ApiRepository, project_id: str) -> dict[str, Any]:
    project = repository.get_project(project_id)
    if project is None: raise ApiProblem(404, "project_not_found", "Project not found", project_id)
    return project


def _need_plan(repository: ApiRepository, plan_id: str) -> dict[str, Any]:
    plan = repository.get_plan(plan_id)
    if plan is None: raise ApiProblem(404, "plan_not_found", "Plan not found", plan_id)
    return plan


def _require_if_match(project: dict[str, Any], value: str | None) -> str:
    provided = _normal_etag(value)
    if provided is None:
        raise ApiProblem(428, "precondition_required", "If-Match is required", "Mutating IR requests require the current If-Match ETag.")
    if not hmac.compare_digest(provided, project["etag"]):
        raise ApiProblem(412, "precondition_failed", "Stale ETag", "The IR changed since it was read.",
                         errors=[{"path": "If-Match", "code": "stale_etag", "message": "Use the current ETag and retry."}],
                         headers={"ETag": f'"{project["etag"]}"', "X-Current-Revision": str(project["revision"])})
    return provided


def _advance_project(repository: ApiRepository, project: dict[str, Any], *, ir: dict[str, Any] | None = None,
                     advisories: list[dict[str, Any]] | None = None, thresholds: dict[str, Any] | None = None,
                     author: str, change_summary: str) -> dict[str, Any]:
    previous = {"revision": project["revision"], "etag": project["etag"], "author": project.get("author", "system"),
                "created_at": project.get("updated_at", project.get("created_at")), "change_summary": change_summary,
                "ir": project.get("ir", {}), "advisories": project.get("advisories", []), "thresholds": project.get("thresholds", {})}
    updated = dict(project)
    if ir is not None: updated["ir"] = ir
    if advisories is not None: updated["advisories"] = advisories
    if thresholds is not None: updated["thresholds"] = thresholds
    updated["history"] = list(project.get("history", [])) + [previous]
    updated["revision"] = int(project["revision"]) + 1
    updated["author"] = author
    updated["updated_at"] = utc_now()
    updated["last_change"] = change_summary
    if not repository.save_project(updated, expected_revision=int(project["revision"]), expected_etag=project["etag"]):
        current = repository.get_project(project["project_id"]) or {}
        raise ApiProblem(412, "precondition_failed", "Concurrent IR update", "Another request committed a newer revision.",
                         errors=[{"path": "If-Match", "code": "stale_etag", "message": "Reload the IR and retry."}],
                         headers={"ETag": f'"{current.get("etag", "")}"', "X-Current-Revision": str(current.get("revision", ""))})
    return updated


def _idempotent_replay(repository: ApiRepository, request: Request, user: dict[str, str], body: Any) -> JSONResponse | None:
    key = request.headers.get("Idempotency-Key")
    if not key: return None
    if len(key) > 256:
        raise ApiProblem(400, "invalid_request", "Invalid Idempotency-Key", "The key must be at most 256 characters.")
    body_hash = etag_for(body)
    saved = repository.idempotency_get(user["sub"], key)
    if not saved: return None
    if not hmac.compare_digest(str(saved.get("body_hash", "")), body_hash):
        raise ApiProblem(422, "idempotency_key_reuse", "Idempotency key reuse", "The key was already used with a different request body.")
    response = saved.get("response", {})
    headers = dict(response.get("headers", {}))
    headers["Idempotent-Replayed"] = "true"
    return JSONResponse(_safe(response.get("body", {})), status_code=int(response.get("status_code", 200)), headers=headers)


def _save_idempotency(repository: ApiRepository, request: Request, user: dict[str, str], body: Any,
                      response_body: Any, status_code: int = 200, headers: dict[str, str] | None = None) -> None:
    key = request.headers.get("Idempotency-Key")
    if key:
        repository.idempotency_set(user["sub"], key, etag_for(body), {"body": _safe(response_body), "status_code": status_code, "headers": headers or {}})


def _vault(request: Request):
    try:
        return build_vault(request.app.state.repository.store)
    except RuntimeError as exc:
        raise ApiProblem(503, "credential_escrow_unavailable", "Credential escrow unavailable", str(exc)) from exc


def _connection(repository: ApiRepository, connection_id: str, role: str | None = None, engine: str | None = None) -> dict[str, Any]:
    metadata = repository.get_connection(connection_id)
    if metadata is None:
        raise ApiProblem(404, "connection_not_found", "Connection not found", connection_id)
    if float(metadata.get("expires_at_epoch", 0)) <= time.time():
        raise ApiProblem(410, "connection_expired", "Connection expired", "Renew the connection credentials and retry.")
    if role and metadata.get("role") != role:
        raise ApiProblem(422, "invalid_connection_role", "Connection role mismatch", f"Expected a {role} connection.")
    if engine and metadata.get("engine") != engine:
        raise ApiProblem(422, "target_engine_mismatch", "Connection engine mismatch", f"Expected {engine}, got {metadata.get('engine')}.")
    return metadata


def _api_summary(project: dict[str, Any]) -> dict[str, Any]:
    ir = project.get("ir", {})
    counts: dict[str, Any]
    if project.get("ir_kind") == "relational":
        counts = {"tables": len(ir.get("tables", [])), "relationships": len(ir.get("relationships", []))}
    else:
        counts = {"collections": len(ir.get("collections", [])), "decisions": len(ir.get("decisions", [])),
                  "sampled_documents": sum(item.get("sample", {}).get("taken", 0) + item.get("sample", {}).get("recency_probe", 0) for item in ir.get("collections", []))}
    counts["advisories"] = {severity: sum(item.get("severity") == severity for item in project.get("advisories", [])) for severity in ("INFO", "MEDIUM", "HIGH")}
    counts["duration_ms"] = project.get("duration_ms", 0)
    return counts


def _split_filter_params(request: Request) -> dict[str, Any]:
    filters: dict[str, Any] = {}
    for key, value in request.query_params.multi_items():
        match = re.fullmatch(r"filter\[([^]]+)\](?:\[([^]]+)\])?", key)
        if not match: continue
        field, operator = match.groups()
        if operator == "in": filters[field] = {"$in": [_parse_scalar(item) for item in value.split(",") if item != ""]}
        elif operator in {"gte", "gt", "lte", "lt", "ne"}:
            mongo_op = {"gte": "$gte", "gt": "$gt", "lte": "$lte", "lt": "$lt", "ne": "$ne"}[operator]
            filters[field] = {mongo_op: _parse_scalar(value)}
        else: filters[field] = _parse_scalar(value)
    return filters


def _parse_scalar(value: str) -> Any:
    if value.lower() in {"true", "false"}: return value.lower() == "true"
    if value.lower() == "null": return None
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return value


def _database_problem(exc: BaseException) -> ApiProblem:
    original = getattr(exc, "orig", exc)
    code = None
    args = getattr(original, "args", ())
    if args and isinstance(args[0], int): code = int(args[0])
    text = str(original).lower()
    if code in {1062, 11000} or "duplicate key" in text:
        return ApiProblem(409, "duplicate_key", "Duplicate key", "A unique value already exists.", errors=[{"path": "$", "code": "duplicate_key", "message": "unique constraint violation", "source": "database", "database_code": code}])
    if code in {1452, 1216}:
        return ApiProblem(422, "reference_not_found", "Reference not found", "A foreign-key target does not exist.", errors=[{"path": "$", "code": "reference_not_found", "message": "foreign-key constraint failed", "source": "database", "database_code": code}])
    if code in {1406, 1264, 1265, 22001}:
        return ApiProblem(422, "value_out_of_range", "Value out of range", "A value does not fit the generated SQL type.", errors=[{"path": "$", "code": "value_out_of_range", "message": "value exceeds the database type range", "source": "database", "database_code": code}])
    if code in {3819, 23514}:
        return ApiProblem(422, "check_violation", "Check constraint violation", "The value failed a database CHECK constraint.", errors=[{"path": "$", "code": "check_violation", "message": "check constraint failed", "source": "database", "database_code": code}])
    if code in {1048, 23502}:
        return ApiProblem(422, "not_null_violation", "NOT NULL violation", "A required database value was null.", errors=[{"path": "$", "code": "not_null_violation", "message": "NOT NULL constraint failed", "source": "database", "database_code": code}])
    return ApiProblem(422, "database_constraint", "Database rejected the write", "The target database enforced a constraint.", errors=[{"path": "$", "code": "database_constraint", "message": str(original)[:300], "source": "database", "database_code": code}])


def _mongo_repo_context(request: Request, collection_name: str):
    repository: ApiRepository = request.app.state.repository
    wrapper = next((plan for plan in repository.list_plans() if plan.get("target_engine") == "mongodb" and plan.get("applied")
                    and plan.get("plan", {}).get("options", {}).get("validators", {}).get(collection_name) is not None), None)
    if wrapper is None: raise ApiProblem(404, "resource_not_applied", "CRUD resource unavailable", "The collection must exist in an applied plan.")
    connection_id = wrapper.get("applied_connection_id")
    metadata = _connection(repository, connection_id, role="target", engine="mongodb")
    credentials = _vault(request).get(connection_id)
    client = create_mongo_client(credentials)
    database = client[metadata["database"]]
    plan = ApprovedPlan.model_validate(wrapper["plan"])
    validator = plan.options.get("validators", {}).get(collection_name, {})
    indexes = plan.options.get("indexes", {}).get(collection_name, [])
    indexed = {"_id"}
    for index in indexes: indexed.update(index.get("keys", {}).keys())
    outgoing = []
    incoming = []
    for rel in plan.source_ir.get("relationships", []):
        if rel.get("effective_mapping") or rel.get("user_override"):
            mapping = rel.get("effective_mapping") or rel.get("user_override")
        else:
            mapping = rel.get("proposed_mapping")
        if mapping in {"EMBED", "EMBED_ARRAY", "REFERENCE_ARRAY"}: continue
        parent = rel.get("parent_table", "").split(".")[-1]
        child = rel.get("child_table", "").split(".")[-1]
        field = rel.get("foreign_key", [f"{parent}_ref"])[0] if len(rel.get("foreign_key", [])) == 1 else f"{parent}_ref"
        if child == collection_name:
            outgoing.append((database[parent], field, parent))
        if parent == collection_name:
            incoming.append((database[child], field, child))
    derived = plan.options.get("generation_options", {}).get("derived_fields", {})
    repo = VerifiedMongoRepository(database[collection_name], validator=validator, indexed_fields=indexed,
                                   reference_checks=outgoing, delete_references=incoming, derived_fields=derived)
    return client, database, repo, wrapper, metadata


def _sql_repo_context(request: Request, table_name: str):
    repository: ApiRepository = request.app.state.repository
    wrapper = next((plan for plan in repository.list_plans() if plan.get("target_engine") in {"mysql", "postgresql"} and plan.get("applied")
                    and any(table.get("name") == table_name and table.get("role") == "root" for table in plan.get("plan", {}).get("tables", []))), None)
    if wrapper is None: raise ApiProblem(404, "resource_not_applied", "CRUD resource unavailable", "The SQL root table must exist in an applied reverse plan.")
    connection_id = wrapper.get("applied_connection_id")
    metadata = _connection(repository, connection_id, role="target", engine=wrapper["target_engine"])
    credentials = _vault(request).get(connection_id)
    engine = create_sql_engine(wrapper["target_engine"], credentials)
    plan = ApprovedPlan.model_validate(wrapper["plan"])
    return engine, VerifiedSqlAggregateRepository(engine, plan, table_name), wrapper, metadata


def _delete_plan_artifacts(repository: ApiRepository, plan: dict[str, Any]) -> None:
    for artifact in plan.get("artifacts", []):
        repository.store.delete(f"artifact:{artifact['artifact_id']}")
        path = repository.artifact_dir / f"{artifact['artifact_id']}.bin"
        path.unlink(missing_ok=True)
    repository.store.delete(f"plan:{plan['plan_id']}")


def create_app(*, repository: ApiRepository | None = None, state_store: Any | None = None) -> FastAPI:
    app = FastAPI(title="SynapseMigrate API", version="2.0.0", docs_url="/docs", redoc_url=None)
    if repository is None:
        store = state_store or build_state_store()
        repository = build_repository(store)
    app.state.repository = repository
    app.state.executor = __import__("concurrent.futures").futures.ThreadPoolExecutor(max_workers=int(os.getenv("SYNAPSE_API_THREADS", "4")), thread_name_prefix="synapse-api-job")
    dashboard_dir = Path(__file__).resolve().parents[2] / "dashboard"
    app.mount("/dashboard", StaticFiles(directory=dashboard_dir), name="dashboard-assets")
    app.add_middleware(RequestIdMiddleware)

    @app.get("/", include_in_schema=False)
    def dashboard_home():
        """Serve the static, modular dashboard shell from the API origin."""
        return FileResponse(dashboard_dir / "index.html")

    @app.exception_handler(ApiProblem)
    async def handle_api_problem(request: Request, exc: ApiProblem):
        return _problem(request, exc)

    @app.exception_handler(RequestValidationError)
    async def handle_validation(request: Request, exc: RequestValidationError):
        errors = [{"path": ".".join(str(part) for part in item.get("loc", [])), "code": item.get("type", "invalid_request"),
                   "message": item.get("msg", "invalid value"), "source": "api"} for item in exc.errors()]
        return _problem(request, ApiProblem(400, "invalid_request", "Invalid request", "Request parameters failed validation.", errors=errors))

    @app.exception_handler(Exception)
    async def handle_unexpected(request: Request, exc: Exception):
        if isinstance(exc, ApiProblem): return _problem(request, exc)
        return _problem(request, ApiProblem(500, "internal_error", "Internal server error", "The request failed unexpectedly."))

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    @app.get("/readyz")
    def readyz(request: Request):
        store = request.app.state.repository.store
        checks: dict[str, bool] = {}
        if isinstance(store, ResilientStateStore):
            try: checks["redis"] = bool(store.primary.health())
            except Exception: checks["redis"] = False
            try: checks["state_store"] = bool(store.fallback.health())
            except Exception: checks["state_store"] = False
        else:
            try: checks["state_store"] = bool(store.health())
            except Exception: checks["state_store"] = False
        job_dir = request.app.state.repository.job_dir
        try:
            probe = job_dir / f".ready-{secrets.token_hex(4)}"
            probe.write_text("ok", encoding="utf-8")
            probe.unlink(missing_ok=True)
            checks["job_volume"] = True
        except OSError:
            checks["job_volume"] = False
        ready = all(checks.values())
        return JSONResponse({"status": "ready" if ready else "not_ready", "checks": checks}, status_code=200 if ready else 503)

    @app.get("/metrics")
    def metrics():
        from prometheus_client import CollectorRegistry, Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST
        registry = CollectorRegistry()
        Counter("synapse_job_rows_total", "Rows processed by migration workers", registry=registry)
        Counter("synapse_job_errors_total", "Dead-lettered migration errors", ["code"], registry=registry)
        Gauge("synapse_batch_seconds", "Last observed migration batch duration", registry=registry)
        Gauge("synapse_checkpoint_lag_seconds", "Age of the most recent migration checkpoint", registry=registry)
        Gauge("synapse_idmap_cache_hit_ratio", "ID map bounded-cache hit ratio", registry=registry)
        Counter("synapse_introspect_seconds_total", "Schema introspection durations", registry=registry)
        Counter("synapse_lease_lost_total", "Lost worker leases", registry=registry)
        return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)

    @app.get("/api/version")
    def version(user: dict[str, str] = Depends(_get_user)):
        _require(user, "viewer")
        return {"api": "2.0", "ir_version": 2,
                "engines": {"source": ["sqlite", "mysql", "postgresql", "mongodb"], "target": ["mongodb", "mysql", "postgresql"]}}

    # Connections and encrypted credential escrow.
    @app.get("/api/demo/sqlite")
    def sqlite_demo_info(user: dict[str, str] = Depends(_get_user)):
        """Return the bundled SQLite source path within this server's filesystem."""
        _require(user, "operator")
        configured = os.getenv("SYNAPSE_DEMO_SQLITE_PATH")
        demo_path = Path(configured).expanduser().resolve() if configured else dashboard_dir.parent / "demo" / "synapsemigrate_demo.db"
        if not demo_path.is_file():
            raise ApiProblem(404, "demo_database_missing", "Demo database unavailable", "The bundled SQLite demo file is missing from the server.")
        return {"database": str(demo_path)}

    @app.post("/api/demo/visualize")
    def visualize_demo_migration(request: Request, body: dict[str, Any] = Body(...), user: dict[str, str] = Depends(_get_user)):
        """Read bundled SQLite rows and return an explicitly non-writing migration walkthrough."""
        _require(user, "operator")
        plan_id = str(body.get("plan_id", ""))
        wrapper = _need_plan(repository, plan_id)
        plan = ApprovedPlan.model_validate(wrapper["plan"])
        if not plan.verify_hash(wrapper.get("plan_hash")):
            raise ApiProblem(409, "plan_changed", "Plan changed", "The saved plan hash does not match its immutable contents.")
        if plan.direction.value != "RELATIONAL_TO_DOCUMENT" or plan.target_engine.value != "mongodb":
            raise ApiProblem(422, "demo_direction_unsupported", "Demo direction unsupported", "The guided sample walkthrough currently visualizes SQLite-to-MongoDB plans.")
        connection_id = str(wrapper.get("source_connection_id") or "")
        metadata = repository.get_connection(connection_id)
        if not metadata or metadata.get("engine") != "sqlite":
            raise ApiProblem(422, "demo_source_required", "SQLite demo source required", "Generate this plan from the bundled or another registered SQLite source.")
        try:
            credentials = build_vault(repository.store).get(connection_id)
            engine = create_sql_engine("sqlite", credentials)
            preview_limit = min(8, max(1, int(body.get("preview_rows_per_table", 4))))
            tables: list[dict[str, Any]] = []
            try:
                with engine.connect() as connection:
                    preparer = connection.dialect.identifier_preparer
                    for table in plan.source_ir.get("tables", []):
                        name = str(table["table_name"])
                        schema = table.get("schema_name")
                        qualified = preparer.quote(name)
                        if schema:
                            qualified = preparer.quote_schema(str(schema)) + "." + qualified
                        row_count = int(connection.exec_driver_sql(f"SELECT COUNT(*) FROM {qualified}").scalar_one())
                        rows = connection.exec_driver_sql(f"SELECT * FROM {qualified} LIMIT {preview_limit}").mappings().all()
                        pk = list((table.get("primary_key") or {}).get("columns") or [])
                        documents = []
                        for row_index, row in enumerate(rows, 1):
                            row_data = _safe(dict(row))
                            source_key = [row.get(column) for column in pk]
                            stable_key = ":".join(str(value) for value in source_key) if source_key else str(row_index)
                            documents.append({"_id": f"demo:{name}:{stable_key}", "_source_table": name, **row_data})
                        tables.append({"table": name, "source_rows": row_count, "preview_rows": len(rows),
                                       "sample_documents": documents, "primary_key": pk})
            finally:
                engine.dispose()
        except Exception as exc:
            raise ApiProblem(502, "demo_read_failed", "Demo source read failed", "The SQLite source could not be read for this walkthrough.") from exc
        total_rows = sum(item["source_rows"] for item in tables)
        total_preview = sum(item["preview_rows"] for item in tables)
        return {"plan_id": plan_id, "plan_hash": plan.plan_hash, "source_engine": "sqlite", "target_engine": "mongodb",
                "mode": "visualization_only", "writes_performed": False,
                "notice": "Illustrative walkthrough based on real SQLite sample rows. No target database was contacted and no data was written.",
                "stages": [
                    {"id": "source", "label": "Read sample source", "status": "complete", "detail": f"Read {len(tables)} SQLite tables; {total_rows} rows counted."},
                    {"id": "plan", "label": "Validate approved plan", "status": "complete", "detail": f"Immutable plan {plan.plan_id} · {plan.plan_hash[:19]}…"},
                    {"id": "transform", "label": "Preview document mapping", "status": "complete", "detail": f"Prepared {total_preview} illustrative documents from actual sample records."},
                    {"id": "target", "label": "MongoDB write step", "status": "simulated", "detail": "Skipped: this guided demo does not connect to or write to MongoDB."},
                    {"id": "verify", "label": "Verification step", "status": "simulated", "detail": "Skipped: no target writes were performed."},
                ],
                "summary": {"tables": len(tables), "source_rows": total_rows, "preview_documents": total_preview,
                            "batches_visualized": sum((item["preview_rows"] + 1) // 2 for item in tables), "target_writes": 0},
                "tables": tables}

    @app.post("/api/connections", status_code=201)
    def create_connection(request: Request, body: dict[str, Any] = Body(...), user: dict[str, str] = Depends(_get_user)):
        _require(user, "operator")
        replay = _idempotent_replay(repository, request, user, body)
        if replay: return replay
        engine = str(body.get("engine", "")).lower()
        role = str(body.get("role", "source")).lower()
        if engine not in {"sqlite", "mysql", "postgresql", "mongodb"} or role not in {"source", "target"}:
            raise ApiProblem(400, "invalid_request", "Invalid connection", "engine must be sqlite, mysql, postgresql, or mongodb and role must be source or target.")
        if engine == "sqlite" and role != "source":
            raise ApiProblem(400, "invalid_request", "Invalid SQLite role", "SQLite is supported only as a local/demo source, not as a migration target.")
        ttl = int(body.get("ttl_seconds", 28_800))
        if ttl < 60 or ttl > 7 * 24 * 3600:
            raise ApiProblem(400, "invalid_request", "Invalid TTL", "ttl_seconds must be between 60 and 604800.")
        credentials: dict[str, Any]
        if engine == "mongodb":
            if not body.get("uri"):
                raise ApiProblem(400, "invalid_request", "MongoDB URI required", "Supply the MongoDB URI in the request body.")
            if not str(body.get("database") or "").strip():
                raise ApiProblem(400, "invalid_request", "MongoDB database name required", "Enter a database name separately from the MongoDB URI.")
            database = str(body["database"]).strip()
            credentials = {"uri": body["uri"], "database": database, "socket_timeout_ms": body.get("socket_timeout_ms", 30_000)}
            host = None
            port = None
        elif engine == "sqlite":
            raw_path = body.get("database") or body.get("path")
            if not raw_path:
                raise ApiProblem(400, "invalid_request", "SQLite database path required", "Supply the local .db file path in `database`.")
            db_path = Path(str(raw_path)).expanduser().resolve()
            if not db_path.is_file():
                raise ApiProblem(404, "sqlite_database_not_found", "SQLite demo database not found", str(db_path))
            credentials = {"database": str(db_path)}
            database = str(db_path)
            host = None
            port = None
        else:
            required = [key for key in ("host", "database", "user", "password") if not body.get(key)]
            if required:
                raise ApiProblem(400, "invalid_request", "SQL connection fields missing", f"Required fields: {', '.join(required)}.")
            credentials = {key: body.get(key) for key in ("host", "port", "database", "user", "password", "tls", "charset", "connect_timeout") if body.get(key) is not None}
            database = body.get("database")
            host = body.get("host")
            port = body.get("port") or (3306 if engine == "mysql" else 5432)
        vault = _vault(request)
        fingerprint = _connection_fingerprint(engine, role, database, credentials)
        identity_key = f"connection-identity:{fingerprint}"
        # Detect records created before fingerprint reservations were introduced too.
        for existing in repository.list_connections():
            existing_fingerprint = existing.get("connection_fingerprint")
            if not existing_fingerprint:
                try:
                    old_credentials = vault.get(existing["connection_id"])
                    existing_fingerprint = _connection_fingerprint(existing["engine"], existing["role"],
                                                                   existing.get("database"), old_credentials)
                except Exception:
                    existing_fingerprint = None
            if existing_fingerprint == fingerprint:
                raise ApiProblem(409, "connection_exists", "Connection already exists",
                                 "This server, database, engine, and role are already registered. Reuse the existing connection or remove it first.")
        try:
            verification = verify_connection(engine, credentials)
        except Exception as exc:
            # Driver exceptions can contain DSNs/passwords; the public detail is deliberately generic.
            raise ApiProblem(502, "source_unreachable", "Connection test failed", "The database could not be reached with the supplied credentials.") from exc
        connection_id = opaque_id("conn")
        reserved = repository.store.compare_and_set(identity_key, None, None, {"connection_id": connection_id})
        if not reserved:
            raise ApiProblem(409, "connection_exists", "Connection already exists",
                             "This server, database, engine, and role were registered by another request. Reuse the existing connection.")
        try:
            expiry = vault.put(connection_id, credentials, ttl)
            expires_at = datetime.fromtimestamp(expiry, timezone.utc).isoformat().replace("+00:00", "Z")
            metadata = {"connection_id": connection_id, "name": str(body.get("name", f"{engine}-{database or host or 'connection'}")),
                        "engine": engine, "role": role, "host": host, "port": port, "database": database,
                        "expires_at": expires_at, "expires_at_epoch": expiry, "verified": True,
                        "connection_fingerprint": fingerprint, "connection_status": "successful",
                        "last_checked_at": utc_now(), "status_detail": "Connection test succeeded when saved.",
                        "server_version": verification.get("server_version"), "created_at": utc_now()}
            repository.save_connection(metadata)
        except Exception:
            vault.delete(connection_id)
            repository.delete_connection(connection_id)
            identity = repository.store.get(identity_key) or {}
            if identity.get("connection_id") == connection_id:
                repository.store.delete(identity_key)
            raise
        response_body = {key: value for key, value in metadata.items() if key not in {"expires_at_epoch", "connection_fingerprint"}}
        _save_idempotency(repository, request, user, body, response_body, 201)
        return JSONResponse(response_body, status_code=201)

    @app.get("/api/connections")
    def list_connections(user: dict[str, str] = Depends(_get_user)):
        _require(user, "operator")
        private = {"expires_at_epoch", "connection_fingerprint"}
        return {"items": [{key: value for key, value in item.items() if key not in private} for item in repository.list_connections()]}

    @app.get("/api/database-browser")
    def list_browsable_databases(request: Request, user: dict[str, str] = Depends(_get_user)):
        """List saved databases once per physical endpoint/database, independent of source/target role."""
        _require(user, "operator")
        groups: dict[str, dict[str, Any]] = {}
        unavailable: list[dict[str, str]] = []
        vault = _vault(request)
        for metadata in repository.list_connections():
            try:
                credentials = vault.get(metadata["connection_id"])
            except CredentialExpiredError:
                unavailable.append({"name": metadata.get("name", "Saved connection"), "detail": "Credentials expired; renew this connection to browse it."})
                continue
            database = metadata.get("database")
            if not database:
                continue
            identity = _connection_fingerprint(metadata["engine"], "browser", database, credentials)
            endpoint = "Local file" if metadata["engine"] == "sqlite" else (
                str(metadata.get("host") or "") + (f":{metadata['port']}" if metadata.get("port") else "")
                if metadata["engine"] != "mongodb" else urlsplit(str(credentials.get("uri", ""))).netloc.rsplit("@", 1)[-1]
            )
            group = groups.get(identity)
            if group is None:
                groups[identity] = {"browser_id": identity, "connection_id": metadata["connection_id"],
                                    "name": metadata.get("name", str(database)), "engine": metadata["engine"],
                                    "database": database, "endpoint": endpoint, "roles": [metadata["role"]],
                                    "connection_ids": [metadata["connection_id"]]}
            else:
                if metadata["role"] not in group["roles"]:
                    group["roles"].append(metadata["role"])
                group["connection_ids"].append(metadata["connection_id"])
                # Prefer the source handle for browsing when a database is registered in both roles.
                if metadata["role"] == "source":
                    group["connection_id"] = metadata["connection_id"]
                    group["name"] = metadata.get("name", group["name"])
        items = sorted(groups.values(), key=lambda item: (item["engine"], str(item["database"]).casefold(), item["endpoint"].casefold()))
        return {"items": items, "unavailable": unavailable}

    @app.get("/api/database-browser/{connection_id}/objects")
    def list_database_objects(connection_id: str, request: Request, user: dict[str, str] = Depends(_get_user)):
        _require(user, "operator")
        metadata = _connection(repository, connection_id)
        credentials = _vault(request).get(connection_id)
        if metadata["engine"] == "mongodb":
            client = create_mongo_client(credentials)
            try:
                database = client[metadata["database"]]
                names = sorted(database.list_collection_names())
                return {"items": [{"name": name, "kind": "collection"} for name in names]}
            finally:
                client.close()
        engine = create_sql_engine(metadata["engine"], credentials)
        try:
            from sqlalchemy import inspect as sqlalchemy_inspect
            inspector = sqlalchemy_inspect(engine)
            tables = [{"name": name, "kind": "table", "schema": None} for name in inspector.get_table_names()]
            views = [{"name": name, "kind": "view", "schema": None} for name in inspector.get_view_names()]
            return {"items": sorted(tables + views, key=lambda item: (item["name"].casefold(), item["kind"]))}
        finally:
            engine.dispose()

    @app.get("/api/database-browser/{connection_id}/sample")
    def sample_database_object(connection_id: str, request: Request, object_name: str = Query(..., min_length=1),
                               limit: int = Query(25, ge=1, le=100), offset: int = Query(0, ge=0),
                               user: dict[str, str] = Depends(_get_user)):
        _require(user, "operator")
        metadata = _connection(repository, connection_id)
        credentials = _vault(request).get(connection_id)
        if metadata["engine"] == "mongodb":
            client = create_mongo_client(credentials)
            try:
                database = client[metadata["database"]]
                if object_name not in database.list_collection_names():
                    raise ApiProblem(404, "object_not_found", "Collection not found", object_name)
                cursor = database[object_name].find({}).sort("_id", 1).skip(offset).limit(limit + 1)
                documents = list(cursor)
                more = len(documents) > limit
                return {"object": object_name, "items": _safe(documents[:limit]),
                        "next_offset": offset + limit if more else None, "has_more": more}
            finally:
                client.close()
        engine = create_sql_engine(metadata["engine"], credentials)
        try:
            from sqlalchemy import inspect as sqlalchemy_inspect, text
            inspector = sqlalchemy_inspect(engine)
            available = set(inspector.get_table_names()) | set(inspector.get_view_names())
            if object_name not in available:
                raise ApiProblem(404, "object_not_found", "Table or view not found", object_name)
            preparer = engine.dialect.identifier_preparer
            qualified = preparer.quote(object_name)
            query = text(f"SELECT * FROM {qualified} LIMIT :limit OFFSET :offset")
            with engine.connect() as connection:
                result = connection.execute(query, {"limit": limit + 1, "offset": offset})
                rows = [dict(row) for row in result.mappings()]
            more = len(rows) > limit
            return {"object": object_name, "items": _safe(rows[:limit]),
                    "next_offset": offset + limit if more else None, "has_more": more}
        finally:
            engine.dispose()

    @app.post("/api/connections/{connection_id}/test")
    def test_saved_connection(connection_id: str, request: Request, user: dict[str, str] = Depends(_get_user)):
        """Recheck a saved endpoint and persist its last known live status."""
        _require(user, "operator")
        metadata = repository.get_connection(connection_id)
        if metadata is None:
            raise ApiProblem(404, "connection_not_found", "Connection not found", connection_id)
        checked_at = utc_now()
        if metadata.get("engine") == "mongodb" and not str(metadata.get("database") or "").strip():
            status = "failed"
            detail = "MongoDB database name is missing. Remove and recreate this connection with a database name."
        else:
            try:
                credentials = _vault(request).get(connection_id)
                verification = verify_connection(metadata["engine"], credentials)
                status = "successful"
                detail = "Connection test succeeded."
                metadata["server_version"] = verification.get("server_version")
            except CredentialExpiredError:
                status = "failed"
                detail = "Saved credentials expired. Renew or recreate this connection."
            except Exception:
                # Do not return driver exception text because it can reveal hosts or credentials.
                status = "failed"
                detail = "The database could not be reached with the saved settings. Check that it is running and the endpoint is correct."
        metadata.update(connection_status=status, last_checked_at=checked_at, status_detail=detail)
        repository.save_connection(metadata)
        return {"connection_id": connection_id, "connection_status": status,
                "last_checked_at": checked_at, "status_detail": detail,
                "server_version": metadata.get("server_version")}

    @app.get("/api/connections/{connection_id}")
    def get_connection(connection_id: str, user: dict[str, str] = Depends(_get_user)):
        _require(user, "operator")
        item = _connection(repository, connection_id)
        return {key: value for key, value in item.items() if key not in {"expires_at_epoch", "connection_fingerprint"}}

    @app.post("/api/connections/{connection_id}/renew")
    def renew_connection(connection_id: str, request: Request, body: dict[str, Any] = Body(...), user: dict[str, str] = Depends(_get_user)):
        _require(user, "operator")
        metadata = _connection(repository, connection_id)
        old = _vault(request).get(connection_id)
        ttl = int(body.get("ttl_seconds", 28_800))
        if not 60 <= ttl <= 7 * 24 * 3600:
            raise ApiProblem(400, "invalid_request", "Invalid TTL", "ttl_seconds must be between 60 and 604800.")
        credentials = dict(old)
        if metadata["engine"] == "mongodb":
            if body.get("uri"):
                credentials["uri"] = body["uri"]
            elif body.get("password"):
                parts = urlsplit(credentials["uri"])
                netloc = parts.netloc
                if "@" in netloc:
                    auth, host_part = netloc.rsplit("@", 1)
                    username = auth.split(":", 1)[0]
                    netloc = f"{username}:{quote(str(body['password']), safe='')}@{host_part}"
                    credentials["uri"] = urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))
        elif body.get("password"):
            credentials["password"] = body["password"]
        try:
            verification = verify_connection(metadata["engine"], credentials)
        except Exception as exc:
            raise ApiProblem(502, "source_unreachable", "Connection test failed", "The renewed credentials could not be verified.") from exc
        expiry = _vault(request).renew(connection_id, credentials, ttl)
        metadata["expires_at_epoch"] = expiry
        metadata["expires_at"] = datetime.fromtimestamp(expiry, timezone.utc).isoformat().replace("+00:00", "Z")
        metadata["server_version"] = verification.get("server_version")
        repository.save_connection(metadata)
        return {key: value for key, value in metadata.items() if key != "expires_at_epoch"}

    @app.delete("/api/connections/{connection_id}", status_code=204)
    def delete_connection(connection_id: str, request: Request, user: dict[str, str] = Depends(_get_user)):
        _require(user, "operator")
        metadata = repository.get_connection(connection_id)
        if metadata is None:
            raise ApiProblem(404, "connection_not_found", "Connection not found", connection_id)
        _vault(request).delete(connection_id)
        fingerprint = metadata.get("connection_fingerprint")
        if fingerprint:
            identity_key = f"connection-identity:{fingerprint}"
            identity = repository.store.get(identity_key) or {}
            if identity.get("connection_id") == connection_id:
                repository.store.delete(identity_key)
        repository.delete_connection(connection_id)
        return Response(status_code=204)

    # Schema introspection and projects.
    def introspection_job(project_id: str, engine: str, connection_id: str, options: dict[str, Any], author: str, job_id: str | None = None):
        if job_id:
            job = repository.get_job(job_id) or {}
            job.update(status="running", started_at=utc_now(), updated_at=utc_now())
            repository.save_job(job)
        try:
            project = run_introspection(repository, build_vault(repository.store), project_id=project_id,
                                        source_engine=engine, connection_id=connection_id, options=options, author=author)
            if job_id:
                job = repository.get_job(job_id) or {}
                job.update(status="completed", project_id=project_id, result={"summary": _api_summary(project)}, updated_at=utc_now(), completed_at=utc_now())
                repository.save_job(job)
        except Exception as exc:
            logger.exception("Schema introspection job failed (engine=%s, project_id=%s)", engine, project_id)
            if job_id:
                job = repository.get_job(job_id) or {}
                job.update(status="failed", error_code="introspection_failed", error=str(exc) if isinstance(exc, (ValueError, LookupError)) else "Source introspection failed.", updated_at=utc_now())
                repository.save_job(job)
            else:
                raise

    @app.post("/api/schema/introspect")
    def introspect(request: Request, body: dict[str, Any] = Body(...), user: dict[str, str] = Depends(_get_user)):
        _require(user, "editor")
        replay = _idempotent_replay(repository, request, user, body)
        if replay: return replay
        deprecated = "source_type" not in body
        source_type = str(body.get("source_type", "mysql")).lower()
        if source_type not in {"sqlite", "mysql", "postgresql", "mongodb"}:
            raise ApiProblem(422, "unsupported_source_type", "Unsupported source type", source_type)
        connection_id = body.get("connection_id")
        if not connection_id:
            raise ApiProblem(400, "invalid_request", "Connection handle required", "Store source credentials with POST /api/connections first.")
        _connection(repository, str(connection_id), role="source", engine=source_type)
        options = dict(body.get("options") or {})
        project_id = opaque_id("prj")
        wait = bool(body.get("wait", True))
        if not wait:
            job_id = opaque_id("job")
            job = {"job_id": job_id, "job_type": "introspect", "kind": "introspect", "status": "queued", "project_id": project_id,
                   "source_connection_id": connection_id, "source_engine": source_type, "options": options, "created_at": utc_now(), "updated_at": utc_now()}
            repository.save_job(job)
            request.app.state.executor.submit(introspection_job, project_id, source_type, str(connection_id), options, user["sub"], job_id)
            result = {"job_id": job_id, "kind": "introspect", "status": "queued", "project_id": project_id, "poll": f"/api/jobs/{job_id}"}
            _save_idempotency(repository, request, user, body, result, 202, {"Location": f"/api/jobs/{job_id}"})
            headers = {"Location": f"/api/jobs/{job_id}"}
            if deprecated: headers["Deprecation"] = "true"
            return JSONResponse(result, status_code=202, headers=headers)
        try:
            project = run_introspection(repository, _vault(request), project_id=project_id, source_engine=source_type,
                                        connection_id=str(connection_id), options=options, author=user["sub"])
        except CredentialExpiredError as exc:
            raise ApiProblem(410, "connection_expired", "Connection expired", "Renew the source credentials and retry.") from exc
        except ValueError as exc:
            raise ApiProblem(400, "invalid_request", "Introspection failed", str(exc)) from exc
        except LookupError as exc:
            raise ApiProblem(404, "connection_not_found", "Connection not found", str(exc)) from exc
        except Exception as exc:
            logger.exception("Synchronous schema introspection failed (engine=%s)", source_type)
            raise ApiProblem(502, "source_unreachable", "Source introspection failed", "The source could not be inspected with the supplied options.") from exc
        body_out = {"project_id": project_id, "revision": project["revision"], "etag": project["etag"], "ir_kind": project["ir_kind"],
                    "source_engine": project["source_engine"], "direction": project["direction"], "ir_version": 2,
                    "summary": _api_summary(project), **project["ir"], "advisories": project.get("advisories", [])}
        headers = {"ETag": f'"{project["etag"]}"', "Location": f"/api/schema/ir/{project_id}"}
        if deprecated: headers["Deprecation"] = "true"
        _save_idempotency(repository, request, user, body, body_out, 200, headers)
        return JSONResponse(_safe(body_out), headers=headers)

    @app.get("/api/schema/projects")
    def list_projects(request: Request, direction: str | None = None, status: str | None = None, limit: int = 50, cursor: str | None = None,
                      user: dict[str, str] = Depends(_get_user)):
        _require(user, "viewer")
        if not 1 <= limit <= 500: raise ApiProblem(400, "invalid_request", "Invalid limit", "limit must be 1..500.")
        items = repository.list_projects()
        if direction: items = [item for item in items if item.get("direction") == direction]
        if status: items = [item for item in items if item.get("status") == status]
        items.sort(key=lambda item: item.get("project_id", ""))
        if cursor:
            after = decode_cursor(cursor)[0]
            items = [item for item in items if item.get("project_id", "") > after]
        page = items[:limit]
        more = len(items) > limit
        return {"items": [{key: item.get(key) for key in ("project_id", "revision", "etag", "ir_kind", "source_engine", "direction", "status", "created_at", "updated_at")} for item in page],
                "next_cursor": encode_cursor([page[-1]["project_id"]]) if more and page else None, "has_more": more}

    @app.get("/api/schema/ir/{project_id}")
    def get_ir(project_id: str, request: Request, view: str = "full", collection: str | None = None, table: str | None = None,
               include_stats: bool = True, if_none_match: str | None = Header(default=None, alias="If-None-Match"),
               user: dict[str, str] = Depends(_get_user)):
        _require(user, "viewer")
        project = _need_project(repository, project_id)
        if _normal_etag(if_none_match) == project["etag"]:
            return Response(status_code=304, headers={"ETag": f'"{project["etag"]}"'})
        ir = json.loads(json.dumps(project.get("ir", {})))
        if not include_stats:
            for item in ir.get("tables", []): item.pop("stats", None)
            for item in ir.get("collections", []): item.pop("avg_doc_bytes", None)
        if table:
            ir["tables"] = [item for item in ir.get("tables", []) if item.get("table_name") == table]
            ir["relationships"] = [rel for rel in ir.get("relationships", []) if rel.get("parent_table") == table or rel.get("child_table") == table]
        if collection:
            ir["collections"] = [item for item in ir.get("collections", []) if item.get("name") == collection]
            ir["decisions"] = [item for item in ir.get("decisions", []) if item.get("collection") == collection]
        if view == "decisions":
            ir = {key: value for key, value in ir.items() if key in {"relationships", "decisions"}}
        elif view == "summary":
            ir = {"summary": _api_summary(project)}
        elif view != "full":
            raise ApiProblem(400, "invalid_request", "Invalid IR view", "view must be full, decisions, or summary.")
        response = {"project_id": project_id, "revision": project["revision"], "etag": project["etag"], "ir_kind": project["ir_kind"],
                    "source_engine": project["source_engine"], "direction": project["direction"], **ir,
                    "advisories": project.get("advisories", [])}
        return JSONResponse(_safe(response), headers={"ETag": f'"{project["etag"]}"'})

    @app.get("/api/schema/ir/{project_id}/sample")
    def sample_project_data(project_id: str, request: Request, limit: int = Query(default=5, ge=1, le=5),
                            user: dict[str, str] = Depends(_get_user)):
        """Return at most the first five live source records per table or collection."""
        _require(user, "viewer")
        project = _need_project(repository, project_id)
        connection_id = str(project.get("source_connection_id", ""))
        metadata = _connection(repository, connection_id, role="source", engine=project["source_engine"])
        credentials = _vault(request).get(connection_id)
        samples: list[dict[str, Any]] = []
        try:
            if project.get("ir_kind") == "relational":
                engine = create_sql_engine(metadata["engine"], credentials)
                try:
                    from sqlalchemy import text
                    with engine.connect() as connection:
                        preparer = connection.dialect.identifier_preparer
                        for table in project.get("ir", {}).get("tables", []):
                            table_name = str(table["table_name"])
                            qualified = preparer.quote(table_name)
                            schema = table.get("schema_name")
                            if schema:
                                qualified = preparer.quote_schema(str(schema)) + "." + qualified
                            pk_columns = list((table.get("primary_key") or {}).get("columns") or [])
                            order_by = ""
                            if pk_columns:
                                order_by = " ORDER BY " + ", ".join(preparer.quote(str(column)) for column in pk_columns)
                            result = connection.execute(text(f"SELECT * FROM {qualified}{order_by} LIMIT :sample_limit"),
                                                        {"sample_limit": limit}).mappings()
                            samples.append({"name": table_name, "rows": [_safe(dict(row)) for row in result]})
                finally:
                    engine.dispose()
            elif project.get("ir_kind") == "document":
                database_name = str(metadata.get("database") or credentials.get("database") or "")
                if not database_name:
                    raise ValueError("MongoDB source database is not set")
                client = create_mongo_client(credentials)
                try:
                    database = client[database_name]
                    for collection in project.get("ir", {}).get("collections", []):
                        name = str(collection["name"])
                        rows = list(database[name].find({}).limit(limit))
                        samples.append({"name": name, "rows": [_safe(row) for row in rows]})
                finally:
                    client.close()
            else:
                raise ValueError("unsupported project kind")
        except Exception as exc:
            raise ApiProblem(502, "sample_read_failed", "Could not preview source data",
                             "The source connection could not return sample records. Reconnect it and try again.") from exc
        return {"project_id": project_id, "limit_per_table_or_collection": limit,
                "source_engine": project["source_engine"], "items": samples}

    @app.get("/api/schema/ir/{project_id}/revisions")
    def list_revisions(project_id: str, user: dict[str, str] = Depends(_get_user)):
        _require(user, "viewer")
        project = _need_project(repository, project_id)
        history = list(project.get("history", []))
        history.append({"revision": project["revision"], "etag": project["etag"], "author": project.get("author"),
                        "created_at": project.get("updated_at"), "change_summary": project.get("last_change", "current revision")})
        return {"items": [{key: item.get(key) for key in ("revision", "etag", "author", "created_at", "change_summary")} for item in sorted(history, key=lambda x: int(x["revision"]))]}

    @app.get("/api/schema/ir/{project_id}/revisions/{revision}")
    def get_revision(project_id: str, revision: int, user: dict[str, str] = Depends(_get_user)):
        _require(user, "viewer")
        project = _need_project(repository, project_id)
        record = next((item for item in project.get("history", []) if int(item.get("revision", -1)) == revision), None)
        if revision == int(project["revision"]): record = {"revision": revision, "etag": project["etag"], "ir": project["ir"], "advisories": project.get("advisories", [])}
        if record is None: raise ApiProblem(404, "revision_not_found", "Revision not found", str(revision))
        return _safe(record)

    @app.post("/api/schema/ir/{project_id}/restore")
    def restore_revision(project_id: str, request: Request, body: dict[str, Any] = Body(...), if_match: str | None = Header(default=None, alias="If-Match"),
                         user: dict[str, str] = Depends(_get_user)):
        _require(user, "editor")
        project = _need_project(repository, project_id)
        _require_if_match(project, if_match)
        revision = int(body.get("revision", 0))
        prior = next((item for item in project.get("history", []) if int(item.get("revision", -1)) == revision), None)
        if revision == int(project["revision"]): prior = project
        if prior is None: raise ApiProblem(404, "revision_not_found", "Revision not found", str(revision))
        updated = _advance_project(repository, project, ir=prior.get("ir", {}), advisories=prior.get("advisories", []),
                                   thresholds=prior.get("thresholds", project.get("thresholds", {})), author=user["sub"],
                                   change_summary=f"restored revision {revision}")
        return JSONResponse({"project_id": project_id, "revision": updated["revision"], "etag": updated["etag"]}, headers={"ETag": f'"{updated["etag"]}"'})

    @app.delete("/api/schema/projects/{project_id}", status_code=204)
    def delete_project(project_id: str, user: dict[str, str] = Depends(_get_user)):
        _require(user, "editor")
        project = _need_project(repository, project_id)
        active = [job for job in repository.list_jobs() if job.get("project_id") == project_id and job.get("status") in {"queued", "running", "waiting_for_credentials"}]
        if active: raise ApiProblem(409, "project_in_use", "Project is in use", "Cancel active jobs before deleting this project.")
        for plan in repository.list_plans():
            if plan.get("project_id") == project_id: _delete_plan_artifacts(repository, plan)
        repository.delete_project(project_id)
        return Response(status_code=204)

    @app.patch("/api/schema/ir/{project_id}/relationship/{rel_id}")
    def patch_relationship(project_id: str, rel_id: str, request: Request, body: dict[str, Any] = Body(...),
                           if_match: str | None = Header(default=None, alias="If-Match"), user: dict[str, str] = Depends(_get_user)):
        _require(user, "editor")
        project = _need_project(repository, project_id)
        _require_if_match(project, if_match)
        if project.get("ir_kind") != "relational": raise ApiProblem(422, "override_not_allowed", "Wrong IR kind", "Relationship overrides apply only to relational IR.")
        rels = [dict(item) for item in project["ir"].get("relationships", [])]
        relation = next((item for item in rels if item.get("rel_id") == rel_id), None)
        if relation is None: raise ApiProblem(404, "relationship_not_found", "Relationship not found", rel_id)
        value = body.get("user_override")
        if value is not None:
            try: mapping = ForwardMapping(value)
            except ValueError: raise ApiProblem(422, "override_not_allowed", "Invalid relationship override", str(value))
            cardinality = relation["cardinality"]
            allowed = {"REFERENCE"}
            if cardinality == "1:1": allowed.update({"EMBED", "EMBED_ARRAY"})
            if cardinality == "1:N": allowed.add("EMBED_ARRAY")
            if cardinality == "M:N": allowed.add("REFERENCE_ARRAY")
            if value not in allowed:
                raise ApiProblem(422, "override_not_allowed", "Override not allowed", f"{value} is incompatible with cardinality {cardinality}.",
                                 errors=[{"path": "$.user_override", "code": "invalid_value", "message": "allowed: " + ", ".join(sorted(allowed))}])
        relation["user_override"] = value
        relation["override_reason"] = body.get("reason")
        warnings: list[dict[str, Any]] = []
        if value in {"EMBED", "EMBED_ARRAY"} and relation.get("rule_id") in {"H5", "H6", "H8"}:
            warnings.append({"code": "BSON_SIZE_RISK", "message": "The override selects embedding despite a bounded-fan-out or cycle warning."})
        updated_ir = dict(project["ir"])
        updated_ir["relationships"] = rels
        updated = _advance_project(repository, project, ir=updated_ir, author=user["sub"], change_summary=f"relationship {rel_id} override")
        tables = {item["table_name"]: item for item in updated_ir.get("tables", [])}
        parent = tables.get(relation.get("parent_table"), {})
        size_estimate = int(parent.get("stats", {}).get("avg_row_bytes", 0)) + int(relation.get("estimated_embedded_bytes", 0))
        response_relation = {**relation, "effective_mapping": relation.get("user_override") or relation.get("proposed_mapping")}
        result = {"project_id": project_id, "revision": updated["revision"], "etag": updated["etag"],
                  "relationship": response_relation, "warnings": warnings,
                  "document_preview_size_estimate": {"root": relation.get("parent_table"), "p99_bytes": size_estimate, "limit_bytes": 16 * 1024 * 1024}}
        return JSONResponse(_safe(result), headers={"ETag": f'"{updated["etag"]}"'})

    @app.patch("/api/schema/ir/{project_id}/field/{decision_id}")
    def patch_decision(project_id: str, decision_id: str, request: Request, body: dict[str, Any] = Body(...),
                       if_match: str | None = Header(default=None, alias="If-Match"), user: dict[str, str] = Depends(_get_user)):
        _require(user, "editor")
        project = _need_project(repository, project_id)
        _require_if_match(project, if_match)
        if project.get("ir_kind") != "document": raise ApiProblem(422, "override_not_allowed", "Wrong IR kind", "Field decisions apply only to document IR.")
        decisions = [dict(item) for item in project["ir"].get("decisions", [])]
        decision = next((item for item in decisions if item.get("decision_id") == decision_id), None)
        if decision is None: raise ApiProblem(404, "decision_not_found", "Decision not found", decision_id)
        value = body.get("user_override")
        try: action = ReverseAction(value) if value is not None else None
        except ValueError: raise ApiProblem(422, "override_not_allowed", "Invalid reverse override", str(value))
        document_ir = DocumentIR.model_validate(project["ir"])
        node = next((node for collection_item in document_ir.collections if collection_item.name == decision["collection"] for root in collection_item.fields for node in root.walk() if node.path == decision["path"]), None)
        current_action = ReverseAction(decision.get("user_override") or decision["proposed_action"])
        allowed = allowed_reverse_overrides(node, current_action)
        if action is not None and action not in allowed:
            raise ApiProblem(422, "override_not_allowed", "Override not allowed", f"{action.value} cannot be applied to {decision['collection']}.{decision['path']}.",
                             errors=[{"path": "$.user_override", "code": "invalid_value", "message": "allowed: " + ", ".join(sorted(item.value for item in allowed))}])
        decision["user_override"] = value
        decision["override_reason"] = body.get("reason")
        updated_ir = dict(project["ir"])
        updated_ir["decisions"] = decisions
        updated = _advance_project(repository, project, ir=updated_ir, author=user["sub"], change_summary=f"field {decision_id} override")
        response_decision = {**decision, "effective_action": decision.get("user_override") or decision.get("proposed_action")}
        return JSONResponse({"project_id": project_id, "revision": updated["revision"], "etag": updated["etag"], "decision": response_decision,
                             "derived_changes": {"tables_removed": [], "columns_added": [], "indexes_dropped": []}, "warnings": []},
                            headers={"ETag": f'"{updated["etag"]}"'})

    @app.post("/api/schema/ir/{project_id}/overrides:batch")
    def batch_overrides(project_id: str, body: dict[str, Any] = Body(...), if_match: str | None = Header(default=None, alias="If-Match"),
                        user: dict[str, str] = Depends(_get_user)):
        _require(user, "editor")
        project = _need_project(repository, project_id)
        _require_if_match(project, if_match)
        overrides = body.get("overrides", [])
        if not isinstance(overrides, list) or len(overrides) > 200:
            raise ApiProblem(400, "invalid_request", "Invalid override batch", "overrides must be an array of at most 200 items.")
        ir = json.loads(json.dumps(project["ir"]))
        applied = 0
        failures: list[dict[str, Any]] = []
        for index, item in enumerate(overrides):
            target = item.get("target")
            id_key = "rel_id" if target == "relationship" else "decision_id"
            array_key = "relationships" if target == "relationship" else "decisions"
            candidate = next((entry for entry in ir.get(array_key, []) if entry.get(id_key) == item.get("id")), None)
            if candidate is None:
                failures.append({"index": index, "code": "not_found", "message": f"{target} {item.get('id')} not found"})
                continue
            value = item.get("user_override")
            if target == "relationship":
                cardinality = candidate.get("cardinality")
                allowed = {"REFERENCE"} | ({"EMBED", "EMBED_ARRAY"} if cardinality == "1:1" else {"EMBED_ARRAY"} if cardinality == "1:N" else {"REFERENCE_ARRAY"})
                if value is not None and value not in allowed:
                    failures.append({"index": index, "code": "override_not_allowed", "message": f"allowed: {', '.join(sorted(allowed))}"})
                    continue
                candidate["user_override"] = value
                candidate["override_reason"] = item.get("reason")
            elif target == "field":
                try: action = ReverseAction(value) if value is not None else None
                except ValueError:
                    failures.append({"index": index, "code": "override_not_allowed", "message": "unknown ReverseAction"})
                    continue
                doc_ir = DocumentIR.model_validate(ir)
                node = next((node for c in doc_ir.collections if c.name == candidate["collection"] for root in c.fields for node in root.walk() if node.path == candidate["path"]), None)
                allowed = allowed_reverse_overrides(node, ReverseAction(candidate.get("user_override") or candidate["proposed_action"]))
                if action is not None and action not in allowed:
                    failures.append({"index": index, "code": "override_not_allowed", "message": "allowed: " + ", ".join(sorted(a.value for a in allowed))})
                    continue
                candidate["user_override"] = value
                candidate["override_reason"] = item.get("reason")
            else:
                failures.append({"index": index, "code": "invalid_target", "message": "target must be relationship or field"})
                continue
            applied += 1
        if failures:
            raise ApiProblem(422, "override_not_allowed", "Override batch rejected", "No overrides were applied because at least one item was invalid.", errors=failures)
        updated = _advance_project(repository, project, ir=ir, author=user["sub"], change_summary=f"applied {applied} batch overrides")
        return JSONResponse({"revision": updated["revision"], "etag": updated["etag"], "applied": applied, "warnings": []}, headers={"ETag": f'"{updated["etag"]}"'})

    @app.post("/api/schema/ir/{project_id}/reanalyze")
    def reanalyze(project_id: str, body: dict[str, Any] = Body(...), if_match: str | None = Header(default=None, alias="If-Match"),
                  user: dict[str, str] = Depends(_get_user)):
        _require(user, "editor")
        project = _need_project(repository, project_id)
        _require_if_match(project, if_match)
        thresholds = Thresholds.from_mapping({**project.get("thresholds", {}), **(body.get("thresholds") or {})})
        old_ir = project["ir"]
        changed: list[dict[str, Any]] = []
        advisories = [dict(item) for item in project.get("advisories", []) if item.get("acknowledged_by")]
        if project["ir_kind"] == "relational":
            relational = RelationalIR.model_validate(old_ir)
            relationships = propose_forward(relational, thresholds)
            old_by_id = {item["rel_id"]: item for item in old_ir.get("relationships", [])}
            if not body.get("reset_overrides", False):
                relationships = [rel.model_copy(update={"user_override": old_by_id.get(rel.rel_id, {}).get("user_override"),
                                                         "override_reason": old_by_id.get(rel.rel_id, {}).get("override_reason")}) for rel in relationships]
            for rel in relationships:
                before = old_by_id.get(rel.rel_id, {}).get("effective_mapping") or old_by_id.get(rel.rel_id, {}).get("user_override") or old_by_id.get(rel.rel_id, {}).get("proposed_mapping")
                after = (rel.user_override or rel.proposed_mapping).value
                if before and before != after: changed.append({"id": rel.rel_id, "from": before, "to": after, "rule_id": f"{old_by_id.get(rel.rel_id, {}).get('rule_id', '')}→{rel.rule_id}"})
            updated_ir = relational.model_copy(update={"relationships": relationships}).model_dump(mode="json")
            advisory_models = analyze_workload(relational.model_copy(update={"relationships": relationships}), relationships, thresholds)
            advisories.extend(item.model_dump(mode="json") for item in advisory_models)
        else:
            document = DocumentIR.model_validate(old_ir)
            decisions = propose_reverse(document, thresholds)
            old_by_id = {item["decision_id"]: item for item in old_ir.get("decisions", [])}
            if not body.get("reset_overrides", False):
                decisions = [decision.model_copy(update={"user_override": old_by_id.get(decision.decision_id, {}).get("user_override"),
                                                        "override_reason": old_by_id.get(decision.decision_id, {}).get("override_reason")}) for decision in decisions]
            for decision in decisions:
                before = old_by_id.get(decision.decision_id, {}).get("effective_action") or old_by_id.get(decision.decision_id, {}).get("user_override") or old_by_id.get(decision.decision_id, {}).get("proposed_action")
                after = (decision.user_override or decision.proposed_action).value
                if before and before != after: changed.append({"id": decision.decision_id, "from": before, "to": after, "rule_id": f"{old_by_id.get(decision.decision_id, {}).get('rule_id', '')}→{decision.rule_id}"})
            updated_ir = document.model_copy(update={"decisions": decisions}).model_dump(mode="json")
        previous_ids = {item.get("advisory_id") for item in project.get("advisories", [])}
        current_ids = {item.get("advisory_id") for item in advisories}
        updated = _advance_project(repository, project, ir=updated_ir, advisories=advisories, thresholds=thresholds.model_dump(mode="json"),
                                   author=user["sub"], change_summary="heuristics re-analyzed")
        return JSONResponse({"revision": updated["revision"], "etag": updated["etag"], "changed_decisions": changed,
                             "advisories": {"added": len(current_ids - previous_ids), "removed": len(previous_ids - current_ids)}}, headers={"ETag": f'"{updated["etag"]}"'})

    @app.get("/api/schema/ir/{project_id}/advisories")
    def list_advisories(project_id: str, severity: str | None = None, rule_id: str | None = None, acknowledged: bool | None = None,
                        user: dict[str, str] = Depends(_get_user)):
        _require(user, "viewer")
        project = _need_project(repository, project_id)
        items = list(project.get("advisories", []))
        if severity: items = [item for item in items if item.get("severity") == severity]
        if rule_id: items = [item for item in items if item.get("rule_id") == rule_id]
        if acknowledged is not None: items = [item for item in items if bool(item.get("acknowledged_by")) == acknowledged]
        return {"items": items}

    @app.post("/api/schema/ir/{project_id}/advisories/{advisory_id}/acknowledge")
    def acknowledge_advisory(project_id: str, advisory_id: str, body: dict[str, Any] = Body(...), if_match: str | None = Header(default=None, alias="If-Match"),
                             user: dict[str, str] = Depends(_get_user)):
        _require(user, "editor")
        project = _need_project(repository, project_id)
        _require_if_match(project, if_match)
        advisories = [dict(item) for item in project.get("advisories", [])]
        advisory = next((item for item in advisories if item.get("advisory_id") == advisory_id), None)
        if advisory is None: raise ApiProblem(404, "advisory_not_found", "Advisory not found", advisory_id)
        advisory.update(acknowledged_by=user["sub"], acknowledged_at=utc_now(), acknowledgement_note=body.get("note"))
        updated = _advance_project(repository, project, advisories=advisories, author=user["sub"], change_summary=f"acknowledged advisory {advisory_id}")
        return JSONResponse({"revision": updated["revision"], "etag": updated["etag"], "advisory": advisory}, headers={"ETag": f'"{updated["etag"]}"'})

    @app.post("/api/schema/ir/{project_id}/preview")
    def preview(project_id: str, body: dict[str, Any] = Body(...), user: dict[str, str] = Depends(_get_user)):
        _require(user, "viewer")
        project = _need_project(repository, project_id)
        if project["ir_kind"] == "relational":
            ir = RelationalIR.model_validate(project["ir"])
            emitted = __import__("synapse.adapters.emitters.document_emitter", fromlist=["DocumentEmitter"]).DocumentEmitter().emit(ir, ir.relationships, project_id=project_id)
            root_name = body.get("root") or next(iter(emitted.validators), None)
            if root_name not in emitted.validators: raise ApiProblem(404, "table_not_found", "Root table not found", str(root_name))
            document = {"_id": "000000000000000000000001"}
            root = next((table for table in ir.tables if table.table_name == root_name or table.qualified_name == root_name), None)
            if root:
                for column in root.columns:
                    document[column.name] = _synthetic_value(column.canonical_type.value, column.name)
            document["_legacy"] = {"table": root_name, "pk": body.get("sample_key", [1])}
            validator = emitted.validators[root_name]
            return {"document": document, "size_bytes": len(json.dumps(document, default=str).encode()),
                    "size_p99_estimate_bytes": (root.stats.avg_row_bytes if root else 0), "validator": validator}
        document = DocumentIR.model_validate(project["ir"])
        emitted = RelationalEmitter(dialect=str(body.get("dialect", "mysql")), options={"thresholds": project.get("thresholds", {})}).emit(document, document.decisions, project_id=project_id)
        collection = body.get("collection")
        nodes = [table for table in emitted.tables if table.get("collection") == collection and table.get("role") != "system"] if collection else [table for table in emitted.tables if table.get("role") != "system"]
        response = {"collection": collection, "nodes": [{"id": table["name"], "kind": "junction" if table["role"] == "junction" else "table", "origin": table["role"], "columns": len(table.get("columns", []))} for table in nodes],
                    "edges": [{"from": table["name"], "to": fk["target_table"], "columns": fk["columns"], "ref_columns": fk["target_columns"],
                               "on_delete": fk.get("on_delete", "RESTRICT"), "cardinality": "N:1"} for table in nodes for fk in table.get("foreign_keys", [])],
                    "field_map": [{"path": key, "target": value, "rule": "RH1"} for key, value in emitted.naming.items()]}
        format_value = body.get("format", "graph")
        if format_value in {"ddl", "both"}: response["ddl"] = emitted.pre_load_sql + "\n" + emitted.post_load_sql
        if format_value not in {"graph", "ddl", "both"}: raise ApiProblem(400, "invalid_request", "Invalid preview format", "format must be graph, ddl, or both.")
        return response

    # Immutable generation and artifacts.
    @app.post("/api/schema/generate", status_code=201)
    def generate(request: Request, body: dict[str, Any] = Body(...), user: dict[str, str] = Depends(_get_user)):
        _require(user, "editor")
        replay = _idempotent_replay(repository, request, user, body)
        if replay: return replay
        project = _need_project(repository, str(body.get("project_id", "")))
        target_engine = str(body.get("target_engine", "")).lower()
        try:
            wrapper, artifacts = compile_plan(repository, project, target_engine=target_engine, if_revision=body.get("if_revision"),
                                              targets=body.get("targets"), options=body.get("options"),
                                              acknowledge_advisories=body.get("acknowledge_advisories"), author=user["sub"])
        except PermissionError as exc:
            missing = str(exc).split(":", 1)[-1].split(",")
            raise ApiProblem(409, "advisory_ack_required", "High advisories require acknowledgement", "Acknowledge all listed HIGH advisories before generation.",
                             errors=[{"path": "$.acknowledge_advisories", "code": "advisory_ack_required", "message": item} for item in missing]) from exc
        except ValueError as exc:
            code = str(exc)
            if code == "revision_mismatch": raise ApiProblem(409, code, "Revision mismatch", "The project moved beyond if_revision.")
            if code == "target_engine_mismatch": raise ApiProblem(422, code, "Target engine mismatch", "The target engine is incompatible with the source direction.")
            raise ApiProblem(422, "plan_invalid", "Plan could not be generated", str(exc)) from exc
        except RuntimeError as exc:
            raise ApiProblem(409, "revision_mismatch", "Concurrent IR update", str(exc)) from exc
        apply_id = (body.get("options") or {}).get("apply_ddl_to_connection_id")
        if apply_id:
            metadata = _connection(repository, str(apply_id), role="target", engine=target_engine)
            plan = ApprovedPlan.model_validate(wrapper["plan"])
            try:
                if target_engine == "mongodb":
                    credentials = _vault(request).get(str(apply_id))
                    client = create_mongo_client(credentials)
                    try:
                        _apply_mongo_plan(client[metadata["database"]], plan)
                    finally: client.close()
                else:
                    credentials = _vault(request).get(str(apply_id))
                    engine = create_sql_engine(target_engine, credentials)
                    try:
                        with engine.begin() as connection:
                            _execute_sql_script(connection, plan.options.get("pre_load_sql", ""))
                    finally: engine.dispose()
                wrapper.update(applied=True, applied_connection_id=str(apply_id), applied_at=utc_now())
                repository.save_plan(wrapper)
            except Exception as exc:
                raise ApiProblem(502, "target_unreachable", "Target DDL application failed", "The target could not apply generated schema artifacts.") from exc
        result = {"plan_id": wrapper["plan_id"], "plan_hash": wrapper["plan_hash"], "project_id": wrapper["project_id"],
                  "ir_revision": wrapper["plan"]["ir_revision"], "direction": wrapper["plan"]["direction"],
                  "target_engine": target_engine,
                  "collections_or_tables": [table.get("name", table.get("table_name")) for table in wrapper["plan"].get("tables", [])] or [unit["root"] for unit in wrapper["plan"].get("units", [])],
                  "artifacts": artifacts, "not_translated": wrapper.get("not_translated", []), "warnings": wrapper.get("warnings", []),
                  "applied": wrapper.get("applied", False)}
        _save_idempotency(repository, request, user, body, result, 201, {"Location": f"/api/plans/{wrapper['plan_id']}"})
        return JSONResponse(_safe(result), status_code=201, headers={"Location": f"/api/plans/{wrapper['plan_id']}"})

    @app.get("/api/artifacts/{artifact_id}")
    def get_artifact(artifact_id: str, user: dict[str, str] = Depends(_get_user)):
        _require(user, "viewer")
        result = repository.get_artifact(artifact_id)
        if result is None: raise ApiProblem(404, "artifact_not_found", "Artifact not found", artifact_id)
        metadata, content = result
        return Response(content, media_type="application/octet-stream", headers={"Content-Disposition": f'attachment; filename="{metadata["name"]}"',
                                                                                     "ETag": f'"{metadata["sha256"]}"'})

    @app.get("/api/plans/{plan_id}")
    def get_plan(plan_id: str, user: dict[str, str] = Depends(_get_user)):
        _require(user, "viewer")
        wrapper = _need_plan(repository, plan_id)
        return _safe({key: value for key, value in wrapper.items() if key != "artifacts"} | {"artifacts": wrapper.get("artifacts", [])})

    @app.get("/api/plans/{plan_id}/artifacts")
    def get_plan_artifacts(plan_id: str, user: dict[str, str] = Depends(_get_user)):
        _require(user, "viewer")
        _need_plan(repository, plan_id)
        return {"items": repository.artifacts_for_plan(plan_id)}

    @app.get("/api/plans/{plan_id}/bundle.zip")
    def get_plan_bundle(plan_id: str, user: dict[str, str] = Depends(_get_user)):
        _require(user, "viewer")
        wrapper = _need_plan(repository, plan_id)
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("plan.json", json.dumps(wrapper["plan"], sort_keys=True, indent=2))
            for artifact in repository.artifacts_for_plan(plan_id):
                found = repository.get_artifact(artifact["artifact_id"])
                if found: archive.writestr(artifact["name"], found[1])
        return Response(buffer.getvalue(), media_type="application/zip", headers={"Content-Disposition": f'attachment; filename="{plan_id}.zip"'})

    # Migration jobs and durable worker controls.
    def launch_worker(job_id: str) -> None:
        def execute():
            try: JobWorker(repository).run_job(job_id)
            except Exception:
                current = repository.get_job(job_id)
                if current:
                    current.update(status="failed", error_code="worker_start_failed", error="The worker could not start.", updated_at=utc_now())
                    repository.save_job(current)
        request_app = app
        request_app.state.executor.submit(execute)

    def create_migration_job(request: Request, body: dict[str, Any], user: dict[str, str], *, replay_parent: str | None = None,
                             selected_errors: list[str] | None = None) -> dict[str, Any]:
        plan_wrapper = _need_plan(repository, str(body.get("plan_id", "")))
        plan = ApprovedPlan.model_validate(plan_wrapper["plan"])
        if not plan.verify_hash(): raise ApiProblem(422, "plan_invalid", "Invalid Approved Plan", "The immutable plan hash does not verify.")
        source_id = str(body.get("source_connection_id") or plan_wrapper.get("source_connection_id") or "")
        target_id = str(body.get("target_connection_id") or "")
        source_meta = _connection(repository, source_id, role="source")
        target_meta = _connection(repository, target_id, role="target", engine=plan.target_engine.value)
        if source_meta["engine"] == "mongodb" and not str(source_meta.get("database") or "").strip():
            raise ApiProblem(422, "source_database_required", "MongoDB source database missing", "Reconnect this MongoDB source and specify its database name.")
        if target_meta["engine"] == "mongodb" and not str(target_meta.get("database") or "").strip():
            raise ApiProblem(422, "target_database_required", "MongoDB target database missing", "Reconnect this MongoDB target and specify its database name.")
        if plan.direction.value == "DOCUMENT_TO_RELATIONAL" and source_meta["engine"] != "mongodb":
            raise ApiProblem(422, "source_engine_mismatch", "Source engine mismatch", "Reverse plans require a MongoDB source.")
        if plan.direction.value == "RELATIONAL_TO_DOCUMENT" and source_meta["engine"] not in {"sqlite", "mysql", "postgresql"}:
            raise ApiProblem(422, "source_engine_mismatch", "Source engine mismatch", "Forward plans require a relational source.")
        mode = str(body.get("mode", "run"))
        if mode not in {"run", "dry_run"}: raise ApiProblem(400, "invalid_request", "Invalid migration mode", "mode must be run or dry_run.")
        options = dict(body.get("options") or {})
        drop_first = bool(body.get("drop_first", False))
        if drop_first:
            if body.get("confirm_token") != plan.plan_hash[:8]: raise ApiProblem(400, "invalid_request", "Drop confirmation mismatch", "confirm_token must equal the first eight plan-hash characters.")
            existing_checkpoints = [item for item in repository.list_jobs() if item.get("plan_id") == plan.plan_id
                                    and item.get("target_connection_id") == target_id
                                    and (repository.job_dir / item.get("job_id", "") / "checkpoints.sqlite").exists()]
            if existing_checkpoints:
                raise ApiProblem(409, "resume_forbids_drop_first", "Drop-first forbidden on resume", "A checkpoint already exists for this plan and target.")
            if not plan_wrapper.get("applied") or plan_wrapper.get("applied_connection_id") != target_id:
                raise ApiProblem(409, "target_not_owned", "Target objects are not owned", "Only objects created by this applied plan on this target may be dropped.")
            _drop_plan_objects(request, plan_wrapper, target_meta, target_id)
        job_id = opaque_id("job")
        units = [{"unit_id": unit["unit_id"], "stage": "forward" if plan.direction.value == "RELATIONAL_TO_DOCUMENT" else "phase1"} for unit in plan_wrapper["plan"].get("units", [])]
        if plan.direction.value == "DOCUMENT_TO_RELATIONAL":
            units += [{"unit_id": unit["unit_id"], "stage": "phase2"} for unit in plan_wrapper["plan"].get("units", [])]
        job_kind = "replay" if replay_parent else "migration"
        job = {"job_id": job_id, "job_type": job_kind, "kind": job_kind, "status": "queued", "plan_id": plan.plan_id,
               "plan_hash": plan.plan_hash, "project_id": plan.project_id, "direction": plan.direction.value,
               "source_connection_id": source_id, "target_connection_id": target_id, "mode": mode, "options": options,
               "units": units, "progress": {"units": []}, "created_at": utc_now(), "updated_at": utc_now(),
               "replay_parent_job_id": replay_parent, "selected_errors": selected_errors or []}
        repository.save_job(job)
        repository.save_connection_job_marker(source_id, job_id, True)
        repository.save_connection_job_marker(target_id, job_id, True)
        launch_worker(job_id)
        return {"job_id": job_id, "status": "queued", "plan_id": plan.plan_id, "plan_hash": plan.plan_hash,
                "units": units, "links": {"self": f"/api/migrations/{job_id}", "errors": f"/api/migrations/{job_id}/errors", "events": f"/api/jobs/{job_id}/events"}}

    @app.post("/api/migrations", status_code=202)
    def create_migration(request: Request, body: dict[str, Any] = Body(...), user: dict[str, str] = Depends(_get_user)):
        _require(user, "operator")
        replay = _idempotent_replay(repository, request, user, body)
        if replay: return replay
        result = create_migration_job(request, body, user)
        headers = {"Location": f"/api/jobs/{result['job_id']}"}
        _save_idempotency(repository, request, user, body, result, 202, headers)
        return JSONResponse(result, status_code=202, headers=headers)

    def job_view(job: dict[str, Any]) -> dict[str, Any]:
        view = {key: value for key, value in job.items() if key not in {"source_connection_id", "target_connection_id", "options", "traceback", "control", "selected_errors"}}
        # Expose only the final Python frame, never the raw traceback which could contain sensitive driver details.
        frames = re.findall(r'^\s*File "([^"]+)", line (\d+), in ([^\r\n]+)', str(job.get("traceback", "")), re.MULTILINE)
        if frames:
            filename, line_number, function = frames[-1]
            view["error_location"] = f"{Path(filename).name}:{line_number} ({function})"
        return _safe(view)

    @app.get("/api/migrations/{job_id}")
    def get_migration(job_id: str, user: dict[str, str] = Depends(_get_user)):
        _require(user, "viewer")
        job = repository.get_job(job_id)
        if not job or job.get("job_type") not in {"migration", "verify", "replay"}: raise ApiProblem(404, "job_not_found", "Migration job not found", job_id)
        return job_view(job)

    @app.get("/api/migrations")
    def list_migrations(status: str | None = None, project_id: str | None = None, limit: int = 50, cursor: str | None = None,
                        user: dict[str, str] = Depends(_get_user)):
        _require(user, "viewer")
        if not 1 <= limit <= 500: raise ApiProblem(400, "invalid_request", "Invalid limit", "limit must be 1..500.")
        jobs = [job for job in repository.list_jobs() if job.get("job_type") in {"migration", "verify", "replay"}]
        if status: jobs = [job for job in jobs if job.get("status") == status]
        if project_id: jobs = [job for job in jobs if job.get("project_id") == project_id]
        jobs.sort(key=lambda item: item.get("job_id", ""))
        if cursor:
            after = decode_cursor(cursor)[0]
            jobs = [job for job in jobs if job["job_id"] > after]
        page = jobs[:limit]
        more = len(jobs) > limit
        return {"items": [job_view(item) for item in page], "next_cursor": encode_cursor([page[-1]["job_id"]]) if more and page else None, "has_more": more}

    @app.get("/api/jobs/{job_id}")
    def get_job(job_id: str, user: dict[str, str] = Depends(_get_user)):
        _require(user, "viewer")
        job = repository.get_job(job_id)
        if not job: raise ApiProblem(404, "job_not_found", "Job not found", job_id)
        return job_view(job)

    @app.get("/api/migrations/{job_id}/checkpoints")
    def migration_checkpoints(job_id: str, user: dict[str, str] = Depends(_get_user)):
        _require(user, "viewer")
        job = repository.get_job(job_id)
        if not job: raise ApiProblem(404, "job_not_found", "Job not found", job_id)
        db_path = repository.job_dir / job_id / "checkpoints.sqlite"
        if not db_path.exists(): return {"items": []}
        manager = CheckpointManager(job_id, db_path, repository.store)
        try: items = [checkpoint.to_dict() for checkpoint in manager.list()]
        finally: manager.close()
        return {"items": items}

    @app.post("/api/migrations/{job_id}/pause", status_code=202)
    def pause_migration(job_id: str, user: dict[str, str] = Depends(_get_user)):
        _require(user, "operator")
        job = repository.get_job(job_id)
        if not job: raise ApiProblem(404, "job_not_found", "Job not found", job_id)
        if job.get("status") == "queued": job["status"] = "paused"
        elif job.get("status") == "running": job["control"] = "pause_requested"
        else: raise ApiProblem(409, "invalid_job_state", "Job cannot be paused", job.get("status", ""))
        job["updated_at"] = utc_now(); repository.save_job(job)
        return JSONResponse({"job_id": job_id, "status": job["status"], "requested": job.get("control")}, status_code=202)

    @app.post("/api/migrations/{job_id}/resume", status_code=202)
    def resume_migration(job_id: str, request: Request, body: dict[str, Any] = Body(default={}), user: dict[str, str] = Depends(_get_user)):
        _require(user, "operator")
        job = repository.get_job(job_id)
        if not job: raise ApiProblem(404, "job_not_found", "Job not found", job_id)
        wrapper = _need_plan(repository, job.get("plan_id", ""))
        plan = ApprovedPlan.model_validate(wrapper["plan"])
        if plan.plan_hash != job.get("plan_hash"): raise ApiProblem(409, "plan_changed", "Plan changed", "Resume requires the exact Approved Plan hash.")
        for key in ("source_connection_id", "target_connection_id"):
            if body.get(key): job[key] = body[key]
        _connection(repository, job["source_connection_id"], role="source")
        _connection(repository, job["target_connection_id"], role="target", engine=plan.target_engine.value)
        if job.get("status") not in {"paused", "waiting_for_credentials", "failed", "failed_threshold"}:
            raise ApiProblem(409, "invalid_job_state", "Job cannot be resumed", job.get("status", ""))
        job.update(status="queued", control=None, updated_at=utc_now(), error=None, error_code=None)
        repository.save_job(job); launch_worker(job_id)
        return JSONResponse({"job_id": job_id, "status": "queued"}, status_code=202, headers={"Location": f"/api/jobs/{job_id}"})

    @app.post("/api/migrations/{job_id}/cancel", status_code=202)
    def cancel_migration(job_id: str, user: dict[str, str] = Depends(_get_user)):
        _require(user, "operator")
        job = repository.get_job(job_id)
        if not job: raise ApiProblem(404, "job_not_found", "Job not found", job_id)
        if job.get("status") == "queued": job["status"] = "cancelled"
        elif job.get("status") == "running": job["control"] = "cancel_requested"
        else: raise ApiProblem(409, "invalid_job_state", "Job cannot be cancelled", job.get("status", ""))
        job["updated_at"] = utc_now(); repository.save_job(job)
        return JSONResponse({"job_id": job_id, "status": job["status"], "requested": job.get("control")}, status_code=202)

    def _dlq(job_id: str) -> DeadLetterLog:
        directory = repository.job_dir / job_id
        return DeadLetterLog(directory / "migration_errors.log", job_id, directory / "checkpoints.sqlite")

    @app.get("/api/migrations/{job_id}/errors/download")
    def download_errors(job_id: str, user: dict[str, str] = Depends(_get_user)):
        _require(user, "viewer")
        if not repository.get_job(job_id): raise ApiProblem(404, "job_not_found", "Job not found", job_id)
        log = _dlq(job_id)
        try: data = log.download()
        finally: log.close()
        return Response(data, media_type="application/x-ndjson", headers={"Content-Disposition": f'attachment; filename="migration_errors.log"'})

    @app.post("/api/migrations/{job_id}/errors/replay", status_code=202)
    def replay_errors(job_id: str, request: Request, body: dict[str, Any] = Body(...), user: dict[str, str] = Depends(_get_user)):
        _require(user, "operator")
        replay = _idempotent_replay(repository, request, user, body)
        if replay: return replay
        job = repository.get_job(job_id)
        if not job: raise ApiProblem(404, "job_not_found", "Job not found", job_id)
        log = _dlq(job_id)
        try:
            codes = set(body.get("codes", [])); units = set(body.get("units", [])); maximum = min(10_000, max(1, int(body.get("max_items", 10_000))))
            selected = []
            cursor = None
            while len(selected) < maximum:
                page, cursor, more = log.list(resolved=False, limit=min(500, maximum - len(selected)), cursor=cursor)
                selected.extend(item for item in page if item.get("replayable") and (not codes or item.get("error_code") in codes) and (not units or item.get("unit") in units))
                if not more or not page: break
        finally: log.close()
        if not selected: raise ApiProblem(422, "nothing_replayable", "No replayable errors", "The selected DLQ entries are not replayable against this plan.")
        wrapper = _need_plan(repository, job["plan_id"])
        new_body = {"plan_id": job["plan_id"], "source_connection_id": job["source_connection_id"], "target_connection_id": job["target_connection_id"],
                    "mode": "run", "options": {**job.get("options", {}), "on_drift": body.get("on_drift", job.get("options", {}).get("on_drift", "widen"))}}
        result = create_migration_job(request, new_body, user, replay_parent=job_id, selected_errors=[item["error_id"] for item in selected])
        result = {"replay_job_id": result["job_id"], "selected": len(selected)}
        _save_idempotency(repository, request, user, body, result, 202)
        return JSONResponse(result, status_code=202, headers={"Location": f"/api/jobs/{result['replay_job_id']}"})

    @app.get("/api/migrations/{job_id}/errors/{error_id}")
    def get_error(job_id: str, error_id: str, user: dict[str, str] = Depends(_get_user)):
        _require(user, "viewer")
        if not repository.get_job(job_id): raise ApiProblem(404, "job_not_found", "Job not found", job_id)
        log = _dlq(job_id)
        try: item = log.get(error_id)
        finally: log.close()
        if item is None: raise ApiProblem(404, "error_not_found", "Error not found", error_id)
        return _safe(item)

    @app.get("/api/migrations/{job_id}/errors")
    def list_errors(job_id: str, code: str | None = None, unit: str | None = None, stage: str | None = None,
                    resolved: bool | None = False, limit: int = 100, cursor: str | None = None,
                    user: dict[str, str] = Depends(_get_user)):
        _require(user, "viewer")
        if not repository.get_job(job_id): raise ApiProblem(404, "job_not_found", "Job not found", job_id)
        log = _dlq(job_id)
        try: items, next_cursor, more = log.list(code=code, unit=unit, stage=stage, resolved=resolved, limit=limit, cursor=cursor)
        except ValueError as exc: raise ApiProblem(400, "invalid_request", "Invalid error page", str(exc)) from exc
        finally: log.close()
        return {"items": _safe(items), "next_cursor": next_cursor, "has_more": more}

    @app.post("/api/migrations/{job_id}/verify", status_code=202)
    def start_verification(job_id: str, user: dict[str, str] = Depends(_get_user)):
        _require(user, "operator")
        job = repository.get_job(job_id)
        if not job: raise ApiProblem(404, "job_not_found", "Migration not found", job_id)
        if job.get("status") not in {"completed", "completed_with_errors", "paused"}:
            raise ApiProblem(409, "job_not_complete", "Migration is not ready for verification", job.get("status", ""))
        verify_id = opaque_id("job")
        verify_job = {"job_id": verify_id, "job_type": "verify", "kind": "verify", "status": "queued", "plan_id": job["plan_id"],
                      "plan_hash": job["plan_hash"], "project_id": job.get("project_id"), "direction": job.get("direction"),
                      "source_connection_id": job["source_connection_id"], "target_connection_id": job["target_connection_id"],
                      "parent_job_id": job_id, "created_at": utc_now(), "updated_at": utc_now()}
        repository.save_job(verify_job); launch_worker(verify_id)
        return JSONResponse({"job_id": verify_id, "status": "queued"}, status_code=202, headers={"Location": f"/api/jobs/{verify_id}"})

    @app.get("/api/migrations/{job_id}/verification")
    def get_verification(job_id: str, user: dict[str, str] = Depends(_get_user)):
        _require(user, "viewer")
        job = repository.get_job(job_id)
        if not job: raise ApiProblem(404, "job_not_found", "Migration not found", job_id)
        if job.get("job_type") == "verify": return _safe(job.get("result", {"status": job.get("status")}))
        verification = (job.get("result") or {}).get("verification")
        if verification is None: raise ApiProblem(404, "verification_not_found", "Verification not found", job_id)
        return _safe(verification)

    @app.get("/api/jobs/{job_id}/events")
    def job_events(job_id: str, request: Request, last_event_id: str | None = Header(default=None, alias="Last-Event-ID"),
                   user: dict[str, str] = Depends(_get_user)):
        _require(user, "viewer")
        if not repository.get_job(job_id): raise ApiProblem(404, "job_not_found", "Job not found", job_id)
        start = int(last_event_id or 0)
        def stream():
            last = start
            terminal = {"completed", "completed_with_errors", "failed", "failed_threshold", "cancelled"}
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                for event in repository.list_events(job_id, after_id=last):
                    last = int(event["id"])
                    yield f"id: {last}\nevent: {event.get('event','message')}\ndata: {json.dumps(_safe(event), separators=(',', ':'))}\n\n"
                current = repository.get_job(job_id)
                if not current or current.get("status") in terminal:
                    yield "event: done\ndata: {}\n\n"
                    return
                time.sleep(1)
            yield ": keep-alive\n\n"
        return StreamingResponse(stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    # Verified CRUD. A plan must have created the target schema before it is discoverable here.
    def _mongo_action(collection: str, operation: str, request: Request, user: dict[str, str], body: dict[str, Any] | None = None,
                      item_id: str | None = None, if_match: str | None = None, restrict: bool = True):
        client, database, repo, wrapper, _metadata = _mongo_repo_context(request, collection)
        try:
            if operation == "create": return repo.create(body or {})
            if operation == "list":
                filters = _split_filter_params(request)
                limit = int(request.query_params.get("limit", 50))
                cursor = request.query_params.get("cursor")
                after = decode_cursor(cursor) if cursor else None
                fields = request.query_params.get("fields")
                projection = {field: 1 for field in fields.split(",")} if fields else None
                allow_scan = request.query_params.get("allow_scan", "false").lower() == "true"
                if allow_scan: _require(user, "operator")
                docs, next_key, more = repo.list(limit=limit, after=after, filters=filters, projection=projection, allow_scan=allow_scan,
                                                 sort=request.query_params.get("sort", "_id"))
                return {"items": docs, "next_cursor": encode_cursor(next_key) if next_key is not None else None, "has_more": more}
            if operation == "get": return repo.get(item_id)
            if operation == "replace": return repo.replace(item_id, body or {}, if_match=_parse_if_match_version(if_match))
            if operation == "patch":
                if body and "ops" in body: return repo.patch_operations(item_id, body, if_match=_parse_if_match_version(if_match))
                return repo.patch_merge(item_id, body or {}, if_match=_parse_if_match_version(if_match))
            if operation == "delete": return {"deleted": repo.delete(item_id, if_match=_parse_if_match_version(if_match), restrict=restrict)}
            raise ApiProblem(400, "invalid_request", "Unknown CRUD operation", operation)
        except MongoDocumentNotFound as exc: raise ApiProblem(404, "not_found", "Document not found", str(exc)) from exc
        except MongoVersionConflict as exc: raise ApiProblem(412, "version_mismatch", "Version mismatch", str(exc)) from exc
        except MongoValidationError as exc: raise ApiProblem(422, "validation_failed", "Validation failed", str(exc), errors=exc.errors) from exc
        except RuntimeError as exc:
            if "referenced" in str(exc): raise ApiProblem(409, "referenced", "Document is referenced", str(exc)) from exc
            raise
        except ValueError as exc: raise ApiProblem(400, "invalid_request", "Invalid CRUD request", str(exc)) from exc
        except Exception as exc: raise _database_problem(exc) from exc
        finally:
            client.close()

    def _parse_if_match_version(value: str | None) -> int | None:
        if value is None: return None
        raw = _normal_etag(value)
        try: return int(raw or "")
        except ValueError as exc: raise ApiProblem(400, "invalid_request", "Invalid If-Match", "CRUD If-Match must contain a numeric version.") from exc

    def _crud_response(value: Any, status_code: int = 200):
        safe = _safe(value)
        version = safe.get("_version") if isinstance(safe, dict) else safe.get("row_version") if isinstance(safe, dict) else None
        headers = {"ETag": f'"{version}"'} if version is not None else {}
        return JSONResponse(safe, status_code=status_code, headers=headers)

    @app.post("/api/crud/mongodb/{collection}", status_code=201)
    def mongo_create(collection: str, request: Request, body: dict[str, Any] = Body(...), user: dict[str, str] = Depends(_get_user)):
        _require(user, "editor")
        replay = _idempotent_replay(repository, request, user, body)
        if replay: return replay
        result = _mongo_action(collection, "create", request, user, body=body)
        response = _safe(result)
        headers = {"Location": f"/api/crud/mongodb/{collection}/{response.get('_id')}"}
        if response.get("_version") is not None: headers["ETag"] = f'"{response["_version"]}"'
        _save_idempotency(repository, request, user, body, response, 201, headers)
        return JSONResponse(response, status_code=201, headers=headers)

    @app.get("/api/crud/mongodb/{collection}")
    def mongo_list(collection: str, request: Request, user: dict[str, str] = Depends(_get_user)):
        _require(user, "viewer")
        return _crud_response(_mongo_action(collection, "list", request, user))

    @app.get("/api/crud/mongodb/{collection}/{item_id}")
    def mongo_get(collection: str, item_id: str, request: Request, user: dict[str, str] = Depends(_get_user)):
        _require(user, "viewer")
        result = _mongo_action(collection, "get", request, user, item_id=item_id)
        return _crud_response(result)

    @app.put("/api/crud/mongodb/{collection}/{item_id}")
    def mongo_replace(collection: str, item_id: str, request: Request, body: dict[str, Any] = Body(...), if_match: str | None = Header(default=None, alias="If-Match"),
                      user: dict[str, str] = Depends(_get_user)):
        _require(user, "editor")
        result = _mongo_action(collection, "replace", request, user, body=body, item_id=item_id, if_match=if_match)
        return _crud_response(result)

    @app.patch("/api/crud/mongodb/{collection}/{item_id}")
    def mongo_patch(collection: str, item_id: str, request: Request, body: dict[str, Any] = Body(...), if_match: str | None = Header(default=None, alias="If-Match"),
                    user: dict[str, str] = Depends(_get_user)):
        _require(user, "editor")
        result = _mongo_action(collection, "patch", request, user, body=body, item_id=item_id, if_match=if_match)
        return _crud_response(result)

    @app.delete("/api/crud/mongodb/{collection}/{item_id}")
    def mongo_delete(collection: str, item_id: str, request: Request, if_match: str | None = Header(default=None, alias="If-Match"),
                     restrict: bool = True, user: dict[str, str] = Depends(_get_user)):
        _require(user, "editor")
        return _crud_response(_mongo_action(collection, "delete", request, user, item_id=item_id, if_match=if_match, restrict=restrict))

    @app.post("/api/crud/sql/{table_name}", status_code=201)
    def sql_create(table_name: str, request: Request, body: dict[str, Any] = Body(...), user: dict[str, str] = Depends(_get_user)):
        _require(user, "editor")
        replay = _idempotent_replay(repository, request, user, body)
        if replay: return replay
        engine, repo, wrapper, _metadata = _sql_repo_context(request, table_name)
        try:
            result = repo.create(body)
            safe = _safe(result)
            headers = {"Location": f"/api/crud/sql/{table_name}/{safe.get(repo.root['id_column'])}", "ETag": f'"{safe.get("row_version", 1)}"'}
            _save_idempotency(repository, request, user, body, safe, 201, headers)
            return JSONResponse(safe, status_code=201, headers=headers)
        except SqlValidationError as exc: raise ApiProblem(422, "validation_failed", "Validation failed", str(exc), errors=exc.errors) from exc
        except Exception as exc:
            if isinstance(exc, ApiProblem): raise
            raise _database_problem(exc) from exc
        finally: engine.dispose()

    @app.get("/api/crud/sql/{table_name}")
    def sql_list(table_name: str, request: Request, user: dict[str, str] = Depends(_get_user)):
        _require(user, "viewer")
        engine, repo, wrapper, _metadata = _sql_repo_context(request, table_name)
        try:
            filters: dict[str, Any] = {}
            for key, value in request.query_params.multi_items():
                match = re.fullmatch(r"filter\[([^]]+)\](?:\[([^]]+)\])?", key)
                if not match: continue
                field, operator = match.groups()
                if operator == "in": filters[field] = {"$in": [_parse_scalar(item) for item in value.split(",") if item != ""]}
                elif operator in {"gte", "gt", "lte", "lt", "ne"}:
                    filters[field] = {operator: _parse_scalar(value)}
                else: filters[field] = _parse_scalar(value)
            allow_scan = request.query_params.get("allow_scan", "false").lower() == "true"
            if allow_scan: _require(user, "operator")
            plan_table = repo.root
            indexed = {name for item in plan_table.get("indexes", []) for name in item.get("columns", [])}
            indexed.update(plan_table.get("primary_key", []))
            if not allow_scan:
                bad = [field for field in filters if field not in indexed]
                if bad: raise ApiProblem(400, "unindexed_filter", "Filter requires an index", ", ".join(bad))
            items, cursor, more = repo.list(limit=int(request.query_params.get("limit", 50)), cursor=request.query_params.get("cursor"), filters=filters)
            return _crud_response({"items": items, "next_cursor": cursor, "has_more": more})
        except ApiProblem: raise
        except Exception as exc: raise ApiProblem(400, "invalid_request", "Invalid SQL list query", str(exc)) from exc
        finally: engine.dispose()

    @app.get("/api/crud/sql/{table_name}/{item_id}")
    def sql_get(table_name: str, item_id: str, request: Request, by: str | None = None, user: dict[str, str] = Depends(_get_user)):
        _require(user, "viewer")
        engine, repo, _wrapper, _metadata = _sql_repo_context(request, table_name)
        try:
            identifier: Any = int(item_id) if item_id.isdigit() else item_id
            result = repo.get(identifier, by=by)
            return _crud_response(result)
        except SqlAggregateNotFound as exc: raise ApiProblem(404, "not_found", "SQL aggregate not found", str(exc)) from exc
        except Exception as exc:
            if isinstance(exc, ApiProblem): raise
            raise _database_problem(exc) from exc
        finally: engine.dispose()

    @app.put("/api/crud/sql/{table_name}/{item_id}")
    def sql_replace(table_name: str, item_id: str, request: Request, body: dict[str, Any] = Body(...), if_match: str | None = Header(default=None, alias="If-Match"),
                    user: dict[str, str] = Depends(_get_user)):
        _require(user, "editor")
        engine, repo, _wrapper, _metadata = _sql_repo_context(request, table_name)
        try:
            identifier: Any = int(item_id) if item_id.isdigit() else item_id
            result = repo.replace(identifier, body, if_match=_parse_if_match_version(if_match))
            return _crud_response(result)
        except SqlAggregateNotFound as exc: raise ApiProblem(404, "not_found", "SQL aggregate not found", str(exc)) from exc
        except SqlVersionConflict as exc: raise ApiProblem(412, "version_mismatch", "Version mismatch", str(exc)) from exc
        except SqlValidationError as exc: raise ApiProblem(422, "validation_failed", "Validation failed", str(exc), errors=exc.errors) from exc
        except Exception as exc:
            if isinstance(exc, ApiProblem): raise
            raise _database_problem(exc) from exc
        finally: engine.dispose()

    @app.patch("/api/crud/sql/{table_name}/{item_id}")
    def sql_patch(table_name: str, item_id: str, request: Request, body: dict[str, Any] = Body(...), if_match: str | None = Header(default=None, alias="If-Match"),
                  user: dict[str, str] = Depends(_get_user)):
        _require(user, "editor")
        engine, repo, _wrapper, _metadata = _sql_repo_context(request, table_name)
        try:
            identifier: Any = int(item_id) if item_id.isdigit() else item_id
            result = repo.patch(identifier, body, if_match=_parse_if_match_version(if_match))
            return _crud_response(result)
        except SqlAggregateNotFound as exc: raise ApiProblem(404, "not_found", "SQL aggregate not found", str(exc)) from exc
        except SqlVersionConflict as exc: raise ApiProblem(412, "version_mismatch", "Version mismatch", str(exc)) from exc
        except SqlValidationError as exc: raise ApiProblem(422, "validation_failed", "Validation failed", str(exc), errors=exc.errors) from exc
        except Exception as exc:
            if isinstance(exc, ApiProblem): raise
            raise _database_problem(exc) from exc
        finally: engine.dispose()

    @app.delete("/api/crud/sql/{table_name}/{item_id}")
    def sql_delete(table_name: str, item_id: str, request: Request, if_match: str | None = Header(default=None, alias="If-Match"),
                   restrict: bool = True, user: dict[str, str] = Depends(_get_user)):
        _require(user, "editor")
        engine, repo, _wrapper, _metadata = _sql_repo_context(request, table_name)
        try:
            identifier: Any = int(item_id) if item_id.isdigit() else item_id
            repo.delete(identifier, if_match=_parse_if_match_version(if_match), restrict=restrict)
            return Response(status_code=204)
        except SqlAggregateNotFound as exc: raise ApiProblem(404, "not_found", "SQL aggregate not found", str(exc)) from exc
        except SqlVersionConflict as exc: raise ApiProblem(412, "version_mismatch", "Version mismatch", str(exc)) from exc
        except RuntimeError as exc:
            if "referenced" in str(exc): raise ApiProblem(409, "referenced", "SQL aggregate is referenced", str(exc)) from exc
            raise
        except Exception as exc:
            if isinstance(exc, ApiProblem): raise
            raise _database_problem(exc) from exc
        finally: engine.dispose()

    # Backwards-compatible demo aliases.
    @app.post("/api/users", status_code=201)
    def create_users_alias(request: Request, body: dict[str, Any] = Body(...), user: dict[str, str] = Depends(_get_user)):
        _require(user, "editor")
        result = _mongo_action("users", "create", request, user, body=body)
        return _crud_response(result, 201)

    @app.get("/api/users")
    def list_users_alias(request: Request, user: dict[str, str] = Depends(_get_user)):
        _require(user, "viewer")
        return _crud_response(_mongo_action("users", "list", request, user))

    @app.post("/api/orders", status_code=201)
    def create_orders_alias(request: Request, body: dict[str, Any] = Body(...), user: dict[str, str] = Depends(_get_user)):
        _require(user, "editor")
        result = _mongo_action("orders", "create", request, user, body=body)
        return _crud_response(result, 201)

    @app.get("/api/orders")
    def list_orders_alias(request: Request, user: dict[str, str] = Depends(_get_user)):
        _require(user, "viewer")
        return _crud_response(_mongo_action("orders", "list", request, user))

    return app


def _synthetic_value(canonical_type: str, name: str) -> Any:
    if canonical_type in {"int", "long"}: return 1
    if canonical_type in {"double", "decimal"}: return "1.00" if canonical_type == "decimal" else 1.0
    if canonical_type == "bool": return False
    if canonical_type in {"datetime", "date"}: return "2026-01-01T00:00:00Z"
    if canonical_type in {"json", "array"}: return {}
    if canonical_type == "binary": return {"$binary": "00"}
    return f"example_{name}"


def _execute_sql_script(connection: Any, script: str) -> None:
    from sqlalchemy import text
    for statement in (part.strip() for part in script.split(";")):
        if statement: connection.execute(text(statement))


def _apply_mongo_plan(database: Any, plan: ApprovedPlan) -> None:
    for name, validator in plan.options.get("validators", {}).items():
        if name not in database.list_collection_names():
            database.create_collection(name, validator=validator, validationLevel="strict", validationAction="error")
        else:
            database.command({"collMod": name, "validator": validator, "validationLevel": "strict", "validationAction": "error"})
    for name, indexes in plan.options.get("indexes", {}).items():
        for index in indexes: database[name].create_index(list(index["keys"].items()), **index.get("options", {}))


def _drop_plan_objects(request: Request, wrapper: dict[str, Any], target_meta: dict[str, Any], connection_id: str) -> None:
    plan = ApprovedPlan.model_validate(wrapper["plan"])
    credentials = _vault(request).get(connection_id)
    if plan.target_engine.value == "mongodb":
        client = create_mongo_client(credentials)
        try:
            database = client[target_meta["database"]]
            expected = set(plan.options.get("validators", {}))
            existing = set(database.list_collection_names()) & expected
            if existing - expected: raise ApiProblem(409, "target_not_owned", "Foreign collections found", "Refusing to drop collections outside the plan.")
            for name in existing: database.drop_collection(name)
        finally: client.close()
    else:
        engine = create_sql_engine(plan.target_engine.value, credentials)
        try:
            from sqlalchemy import inspect, text
            expected = {table["name"] for table in plan.tables if table.get("role") != "system"}
            with engine.begin() as connection:
                existing = set(inspect(connection).get_table_names()) & expected
                for table in reversed([item["name"] for item in plan.tables if item.get("role") != "system" and item["name"] in existing]):
                    q = connection.dialect.identifier_preparer.quote(table)
                    suffix = " CASCADE" if connection.dialect.name == "postgresql" else ""
                    connection.execute(text(f"DROP TABLE {q}{suffix}"))
        finally: engine.dispose()


app = create_app()
