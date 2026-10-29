"""Standalone plan runner and API job-control CLI."""
from __future__ import annotations

import argparse
import json
import os
import secrets
import sys
from pathlib import Path
from typing import Any

from synapse.adapters.connections import create_mongo_client, create_sql_engine
from synapse.adapters.idmap.sqlite_store import SqliteIdMapStore
from synapse.adapters.state.file_store import FileStateStore
from synapse.domain.ir.plan import ApprovedPlan
from synapse.etl.checkpoint import CheckpointManager
from synapse.etl.dlq import DeadLetterLog
from synapse.etl.forward import ForwardETLRunner
from synapse.etl.reverse import ReverseETLRunner
from synapse.etl.verify import Verifier


def _database(client: Any, uri: str, configured: str | None = None):
    if configured:
        return client[configured]
    try:
        return client.get_default_database()
    except Exception as exc:
        raise ValueError("MongoDB URL must include a database name or pass --source-database/--target-database") from exc


def _mongo_schema(database: Any, plan: ApprovedPlan) -> None:
    for name, validator in plan.options.get("validators", {}).items():
        if name not in database.list_collection_names():
            database.create_collection(name, validator=validator, validationLevel="strict", validationAction="error")
        else:
            database.command({"collMod": name, "validator": validator, "validationLevel": "strict", "validationAction": "error"})
    for name, indexes in plan.options.get("indexes", {}).items():
        for index in indexes:
            database[name].create_index(list(index["keys"].items()), **index.get("options", {}))


def _open_endpoints(plan: ApprovedPlan, source_url: str, target_url: str, source_database: str | None, target_database: str | None):
    if plan.direction.value == "RELATIONAL_TO_DOCUMENT":
        source_engine_name = "mysql" if source_url.startswith(("mysql:", "mysql+")) else "postgresql"
        source_engine = create_sql_engine(source_engine_name, {"url": source_url})
        target_client = create_mongo_client({"uri": target_url})
        target_db = _database(target_client, target_url, target_database)
        return source_engine, target_db, source_engine, target_client, None, source_engine_name
    source_client = create_mongo_client({"uri": source_url})
    source_db = _database(source_client, source_url, source_database)
    target_engine_name = plan.target_engine.value
    target_engine = create_sql_engine(target_engine_name, {"url": target_url})
    return source_db, target_engine, target_engine, None, source_client, target_engine_name


def run_plan_file(plan_file: str = "plan.json", *, source_url: str | None = None, target_url: str | None = None,
                  job_id: str | None = None, job_dir: str | None = None, source_database: str | None = None,
                  target_database: str | None = None, verify: bool = False, replay_parent: str | None = None) -> dict[str, Any]:
    plan_path = Path(plan_file).expanduser().resolve()
    plan = ApprovedPlan.model_validate_json(plan_path.read_text(encoding="utf-8"))
    if not plan.verify_hash(): raise ValueError("Approved Plan hash is invalid")
    source_url = source_url or os.getenv("SYNAPSE_SOURCE_URL")
    target_url = target_url or os.getenv("SYNAPSE_TARGET_URL")
    if not source_url or not target_url:
        raise ValueError("set SYNAPSE_SOURCE_URL and SYNAPSE_TARGET_URL, or pass --source-url and --target-url")
    job_id = job_id or os.getenv("SYNAPSE_JOB_ID") or f"cli_{secrets.token_hex(8)}"
    root_dir = Path(job_dir or os.getenv("SYNAPSE_JOB_DIR", "./var/jobs")).expanduser().resolve()
    directory = root_dir / job_id
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "plan.json").write_text(json.dumps(plan.as_json(), sort_keys=True, indent=2), encoding="utf-8")
    source, target, sql_engine, mongo_client, source_client, _engine_name = _open_endpoints(plan, source_url, target_url, source_database, target_database)
    state = FileStateStore(root_dir / "state")
    checkpoints = CheckpointManager(job_id, directory / "checkpoints.sqlite", state)
    map_directory = root_dir / replay_parent if replay_parent else directory
    idmap = SqliteIdMapStore(map_directory / "idmap.sqlite", cache_entries=int(plan.etl_options.get("idmap_cache_entries", 100_000)))
    dlq = DeadLetterLog(directory / "migration_errors.log", job_id, directory / "checkpoints.sqlite",
                        redact_payload=bool(plan.etl_options.get("redact_payload", False)))
    lease = checkpoints.lease(f"cli-{os.getpid()}")
    try:
        with lease:
            options = {**plan.etl_options, "apply_ddl": True}
            if plan.direction.value == "RELATIONAL_TO_DOCUMENT":
                _mongo_schema(target, plan)
                runner = ForwardETLRunner(plan, source, target, idmap, checkpoints, dlq, options=options, lease=lease)
            else:
                runner = ReverseETLRunner(plan, source, target, idmap, checkpoints, dlq, options=options, lease=lease)
            result = runner.run()
            if verify and result.get("status", "").startswith("completed"):
                if plan.direction.value == "RELATIONAL_TO_DOCUMENT":
                    with source.connect() as connection:
                        result["verification"] = Verifier(idmap=idmap).verify_forward(connection, target, plan)
                else:
                    with target.connect() as connection:
                        result["verification"] = Verifier(idmap=idmap).verify_reverse(source, connection, plan)
            return result
    finally:
        dlq.close()
        idmap.close()
        checkpoints.close()
        state.close()
        if sql_engine is not None: sql_engine.dispose()
        if mongo_client is not None: mongo_client.close()
        if source_client is not None: source_client.close()


def _api_request(method: str, path: str, body: dict[str, Any] | None = None) -> Any:
    import requests
    base = os.getenv("SYNAPSE_API_URL", "http://localhost:8000/api").rstrip("/")
    token = os.getenv("SYNAPSE_API_TOKEN", "")
    if not token: raise ValueError("SYNAPSE_API_TOKEN is required for API job controls")
    response = requests.request(method, f"{base}{path}", json=body, headers={"Authorization": f"Bearer {token}"}, timeout=60)
    if not response.ok: raise RuntimeError(f"API returned {response.status_code}: {response.text[:500]}")
    return response.json() if response.content else None


def main() -> None:
    parser = argparse.ArgumentParser(prog="synapse", description="SynapseMigrate migration tools")
    sub = parser.add_subparsers(dest="command", required=True)
    migrate = sub.add_parser("migrate", help="run an immutable Approved Plan directly")
    migrate.add_argument("plan_file", nargs="?", default="plan.json")
    migrate.add_argument("--source-url", default=os.getenv("SYNAPSE_SOURCE_URL"))
    migrate.add_argument("--target-url", default=os.getenv("SYNAPSE_TARGET_URL"))
    migrate.add_argument("--source-database", default=os.getenv("SYNAPSE_SOURCE_DATABASE"))
    migrate.add_argument("--target-database", default=os.getenv("SYNAPSE_TARGET_DATABASE"))
    migrate.add_argument("--job-id", default=None)
    migrate.add_argument("--verify", action="store_true")
    resume = sub.add_parser("resume", help="resume a direct CLI migration using its stored plan and checkpoints")
    resume.add_argument("job_id")
    resume.add_argument("--job-dir", default=os.getenv("SYNAPSE_JOB_DIR", "./var/jobs"))
    replay = sub.add_parser("replay-errors", help="create an idempotent replay job through the API")
    replay.add_argument("--job", required=True)
    replay.add_argument("--code", action="append", default=[])
    replay.add_argument("--unit", action="append", default=[])
    replay.add_argument("--max-items", type=int, default=10000)
    verify = sub.add_parser("verify", help="start verification for an API migration job")
    verify.add_argument("job_id")
    args = parser.parse_args()
    try:
        if args.command == "migrate":
            result = run_plan_file(args.plan_file, source_url=args.source_url, target_url=args.target_url,
                                   job_id=args.job_id, source_database=args.source_database, target_database=args.target_database, verify=args.verify)
        elif args.command == "resume":
            path = Path(args.job_dir).expanduser().resolve() / args.job_id / "plan.json"
            result = run_plan_file(str(path), job_id=args.job_id, job_dir=args.job_dir,
                                   source_database=os.getenv("SYNAPSE_SOURCE_DATABASE"), target_database=os.getenv("SYNAPSE_TARGET_DATABASE"))
        elif args.command == "replay-errors":
            result = _api_request("POST", f"/migrations/{args.job}/errors/replay", {"codes": args.code, "units": args.unit, "max_items": args.max_items})
        else:
            result = _api_request("POST", f"/migrations/{args.job_id}/verify", {})
        print(json.dumps(result, sort_keys=True, indent=2, default=str))
    except Exception as exc:
        print(f"synapse: {exc}", file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
