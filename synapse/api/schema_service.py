"""Schema orchestration: introspection, deterministic analysis, and immutable plan compilation."""
from __future__ import annotations

import time
from typing import Any

from synapse.adapters.emitters.document_emitter import DocumentEmitter
from synapse.adapters.emitters.relational_emitter import RelationalEmitter
from synapse.adapters.vault import CredentialVault
from synapse.api.store import ApiRepository, opaque_id, utc_now
from synapse.domain.heuristics.forward import propose_forward
from synapse.domain.heuristics.reverse import propose_reverse
from synapse.domain.heuristics.thresholds import Thresholds
from synapse.domain.heuristics.workload import analyze_workload
from synapse.domain.ir.common import Direction, Engine
from synapse.domain.ir.document import DocumentIR
from synapse.domain.ir.plan import ApprovedPlan, PlanUnit
from synapse.domain.ir.relational import RelationalIR
from synapse.adapters.extractors.document import DocumentExtractor
from synapse.adapters.extractors.relational import RelationalExtractor
from synapse.adapters.connections import create_mongo_client, create_sql_engine


def _history_entry(project: dict[str, Any], change_summary: str) -> dict[str, Any]:
    return {"revision": project["revision"], "etag": project["etag"], "author": project.get("author", "system"),
            "created_at": project.get("updated_at", project.get("created_at")), "change_summary": change_summary,
            "ir": project.get("ir", {}), "advisories": project.get("advisories", []), "thresholds": project.get("thresholds", {})}


def run_introspection(repository: ApiRepository, vault: CredentialVault, *, project_id: str, source_engine: str,
                      connection_id: str, options: dict[str, Any], author: str = "system") -> dict[str, Any]:
    metadata = repository.get_connection(connection_id)
    if metadata is None:
        raise LookupError("connection_not_found")
    credentials = vault.get(connection_id)
    started = time.monotonic()
    thresholds = Thresholds.from_mapping(options.get("thresholds"))
    advisories: list[dict[str, Any]] = []
    if source_engine in {"mysql", "postgresql", "sqlite"}:
        engine = create_sql_engine(source_engine, credentials)
        try:
            with engine.connect() as connection:
                relational = RelationalExtractor().extract(connection, options)
            threshold_values = thresholds.model_dump()
            if "infer_implicit_fks" in options:
                threshold_values["infer_implicit_fks"] = bool(options["infer_implicit_fks"])
            thresholds = Thresholds.model_validate(threshold_values)
            relationships = propose_forward(relational, thresholds)
            relational = relational.model_copy(update={"relationships": relationships})
            advisories = [item.model_dump(mode="json") for item in analyze_workload(relational, relationships, thresholds)]
            ir_payload = relational.model_dump(mode="json")
            ir_kind = "relational"
            direction = Direction.RELATIONAL_TO_DOCUMENT.value
        finally:
            engine.dispose()
    elif source_engine == "mongodb":
        database_name = str(options.get("database") or metadata.get("database") or credentials.get("database") or "")
        if not database_name:
            raise ValueError("MongoDB database must be specified in the connection or introspection options")
        client = create_mongo_client(credentials, timeout_ms=int((options.get("sample") or {}).get("max_time_ms", 20_000)))
        try:
            document_ir = DocumentExtractor().extract(client[database_name], options)
            decisions = propose_reverse(document_ir, thresholds)
            document_ir = document_ir.model_copy(update={"decisions": decisions})
            ir_payload = document_ir.model_dump(mode="json")
            ir_kind = "document"
            direction = Direction.DOCUMENT_TO_RELATIONAL.value
            for collection in document_ir.collections:
                if collection.sample.random_cursor_fallback:
                    advisories.append({"advisory_id": opaque_id("adv"), "rule_id": "SAMPLE_FALLBACK", "severity": "MEDIUM",
                                       "target": collection.name, "message": "Randomized sample fallback was used because $sample was unavailable.",
                                       "metrics": {}, "recommendation": "Review sample representativeness before approving reverse decisions.",
                                       "acknowledged_by": None, "acknowledged_at": None})
            for decision in decisions:
                if decision.rule_id == "RH2" and "unresolved ObjectId array" in str(decision.evidence.get("reason", "")):
                    advisories.append({"advisory_id": decision.advisory_ids[0] if decision.advisory_ids else opaque_id("adv"),
                                       "rule_id": "UNRESOLVED_REFERENCE", "severity": "MEDIUM", "target": decision.decision_id,
                                       "message": f"Reference target for {decision.collection}.{decision.path} could not be verified.",
                                       "metrics": {}, "recommendation": "Confirm the referenced collection or retain the value as JSON.",
                                       "acknowledged_by": None, "acknowledged_at": None})
        finally:
            client.close()
    else:
        raise ValueError("unsupported_source_type")
    elapsed = round((time.monotonic() - started) * 1000, 2)
    now = utc_now()
    project = {"project_id": project_id, "ir_version": 2, "revision": 1, "etag": "", "ir_kind": ir_kind,
               "source_engine": source_engine, "direction": direction, "thresholds": thresholds.model_dump(mode="json"),
               "advisories": advisories, "ir": ir_payload, "source_connection_id": connection_id,
               "source_metadata": {key: metadata.get(key) for key in ("name", "engine", "database", "host", "port", "verified")},
               "status": "analyzed", "created_at": now, "updated_at": now, "author": author,
               "history": [], "last_change": "initial introspection", "duration_ms": elapsed}
    repository.save_project(project, expected_revision=None)
    project["history"] = [_history_entry(project, "initial introspection")]
    repository.store.set(f"project:{project_id}", project)
    return project


def compile_plan(repository: ApiRepository, project: dict[str, Any], *, target_engine: str, if_revision: int | None,
                 targets: list[str] | None, options: dict[str, Any] | None, acknowledge_advisories: list[str] | None,
                 author: str = "system") -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if if_revision is not None and int(if_revision) != int(project["revision"]):
        raise ValueError("revision_mismatch")
    if project["direction"] == Direction.RELATIONAL_TO_DOCUMENT.value and target_engine != "mongodb":
        raise ValueError("target_engine_mismatch")
    if project["direction"] == Direction.DOCUMENT_TO_RELATIONAL.value and target_engine not in {"mysql", "postgresql"}:
        raise ValueError("target_engine_mismatch")
    options = dict(options or {})
    selected_targets = targets or (["ddl", "etl_python", "verify_sql", "report"] if target_engine != "mongodb" else ["validators", "indexes", "etl_python", "report"])
    accepted_targets = {"ddl", "validators", "indexes", "etl_python", "etl_node", "verify_sql", "report", "etl"}
    if set(selected_targets) - accepted_targets:
        raise ValueError("invalid_generation_target")
    acknowledged = set(acknowledge_advisories or [])
    high = [item for item in project.get("advisories", []) if item.get("severity") == "HIGH" and not item.get("acknowledged_by")]
    missing = [item["advisory_id"] for item in high if item["advisory_id"] not in acknowledged]
    if missing:
        raise PermissionError("advisory_ack_required:" + ",".join(missing))
    if acknowledged:
        updated_project = dict(project)
        copied = [dict(item) for item in project.get("advisories", [])]
        for advisory in copied:
            if advisory.get("advisory_id") in acknowledged and not advisory.get("acknowledged_by"):
                advisory.update(acknowledged_by=author, acknowledged_at=utc_now(), acknowledgement_note="acknowledged atomically with plan generation")
        updated_project["advisories"] = copied
        updated_project["history"] = list(project.get("history", [])) + [_history_entry(project, "advisories acknowledged during generation")]
        updated_project["revision"] = int(project["revision"]) + 1
        updated_project["updated_at"] = utc_now()
        if not repository.save_project(updated_project, expected_revision=int(project["revision"]), expected_etag=project["etag"]):
            raise RuntimeError("revision_mismatch")
        project = updated_project

    plan_id = opaque_id("pln")
    etl_options = dict(options.get("etl", {}))
    etl_options.setdefault("batch_size", 2000)
    etl_options.setdefault("on_drift", "widen")
    etl_options.setdefault("on_orphan", "DLQ")
    etl_options.setdefault("max_error_rate", 0.01)
    etl_options.setdefault("max_widenings", int(project.get("thresholds", {}).get("max_widenings", 3)))
    unit_values: list[PlanUnit] = []
    artifacts_data: dict[str, tuple[str, str]] = {}
    table_specs: list[dict[str, Any]] = []
    naming: dict[str, str] = {}
    not_translated: list[dict[str, str]] = []
    warnings: list[dict[str, Any]] = []
    plan_options: dict[str, Any] = {"generation_options": options}
    if target_engine == "mongodb":
        relational = RelationalIR.model_validate(project["ir"])
        relationships = relational.relationships
        emitted = DocumentEmitter(options=options).emit(relational, relationships, project_id=project["project_id"])
        plan_options.update({"validators": emitted.validators, "indexes": emitted.indexes})
        not_translated.extend(emitted.not_translated)
        artifacts_data.update({
            "create_collections.js": ("validators", emitted.create_collections_js),
            "indexes.js": ("indexes", emitted.indexes_js),
            "validators.json": ("validators", emitted.validators_json),
            "migrate.py": ("etl", emitted.etl_script),
            "report.md": ("report", emitted.report),
        })
        embedded = {rel.child_table for rel in relationships if rel.effective_mapping.value in {"EMBED", "EMBED_ARRAY"}}
        junctions = {rel.child_table for rel in relationships if rel.rule_id == "H2" and rel.effective_mapping.value == "REFERENCE_ARRAY"}
        key_hints = options.get("etl", {}).get("key_hints", {})
        for table in relational.tables:
            if table.table_name in embedded or table.qualified_name in embedded or table.table_name in junctions or table.qualified_name in junctions:
                continue
            selected_keys = key_hints.get(table.table_name) or key_hints.get(table.qualified_name) or (table.primary_key.columns if table.primary_key else ())
            unit_values.append(PlanUnit(unit_id=table.table_name, root=table.qualified_name, key_columns=tuple(selected_keys), estimated_rows=table.stats.row_estimate))
        source_ir = relational.model_dump(mode="json")
    else:
        document = DocumentIR.model_validate(project["ir"])
        emitted = RelationalEmitter(dialect=target_engine, options={**options, "thresholds": project.get("thresholds", {})}).emit(document, document.decisions, project_id=project["project_id"])
        table_specs = emitted.tables
        naming = emitted.naming
        not_translated.extend(emitted.not_translated)
        warnings.extend(emitted.warnings)
        source_connection = repository.get_connection(str(project.get("source_connection_id") or "")) or {}
        source_namespace = source_connection.get("connection_fingerprint") or project.get("source_connection_id") or project["project_id"]
        plan_options.update({"pre_load_sql": emitted.pre_load_sql, "post_load_sql": emitted.post_load_sql,
                             "verification_sql": emitted.verification_sql, "apply_ddl": True,
                             "reverse_idmap_table": "_synapse_reverse_idmap",
                             "reverse_idmap_namespace": str(source_namespace)})
        artifacts_data.update({
            "01_tables.sql": ("ddl_pre", emitted.pre_load_sql),
            "02_constraints.sql": ("ddl_post", emitted.post_load_sql),
            "03_verify.sql": ("verify", emitted.verification_sql),
            "migrate.py": ("etl", "#!/usr/bin/env python3\n\"\"\"Reverse migration entry point.\"\"\"\nfrom synapse.cli import run_plan_file\n\nif __name__ == \"__main__\":\n    run_plan_file('plan.json')\n"),
            "report.md": ("report", _reverse_report(project["project_id"], emitted)),
        })
        for root in [table for table in table_specs if table.get("role") == "root"]:
            phase1 = [root["name"], *[table["name"] for table in table_specs if table.get("role") == "side" and table.get("parent_table") == root["name"]]]
            phase2 = [table["name"] for table in table_specs if table.get("collection") == root["collection"] and table.get("role") in {"child", "junction"}]
            source_collection = next((item for item in document.collections if item.name == root["collection"]), None)
            unit_values.append(PlanUnit(unit_id=root["collection"], root=root["name"], phase1=tuple(phase1), phase2=tuple(phase2), key_columns=("_id",),
                                        estimated_rows=source_collection.doc_count_estimate if source_collection else 0))
        source_ir = document.model_dump(mode="json")

    plan = ApprovedPlan.freeze(plan_id=plan_id, project_id=project["project_id"], ir_revision=int(project["revision"]),
                               direction=Direction(project["direction"]), target_engine=Engine(target_engine),
                               thresholds=Thresholds.from_mapping(project.get("thresholds")), naming=naming,
                               units=tuple(unit_values), etl_options=etl_options, options=plan_options,
                               source_ir=source_ir, tables=tuple(table_specs), not_translated=tuple(not_translated),
                               warnings=tuple(warnings), created_at=utc_now())
    artifacts: list[dict[str, Any]] = []
    for name, (kind, content) in artifacts_data.items():
        if target_engine == "mongodb":
            included = ((name in {"create_collections.js", "validators.json"} and "validators" in selected_targets)
                        or (name == "indexes.js" and "indexes" in selected_targets)
                        or (name == "migrate.py" and ({"etl_python", "etl"} & set(selected_targets)))
                        or (name == "report.md" and "report" in selected_targets))
        else:
            included = ((name.endswith(".sql") and ((kind.startswith("ddl") and "ddl" in selected_targets) or (kind == "verify" and "verify_sql" in selected_targets)))
                        or (name == "migrate.py" and ({"etl_python", "etl"} & set(selected_targets)))
                        or (name == "report.md" and "report" in selected_targets))
        if included:
            artifacts.append(repository.save_artifact(artifact_id=opaque_id("art"), plan_id=plan_id, name=name, kind=kind, content=content))
    wrapper = {"plan_id": plan_id, "plan": plan.as_json(), "plan_hash": plan.plan_hash, "project_id": project["project_id"],
               "target_engine": target_engine, "artifacts": artifacts, "applied": False, "created_at": plan.created_at,
               "source_connection_id": project.get("source_connection_id"), "applied_connection_id": None,
               "not_translated": not_translated, "warnings": warnings}
    repository.save_plan(wrapper)
    return wrapper, artifacts


def _reverse_report(project_id: str, emitted: Any) -> str:
    lines = ["# SynapseMigrate reverse generation report", "", f"Project: `{project_id}`", "", "## Tables", ""]
    lines.extend(f"- `{table['name']}` ({table['role']}) — {len(table.get('columns', []))} columns" for table in emitted.tables if table.get("role") != "system")
    lines.extend(["", "## Warnings", ""])
    if emitted.warnings:
        lines.extend(f"- **{item.get('code', 'WARNING')}** — {item.get('message', '')}" for item in emitted.warnings)
    else:
        lines.append("- None.")
    lines.extend(["", "## Not translated", ""])
    if emitted.not_translated:
        lines.extend(f"- `{item['source']}` — {item['reason']}" for item in emitted.not_translated)
    else:
        lines.append("- None identified.")
    return "\n".join(lines) + "\n"
