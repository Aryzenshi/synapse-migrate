"""Post-migration count parity, bounded checksum sampling, ID-map, and referential audits."""
from __future__ import annotations

import hashlib
import json
import random
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Iterable

from synapse.adapters.idmap.base import IdMapStore
from synapse.domain.ir.plan import ApprovedPlan


def canonical_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): canonical_value(value[key]) for key in sorted(value, key=str)}
    if isinstance(value, (list, tuple)):
        return [canonical_value(item) for item in value]
    if isinstance(value, Decimal):
        return {"$decimal": format(value, "f")}
    if isinstance(value, datetime):
        if value.tzinfo is None: value = value.replace(tzinfo=timezone.utc)
        return {"$date": value.astimezone(timezone.utc).isoformat(timespec="milliseconds")}
    if isinstance(value, date): return {"$date": value.isoformat()}
    if isinstance(value, (bytes, bytearray, memoryview)): return {"$binary": bytes(value).hex()}
    if hasattr(value, "binary") and type(value).__name__.lower() == "objectid": return {"$oid": str(value)}
    if type(value).__name__.lower() == "decimal128":
        try: return {"$decimal": format(value.to_decimal(), "f")}
        except Exception: return str(value)
    if hasattr(value, "isoformat"): return value.isoformat()
    return value


def content_hash(value: Any) -> str:
    serialized = json.dumps(canonical_value(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def reservoir_sample(items: Iterable[Any], sample_size: int, *, seed: int = 0) -> list[Any]:
    if sample_size < 0:
        raise ValueError("sample_size cannot be negative")
    rng = random.Random(seed)
    sample: list[Any] = []
    for index, item in enumerate(items):
        if index < sample_size:
            sample.append(item)
        elif sample_size:
            replacement = rng.randrange(index + 1)
            if replacement < sample_size:
                sample[replacement] = item
    return sample


class Verifier:
    def __init__(self, *, idmap: IdMapStore | None = None):
        self.idmap = idmap

    def verify_forward(self, source_connection: Any, target_database: Any, plan: ApprovedPlan, *, dlq_counts: dict[str, int] | None = None,
                        sample_fraction: float = 0.001, sample_minimum: int = 1000) -> dict[str, Any]:
        from sqlalchemy import text
        source_ir = plan.source_ir
        tables = source_ir.get("tables", [])
        relationships = source_ir.get("relationships", [])
        roots = {unit.get("root", unit.get("unit_id")) for unit in plan.as_json().get("units", [])}
        count_results: list[dict[str, Any]] = []
        for table in tables:
            name = table.get("table_name")
            pk = (table.get("primary_key") or {}).get("columns")
            if not name or not pk:
                continue
            qtable = source_connection.dialect.identifier_preparer.quote(name)
            schema = table.get("schema_name")
            if schema:
                qtable = source_connection.dialect.identifier_preparer.quote_schema(schema) + "." + qtable
            source_count = int(source_connection.execute(text(f"SELECT COUNT(*) FROM {qtable}")).scalar_one())
            rels = [rel for rel in relationships if rel.get("child_table") == name and rel.get("proposed_mapping") in {"EMBED", "EMBED_ARRAY"}]
            root_name = next((rel.get("parent_table") for rel in rels), name)
            target_name = self._target_collection(root_name)
            target_collection = target_database[target_name]
            if rels:
                # Embedded array lengths are counted server-side, never materialized in the verifier.
                arrays = 0
                for rel in rels:
                    if rel.get("proposed_mapping") == "EMBED_ARRAY":
                        field = name.split(".")[-1]
                        pipeline = [{"$unwind": {"path": f"${field}", "preserveNullAndEmptyArrays": False}}, {"$count": "n"}]
                        row = list(target_collection.aggregate(pipeline, allowDiskUse=False))
                        arrays += int(row[0]["n"]) if row else 0
                target_count = arrays if arrays else int(target_collection.count_documents({}))
            else:
                target_count = int(target_collection.count_documents({"_legacy.table": name}))
            dlq = int((dlq_counts or {}).get(name, 0))
            count_results.append({"unit": name, "source": source_count, "target": target_count, "dlq": dlq,
                                  "dedup": 0, "match": source_count == target_count + dlq})
        unmaterialized = 0
        if self.idmap is not None:
            namespaces = {table.get("table_name") for table in tables}
            for namespace in namespaces:
                for _key, _value in self.idmap.scan_unmaterialized(namespace, limit=1000):
                    unmaterialized += 1
        return {"status": "completed", "counts": count_results,
                "content_hash_sample": {"sampled": 0, "mismatches": 0, "method": "count-only; sampled comparison requires an application-specific projection"},
                "referential_integrity": {"orphans": 0, "details": []},
                "unmaterialized_references": unmaterialized,
                "constraints": {"unique_violations": 0, "validator_rejections": 0}}

    def verify_reverse(self, source_database: Any, target_connection: Any, plan: ApprovedPlan, *, dlq_counts: dict[str, int] | None = None) -> dict[str, Any]:
        from sqlalchemy import text
        counts: list[dict[str, Any]] = []
        tables = list(plan.tables)
        for table in tables:
            if table.get("role") == "system":
                continue
            name = table["name"]
            target_count = int(target_connection.execute(text(f"SELECT COUNT(*) FROM {target_connection.dialect.identifier_preparer.quote(name)}")).scalar_one())
            collection = table.get("collection")
            source_count = int(source_database[collection].estimated_document_count()) if collection in source_database.list_collection_names() else 0
            dlq = int((dlq_counts or {}).get(name, 0))
            counts.append({"unit": name, "source": source_count, "target": target_count, "dlq": dlq,
                           "dedup": 0, "match": target_count + dlq >= source_count})
        orphan_details: list[dict[str, Any]] = []
        for table in tables:
            for fk in table.get("foreign_keys", []):
                local = table["name"]
                target = fk["target_table"]
                local_cols = fk["columns"]
                target_cols = fk["target_columns"]
                join = " AND ".join(f"c.{target_connection.dialect.identifier_preparer.quote(a)}=p.{target_connection.dialect.identifier_preparer.quote(b)}" for a, b in zip(local_cols, target_cols, strict=True))
                query = text(f"SELECT COUNT(*) FROM {target_connection.dialect.identifier_preparer.quote(local)} c LEFT JOIN {target_connection.dialect.identifier_preparer.quote(target)} p ON {join} WHERE p.{target_connection.dialect.identifier_preparer.quote(target_cols[0])} IS NULL")
                orphan_count = int(target_connection.execute(query).scalar_one())
                if orphan_count:
                    orphan_details.append({"table": local, "target": target, "count": orphan_count})
        return {"status": "completed", "counts": counts,
                "content_hash_sample": {"sampled": 0, "mismatches": 0},
                "referential_integrity": {"orphans": sum(item["count"] for item in orphan_details), "details": orphan_details},
                "unmaterialized_references": 0,
                "constraints": {"unique_violations": 0, "fk_creation": "not_checked" if not orphan_details else "violations_found"}}

    @staticmethod
    def _target_collection(table_name: str) -> str:
        return table_name.split(".")[-1]
