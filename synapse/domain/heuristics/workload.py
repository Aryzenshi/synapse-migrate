"""WA1–WA4 workload advisories; recommendations never silently change mappings."""
from __future__ import annotations

import hashlib

from synapse.domain.heuristics.thresholds import Thresholds
from synapse.domain.ir.common import Advisory, ForwardMapping, Severity, WorkloadProfile, WorkloadTableProfile
from synapse.domain.ir.relational import Relationship, RelationalIR


def _profile(profile: WorkloadProfile | None, table: str) -> WorkloadTableProfile | None:
    if profile is None:
        return None
    if table in profile.tables:
        return profile.tables[table]
    basename = table.split(".")[-1]
    return profile.tables.get(basename)


def _advisory(rule: str, severity: Severity, target: str, message: str, metrics: dict[str, float], recommendation: str) -> Advisory:
    identity = f"{rule}|{target}|" + "|".join(f"{k}={metrics[k]:.8g}" for k in sorted(metrics))
    aid = "adv_" + hashlib.sha1(identity.encode()).hexdigest()[:10]
    return Advisory(advisory_id=aid, rule_id=rule, severity=severity, target=target, message=message, metrics=metrics, recommendation=recommendation)


def _estimate_parent_bytes(parent, child, relation: Relationship) -> int:
    fanout = relation.fanout.p99 if relation.fanout else 0
    return max(0, parent.stats.avg_row_bytes + fanout * child.stats.avg_row_bytes)


def analyze_workload(ir: RelationalIR, relationships: list[Relationship] | None = None, th: Thresholds | None = None) -> list[Advisory]:
    thresholds = th or Thresholds()
    rels = relationships if relationships is not None else ir.relationships
    tables = {table.qualified_name: table for table in ir.tables}
    tables.update({table.table_name: table for table in ir.tables})
    advisories: list[Advisory] = []
    workload = ir.workload
    embedded = [rel for rel in rels if rel.effective_mapping in {ForwardMapping.EMBED, ForwardMapping.EMBED_ARRAY}]
    if workload is None:
        for rel in embedded:
            advisories.append(_advisory(
                "WORKLOAD_UNKNOWN", Severity.INFO, rel.rel_id,
                "No workload profile was supplied; static schema proxies were used only.", {},
                "Supply per-table read/write rates to evaluate contention, amplification, growth, and skew.",
            ))

    for rel in embedded:
        parent = tables.get(rel.parent_table)
        child = tables.get(rel.child_table)
        if parent is None or child is None:
            continue
        child_profile = _profile(workload, rel.child_table)
        parent_profile = _profile(workload, rel.parent_table)
        if child_profile is not None:
            writes = child_profile.inserts_per_sec + child_profile.updates_per_sec + child_profile.deletes_per_sec
            pure_read = writes <= 1e-12
            # A supplied hot-parent rate is the measured p95 rate. Without it, total writes
            # divided by parent count is the documented bounded static estimate.
            lambda_hot = child_profile.hot_writes_per_sec
            if lambda_hot is None:
                lambda_hot = writes / max(parent.stats.row_estimate, 1)
            latency_s = thresholds.target_write_latency_ms / 1000.0
            concurrent = lambda_hot * latency_s
            supplied_writers = child_profile.p95_concurrent_writers_per_parent
            if not pure_read:
                if (supplied_writers is not None and supplied_writers >= 3) or concurrent >= 0.5:
                    advisories.append(_advisory(
                        "WA1", Severity.HIGH, rel.rel_id,
                        f"Expected concurrent writers on a hot parent L={concurrent:.3g} (p95 writers={supplied_writers if supplied_writers is not None else 'unknown'}).",
                        {"lambda_hot_writes_per_sec": float(lambda_hot), "target_write_latency_ms": thresholds.target_write_latency_ms,
                         "expected_concurrent_writers": float(concurrent), "p95_concurrent_writers_per_parent": float(supplied_writers or 0)},
                        "Switch to REFERENCE or use bounded child buckets to avoid serialized parent-document writes.",
                    ))
                elif concurrent >= 0.25:
                    advisories.append(_advisory(
                        "WA1", Severity.MEDIUM, rel.rel_id,
                        f"Expected concurrent writers on a hot parent L={concurrent:.3g}.",
                        {"lambda_hot_writes_per_sec": float(lambda_hot), "target_write_latency_ms": thresholds.target_write_latency_ms,
                         "expected_concurrent_writers": float(concurrent)},
                        "Consider REFERENCE or a bucket pattern if write latency is material.",
                    ))
                parent_bytes = _estimate_parent_bytes(parent, child, rel)
                child_bytes = max(child.stats.avg_row_bytes, 1)
                amp = parent_bytes / child_bytes
                update_rate = child_profile.updates_per_sec
                if amp >= 200 and update_rate >= 10:
                    advisories.append(_advisory(
                        "WA2", Severity.HIGH, rel.rel_id,
                        f"Estimated write amplification is {amp:.1f}× at {update_rate:g} child updates/s.",
                        {"amplification": amp, "child_updates_per_sec": update_rate, "estimated_parent_doc_bytes": float(parent_bytes)},
                        "Use REFERENCE or split mutable fields from the parent aggregate.",
                    ))
                elif amp >= 50 and update_rate >= 10:
                    advisories.append(_advisory(
                        "WA2", Severity.MEDIUM, rel.rel_id,
                        f"Estimated write amplification is {amp:.1f}× at {update_rate:g} child updates/s.",
                        {"amplification": amp, "child_updates_per_sec": update_rate, "estimated_parent_doc_bytes": float(parent_bytes)},
                        "Consider REFERENCE or the subset pattern for frequently updated child data.",
                    ))

        fanout = rel.fanout
        if fanout is not None:
            growth = child.stats.growth_rows_per_day or 0.0
            projected = fanout.p99 + growth * thresholds.horizon_days
            projected_bytes = projected * max(child.stats.avg_row_bytes, 0)
            if projected > thresholds.n_hard or projected_bytes > 0.5 * 16 * 1024 * 1024:
                severity = Severity.HIGH if projected_bytes > 0.5 * 16 * 1024 * 1024 else Severity.MEDIUM
                advisories.append(_advisory(
                    "WA3", severity, rel.rel_id,
                    f"Projected max fan-out in {thresholds.horizon_days} days: {projected:.0f}.",
                    {"p99_now": float(fanout.p99), "growth_rows_per_day": float(growth), "projected_max": float(projected),
                     "projected_bytes": float(projected_bytes), "horizon_days": float(thresholds.horizon_days)},
                    "Keep REFERENCE; consider a bucket pattern with TTL or archival for append-only children.",
                ))
            skew = fanout.p99 / max(fanout.p50, 1)
            if skew >= 20 and fanout.p99 > thresholds.n_soft:
                advisories.append(_advisory(
                    "WA4", Severity.MEDIUM, rel.rel_id,
                    f"Fan-out is highly skewed (p99/p50={skew:.1f}).",
                    {"p99": float(fanout.p99), "p50": float(fanout.p50), "skew": float(skew)},
                    "Use REFERENCE or an outlier pattern that overflows exceptional parents into a side collection.",
                ))
            if rel.estimated_embedded_bytes > 16 * 1024 * 1024:
                advisories.append(_advisory(
                    "BSON_SIZE_RISK", Severity.HIGH, rel.rel_id,
                    f"Estimated embedded payload {rel.estimated_embedded_bytes} bytes exceeds the 16 MiB BSON document limit.",
                    {"estimated_bytes": float(rel.estimated_embedded_bytes), "limit_bytes": float(16 * 1024 * 1024)},
                    "Switch the relationship to REFERENCE before generation.",
                ))
    return sorted(advisories, key=lambda item: (item.target, item.rule_id, item.advisory_id))


propose_advisories = analyze_workload
