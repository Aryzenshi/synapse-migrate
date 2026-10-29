"""RH1–RH7 document→relational normalization decisions with Wilson confidence bounds."""
from __future__ import annotations

import hashlib
import math
from collections import Counter
from typing import Iterable

from synapse.domain.heuristics.thresholds import Thresholds
from synapse.domain.ir.document import Collection, DocumentIR, FieldDecision, FieldKind, FieldNode, ReverseAction, TypeObservation

Z_95 = 1.959963984540054


def wilson_interval(successes: int | float, n: int, z: float = Z_95) -> tuple[float, float]:
    """Two-sided Wilson score interval; well-defined for n=0 and boundary rates."""
    if n <= 0:
        return 0.0, 1.0
    p = min(1.0, max(0.0, float(successes) / n))
    z2 = z * z
    den = 1 + z2 / n
    center = (p + z2 / (2 * n)) / den
    margin = z * math.sqrt((p * (1 - p) / n) + (z2 / (4 * n * n))) / den
    return max(0.0, center - margin), min(1.0, center + margin)


def wilson_lower_rate(rate: float, n: int, z: float = Z_95) -> float:
    return wilson_interval(round(min(1.0, max(0.0, rate)) * n), n, z)[0]


def _non_null_types(types: Iterable[TypeObservation]) -> set[str]:
    return {item.bson_type for item in types if item.bson_type not in {"null", "undefined"} and item.count > 0}


def _numeric_family(types: set[str]) -> bool:
    return bool(types) and types <= {"int", "long", "double", "decimal", "number"}


def _scalar_type(value: str) -> bool:
    return value in {"string", "int", "long", "double", "decimal", "bool", "date", "objectId", "binData", "timestamp", "regex", "javascript", "symbol"}


def _shape_count(node: FieldNode) -> int:
    observed = _non_null_types(node.types)
    return len(observed)


def _flatten_depth(node: FieldNode) -> int:
    # `orders[].address` adds one scope at [] as well as one path segment.
    return node.path.count(".") + node.path.count("[]") + 1


def _leaf_count(node: FieldNode) -> int:
    if node.kind != FieldKind.OBJECT or not node.children:
        return 1 if node.kind == FieldKind.SCALAR else 0
    return sum(_leaf_count(child) for child in node.children)


def _estimated_bytes(node: FieldNode) -> int:
    if node.kind == FieldKind.OBJECT:
        return sum(_estimated_bytes(child) for child in node.children)
    if node.kind == FieldKind.ARRAY:
        return 0
    if node.string_stats:
        return max(node.string_stats.max_len, int(node.string_stats.p99_len * 1.5), 1)
    types = _non_null_types(node.types)
    if types & {"decimal"}:
        return 16
    if types & {"objectId", "date", "long", "double", "int", "binData"}:
        return 12 if "objectId" in types else 8
    if types & {"bool"}:
        return 1
    return 256


def _homogeneous_objects(profile, th: Thresholds) -> bool:
    if profile is None or profile.element_kind != FieldKind.OBJECT:
        return False
    observations = {item.bson_type: item.rate for item in profile.element_types if item.bson_type != "null"}
    object_rate = observations.get("object", 1.0 if not observations else 0.0)
    return object_rate >= th.homogeneous_type_rate


def _objectid_array(profile) -> bool:
    return bool(profile and profile.element_kind == FieldKind.SCALAR and any(item.bson_type == "objectId" and item.rate > 0 for item in profile.element_types))


def _scalar_array(profile) -> bool:
    return bool(profile and profile.element_kind == FieldKind.SCALAR and profile.element_types)


def _is_reference_scalar(node: FieldNode, th: Thresholds) -> bool:
    types = _non_null_types(node.types)
    return node.kind == FieldKind.SCALAR and types == {"objectId"} and node.ref_target is not None and node.ref_target.match_rate >= th.ref_match


def _make_decision(collection: str, node: str, rule: str, action: ReverseAction, alternatives: list[ReverseAction], evidence: dict, advisory_ids: list[str] | None = None) -> FieldDecision:
    decision_id = "dec_" + hashlib.sha1(f"{collection}|{node}|{rule}".encode()).hexdigest()[:12]
    return FieldDecision(decision_id=decision_id, collection=collection, path=node, rule_id=rule, proposed_action=action, alternatives=alternatives, evidence=evidence, advisory_ids=advisory_ids or [])


def decide_root_id(collection: Collection, th: Thresholds | None = None) -> FieldDecision:
    types = {item.bson_type: item.rate for item in collection.id_profile.types}
    dominant = sorted(types.items(), key=lambda pair: (-pair[1], pair[0]))[0][0] if types else "unknown"
    return _make_decision(
        collection.name, "_id", "RH6", ReverseAction.SURROGATE_PK,
        [],
        {"id_types": types, "dominant_type": dominant, "default": "relational surrogate BIGINT; MongoDB _id stays in internal migration metadata"},
    )


def _decide_node(node: FieldNode, collection: str, th: Thresholds) -> FieldDecision:
    observed = _non_null_types(node.types)
    evidence = {
        "kind": node.kind.value,
        "types": {item.bson_type: item.rate for item in sorted(node.types, key=lambda item: item.bson_type)},
        "presence_rate": node.presence_rate,
        "null_rate": node.null_rate,
        "absent_rate": node.absent_rate,
        "empty_rate": node.empty_rate,
        "presence_ci": list(node.presence_ci),
        "n": node.sample_size,
    }

    if node.is_polymorphic or node.kind == FieldKind.MIXED:
        if observed and all(_scalar_type(item) for item in observed) and not _numeric_family(observed) and len(observed) <= 4:
            return _make_decision(collection, node.path, "RH4", ReverseAction.DEMOTE_TO_VARCHAR,
                                  [ReverseAction.RETAIN_AS_JSON], evidence | {"reason": "heterogeneous scalar values"})
        if _numeric_family(observed):
            # Numeric-compatible values widen under the type map rather than being serialized as text.
            return _make_decision(collection, node.path, "DEFAULT", ReverseAction.PLAIN_COLUMN,
                                  [ReverseAction.RETAIN_AS_JSON], evidence | {"reason": "numeric family widens"})
        return _make_decision(collection, node.path, "RH4", ReverseAction.RETAIN_AS_JSON,
                              [ReverseAction.DEMOTE_TO_VARCHAR], evidence | {"reason": "mixed container or multiple shapes"})

    if node.kind == FieldKind.ARRAY:
        profile = node.array
        if profile is None:
            return _make_decision(collection, node.path, "RH4", ReverseAction.RETAIN_AS_JSON,
                                  [ReverseAction.UNNEST_TO_TABLE], evidence | {"reason": "array profile unavailable"})
        evidence.update({
            "len_min": profile.len_min, "len_p50": profile.len_p50, "len_p95": profile.len_p95,
            "len_max_observed": profile.len_max_observed, "empty_rate": profile.empty_rate,
            "element_kind": profile.element_kind.value,
        })
        if profile.element_kind == FieldKind.OBJECT and _homogeneous_objects(profile, th):
            return _make_decision(collection, node.path, "RH2", ReverseAction.UNNEST_TO_TABLE,
                                  [ReverseAction.RETAIN_AS_JSON], evidence | {"homogeneous_rate": th.homogeneous_type_rate})
        if profile.element_kind == FieldKind.ARRAY or node.kind == FieldKind.MIXED:
            return _make_decision(collection, node.path, "RH4", ReverseAction.RETAIN_AS_JSON,
                                  [ReverseAction.UNNEST_TO_TABLE], evidence | {"reason": "nested or mixed arrays"})
        if _objectid_array(profile):
            ref = profile.element_ref_target
            if ref is not None:
                evidence["ref_target"] = ref.collection
                evidence["match_rate"] = ref.match_rate
                if ref.match_rate >= th.ref_match:
                    return _make_decision(collection, node.path, "RH3", ReverseAction.EXTRACT_JUNCTION,
                                          [ReverseAction.UNNEST_TO_TABLE, ReverseAction.RETAIN_AS_JSON], evidence)
            advisory = "adv_" + hashlib.sha1(f"UNRESOLVED_REFERENCE|{collection}|{node.path}".encode()).hexdigest()[:10]
            return _make_decision(collection, node.path, "RH2", ReverseAction.UNNEST_TO_TABLE,
                                  [ReverseAction.RETAIN_AS_JSON], evidence | {"reason": "unresolved ObjectId array"}, [advisory])
        if _scalar_array(profile):
            types = {item.bson_type for item in profile.element_types if item.bson_type not in {"null", "undefined"} and item.rate > 0}
            if len(types) <= 1 or _numeric_family(types):
                return _make_decision(collection, node.path, "RH2", ReverseAction.UNNEST_TO_TABLE,
                                      [ReverseAction.RETAIN_AS_JSON], evidence | {"value_table": True})
            return _make_decision(collection, node.path, "RH4", ReverseAction.RETAIN_AS_JSON,
                                  [ReverseAction.UNNEST_TO_TABLE], evidence | {"reason": "heterogeneous scalar array"})
        return _make_decision(collection, node.path, "RH4", ReverseAction.RETAIN_AS_JSON,
                              [ReverseAction.UNNEST_TO_TABLE], evidence | {"reason": "unsupported array element shape"})

    if node.kind == FieldKind.OBJECT:
        depth = _flatten_depth(node)
        estimated_columns = _leaf_count(node)
        estimated_bytes = sum(_estimated_bytes(child) for child in node.children)
        evidence.update({"depth": depth, "estimated_flat_columns": estimated_columns, "estimated_flat_bytes": estimated_bytes})
        if depth > th.max_flatten_depth or node.max_depth_observed > th.max_depth:
            return _make_decision(collection, node.path, "RH4", ReverseAction.RETAIN_AS_JSON,
                                  [ReverseAction.SIDE_TABLE_OPTIONAL], evidence | {"reason": "depth guard"})
        n = node.sample_size
        empty_lower = wilson_lower_rate(node.empty_rate, n)
        evidence["empty_rate_lower_95"] = empty_lower
        if empty_lower > th.sparse_object:
            return _make_decision(collection, node.path, "RH5", ReverseAction.SIDE_TABLE_OPTIONAL,
                                  [ReverseAction.FLATTEN_COLUMNS, ReverseAction.RETAIN_AS_JSON], evidence | {"threshold": th.sparse_object})
        if estimated_columns > th.max_flatten_cols or estimated_bytes > th.flatten_row_limit_bytes:
            return _make_decision(collection, node.path, "RH5", ReverseAction.SIDE_TABLE_OPTIONAL,
                                  [ReverseAction.FLATTEN_COLUMNS, ReverseAction.RETAIN_AS_JSON], evidence | {"reason": "MySQL in-page width guard", "limit_bytes": th.flatten_row_limit_bytes})
        return _make_decision(collection, node.path, "RH1", ReverseAction.FLATTEN_COLUMNS,
                              [ReverseAction.RETAIN_AS_JSON, ReverseAction.SIDE_TABLE_OPTIONAL], evidence)

    if _is_reference_scalar(node, th):
        evidence.update({"ref_collection": node.ref_target.collection, "match_rate": node.ref_target.match_rate})
        return _make_decision(collection, node.path, "RH7", ReverseAction.FK_COLUMN,
                              [ReverseAction.PLAIN_COLUMN, ReverseAction.RETAIN_AS_JSON], evidence)

    return _make_decision(collection, node.path, "DEFAULT", ReverseAction.PLAIN_COLUMN,
                          [ReverseAction.RETAIN_AS_JSON], evidence)


def propose_reverse(ir: DocumentIR, th: Thresholds | None = None) -> list[FieldDecision]:
    thresholds = th or Thresholds()
    decisions: list[FieldDecision] = []
    for collection in sorted(ir.collections, key=lambda item: item.name):
        decisions.append(decide_root_id(collection, thresholds))
        nodes = [node for root in collection.fields for node in root.walk()]
        for node in sorted(nodes, key=lambda item: item.path):
            decisions.append(_decide_node(node, collection.name, thresholds))
    return decisions


def allowed_reverse_overrides(node: FieldNode | None, current: ReverseAction) -> set[ReverseAction]:
    if current == ReverseAction.SURROGATE_PK:
        return {ReverseAction.SURROGATE_PK}
    if node is None:
        return set()
    if node.kind == FieldKind.OBJECT:
        return {
            ReverseAction.FLATTEN_COLUMNS, ReverseAction.RETAIN_AS_JSON,
            ReverseAction.SIDE_TABLE_OPTIONAL,
        }
    if node.kind == FieldKind.ARRAY:
        if node.array and node.array.element_ref_target:
            return {ReverseAction.EXTRACT_JUNCTION, ReverseAction.UNNEST_TO_TABLE, ReverseAction.RETAIN_AS_JSON}
        return {ReverseAction.UNNEST_TO_TABLE, ReverseAction.RETAIN_AS_JSON}
    if node.is_polymorphic or node.kind == FieldKind.MIXED:
        return {ReverseAction.DEMOTE_TO_VARCHAR, ReverseAction.RETAIN_AS_JSON}
    if node.ref_target:
        return {ReverseAction.FK_COLUMN, ReverseAction.PLAIN_COLUMN, ReverseAction.RETAIN_AS_JSON}
    return {ReverseAction.PLAIN_COLUMN, ReverseAction.DEMOTE_TO_VARCHAR, ReverseAction.RETAIN_AS_JSON}


def apply_reverse_overrides(decisions: list[FieldDecision], overrides: dict[str, str | None]) -> list[FieldDecision]:
    return [decision.model_copy(update={"user_override": overrides.get(decision.decision_id, decision.user_override)}) for decision in decisions]
