"""Deterministic H1–H8 relational→document mapping rules."""
from __future__ import annotations

import hashlib
import math
import re
from dataclasses import dataclass
from typing import Iterable

from synapse.domain.heuristics.thresholds import Thresholds
from synapse.domain.ir.common import Cardinality, ForwardMapping
from synapse.domain.ir.relational import FanoutStats, ForeignKey, Relationship, RelationalIR, Table


@dataclass(frozen=True)
class FkEdge:
    child: Table
    parent: Table
    fk: ForeignKey
    fanout: FanoutStats

    @property
    def key(self) -> str:
        return self.fk.name or ",".join(self.fk.columns)

    @property
    def identity(self) -> tuple[str, str, tuple[str, ...], tuple[str, ...]]:
        return (self.child.qualified_name, self.parent.qualified_name, tuple(self.fk.columns), tuple(self.fk.ref_columns))


def _resolve_table(tables: Iterable[Table], name: str) -> Table | None:
    for table in tables:
        if table.qualified_name == name:
            return table
    matches = [table for table in tables if table.table_name == name.split(".")[-1]]
    return matches[0] if len(matches) == 1 else None


def _canonical_type_compatible(left: str, right: str) -> bool:
    aliases = {
        "tinyint": "int", "smallint": "int", "mediumint": "int", "integer": "int",
        "bigint": "long", "serial": "int", "bigserial": "long", "varchar": "string", "char": "string",
        "text": "string", "numeric": "decimal", "number": "decimal", "float": "double", "real": "double",
        "timestamp": "datetime", "timestamp with time zone": "datetime", "timestamp without time zone": "datetime",
    }
    def norm(value: str) -> str:
        value = re.sub(r"\(.*\)", "", value).strip().lower().replace(" unsigned", "")
        return aliases.get(value, value)
    return norm(left) == norm(right) or {norm(left), norm(right)} <= {"int", "long"}


def _singular(value: str) -> str:
    if value.endswith("ies") and len(value) > 3:
        return value[:-3] + "y"
    if value.endswith("s") and not value.endswith("ss"):
        return value[:-1]
    return value


def estimate_fanout(child: Table, parent: Table, th: Thresholds) -> FanoutStats:
    parent_rows = max(parent.stats.row_estimate, 1)
    avg = child.stats.row_estimate / parent_rows
    p50 = max(0, int(round(avg)))
    p95 = max(p50, int(math.ceil(avg * max(1.0, th.skew_factor / 2))))
    p99 = max(p95, int(math.ceil(avg * th.skew_factor)))
    maximum = max(p99, int(math.ceil(p99 * 1.5)))
    return FanoutStats(avg=avg, p50=p50, p95=p95, p99=p99, max=maximum, source="estimated")


def _infer_foreign_keys(ir: RelationalIR) -> list[tuple[Table, ForeignKey]]:
    existing = {(table.qualified_name, tuple(fk.columns)) for table in ir.tables for fk in table.foreign_keys}
    inferred: list[tuple[Table, ForeignKey]] = []
    for child in sorted(ir.tables, key=lambda item: item.qualified_name):
        for parent in sorted(ir.tables, key=lambda item: item.qualified_name):
            if parent is child or parent.primary_key is None:
                continue
            ref_cols = parent.primary_key.columns
            parent_names = {parent.table_name.lower(), _singular(parent.table_name.lower())}
            expected_sets: list[list[str]] = []
            if len(ref_cols) == 1:
                expected_sets.extend([[f"{p}_id"] for p in parent_names])
                expected_sets.extend([[f"{p}_{ref_cols[0].lower()}"] for p in parent_names])
            else:
                expected_sets.append([f"{_singular(parent.table_name.lower())}_{col.lower()}" for col in ref_cols])
                expected_sets.append([f"{parent.table_name.lower()}_{col.lower()}" for col in ref_cols])
            for candidate in expected_sets:
                if (child.qualified_name, tuple(candidate)) in existing:
                    continue
                by_name = {column.name.lower(): column for column in child.columns}
                actual = [by_name.get(name.lower()) for name in candidate]
                if any(column is None for column in actual):
                    continue
                ref_defs = [parent.column(name) for name in ref_cols]
                if all(_canonical_type_compatible(col.sql_type, ref.sql_type) for col, ref in zip(actual, ref_defs, strict=True)):
                    digest = hashlib.sha1((child.qualified_name + ":" + ",".join(candidate) + ":" + parent.qualified_name).encode()).hexdigest()[:8]
                    inferred.append((child, ForeignKey(name=f"inferred_{digest}", columns=candidate, ref_table=parent.qualified_name, ref_columns=ref_cols, inferred=True)))
                    existing.add((child.qualified_name, tuple(candidate)))
                    break
    return inferred


def build_fk_graph(ir: RelationalIR, infer: bool = True, thresholds: Thresholds | None = None) -> list[FkEdge]:
    th = thresholds or Thresholds()
    tables = ir.tables
    edges: list[FkEdge] = []
    fk_sources: list[tuple[Table, ForeignKey]] = [(table, fk) for table in tables for fk in table.foreign_keys]
    if infer:
        fk_sources.extend(_infer_foreign_keys(ir))
    for child, fk in fk_sources:
        parent = _resolve_table(tables, fk.ref_table)
        if parent is None:
            continue
        key = fk.name or ",".join(fk.columns)
        fanout = child.stats.fanout_by_fk.get(key) or estimate_fanout(child, parent, th)
        edges.append(FkEdge(child, parent, fk, fanout))
    edges.sort(key=lambda edge: (edge.child.qualified_name, edge.parent.qualified_name, edge.key, tuple(edge.fk.columns)))
    return edges


def _tarjan_components(vertices: set[str], edges: list[FkEdge]) -> list[set[str]]:
    adjacency: dict[str, list[str]] = {vertex: [] for vertex in vertices}
    for edge in edges:
        adjacency[edge.child.qualified_name].append(edge.parent.qualified_name)
    for targets in adjacency.values():
        targets.sort()
    index = 0
    indices: dict[str, int] = {}
    low: dict[str, int] = {}
    stack: list[str] = []
    on_stack: set[str] = set()
    result: list[set[str]] = []

    def visit(vertex: str) -> None:
        nonlocal index
        indices[vertex] = low[vertex] = index
        index += 1
        stack.append(vertex)
        on_stack.add(vertex)
        for target in adjacency[vertex]:
            if target not in indices:
                visit(target)
                low[vertex] = min(low[vertex], low[target])
            elif target in on_stack:
                low[vertex] = min(low[vertex], indices[target])
        if low[vertex] == indices[vertex]:
            component: set[str] = set()
            while True:
                member = stack.pop()
                on_stack.remove(member)
                component.add(member)
                if member == vertex:
                    break
            result.append(component)

    for vertex in sorted(vertices):
        if vertex not in indices:
            visit(vertex)
    return result


def choose_cycle_breakers(edges: list[FkEdge]) -> set[tuple[str, str, tuple[str, ...], tuple[str, ...]]]:
    """Repeatedly remove the highest-p99 edge in each cyclic SCC, deterministically."""
    vertices = {edge.child.qualified_name for edge in edges} | {edge.parent.qualified_name for edge in edges}
    remaining = list(edges)
    removed: set[tuple[str, str, tuple[str, ...], tuple[str, ...]]] = set()
    while True:
        cyclic: list[set[str]] = []
        for component in _tarjan_components(vertices, remaining):
            if len(component) > 1 or any(edge.child.qualified_name == edge.parent.qualified_name and edge.child.qualified_name in component for edge in remaining):
                cyclic.append(component)
        if not cyclic:
            break
        to_remove: list[FkEdge] = []
        for component in cyclic:
            internal = [edge for edge in remaining if edge.child.qualified_name in component and edge.parent.qualified_name in component]
            # max fanout; ties use ascending (child_table, fk_name)
            selected = sorted(internal, key=lambda edge: (-edge.fanout.p99, edge.child.qualified_name, edge.key))[0]
            to_remove.append(selected)
        for edge in to_remove:
            removed.add(edge.identity)
            remaining.remove(edge)
    return removed


def _is_junction(table: Table, edges: list[FkEdge]) -> tuple[bool, bool]:
    own = [edge for edge in edges if edge.child.qualified_name == table.qualified_name]
    if len(own) != 2 or table.primary_key is None:
        return False, False
    fk_columns = set(own[0].fk.columns) | set(own[1].fk.columns)
    pk_columns = set(table.primary_key.columns)
    if not pk_columns <= fk_columns:
        return False, False
    payload = set(column.name for column in table.columns) - fk_columns
    if payload:
        return False, True
    return True, False


def _cardinality(table: Table, fk: ForeignKey, pure_junction: bool) -> Cardinality:
    key = set(fk.columns)
    if table.primary_key and set(table.primary_key.columns) == key:
        return Cardinality.ONE_TO_ONE
    if any(set(unique.columns) == key for unique in table.uniques):
        return Cardinality.ONE_TO_ONE
    if pure_junction:
        return Cardinality.MANY_TO_MANY
    return Cardinality.ONE_TO_MANY


def _growth_unbounded(child: Table, th: Thresholds) -> bool:
    rate = child.stats.growth_rows_per_day
    has_time = any(
        column.auto_increment and column.canonical_type.value in {"date", "datetime"}
        or column.name.lower() in {"created_at", "updated_at", "created_on", "timestamp"}
        or "timestamp" in column.sql_type.lower() or column.canonical_type.value == "datetime"
        for column in child.columns
    )
    return bool(has_time and rate is not None and rate * th.horizon_days > th.n_hard)


def _relationship_id(edge: FkEdge) -> str:
    identity = "|".join((edge.child.qualified_name, edge.parent.qualified_name, ",".join(edge.fk.columns), edge.key))
    return "rel_" + hashlib.sha1(identity.encode()).hexdigest()[:12]


def _junction_array_edge(edges: list[FkEdge], table: Table) -> FkEdge | None:
    candidates = [edge for edge in edges if edge.child.qualified_name == table.qualified_name]
    if len(candidates) != 2:
        return None
    def degree_key(edge: FkEdge) -> tuple[float, int, str]:
        expected = table.stats.row_estimate / max(edge.parent.stats.row_estimate, 1)
        return expected, edge.parent.stats.row_estimate, edge.parent.qualified_name
    return sorted(candidates, key=degree_key)[0]


def _edge_depth(edge: FkEdge, relationships: list[Relationship]) -> int:
    parents_by_child: dict[str, list[str]] = {}
    for rel in relationships:
        if rel.effective_mapping in {ForwardMapping.EMBED, ForwardMapping.EMBED_ARRAY}:
            parents_by_child.setdefault(rel.parent_table, []).append(rel.child_table)
    depth = 0
    current = edge.parent.qualified_name
    visited = set()
    while current not in visited:
        visited.add(current)
        nexts = sorted(parents_by_child.get(current, []))
        if not nexts:
            break
        # The longest root-to-edge path is selected below by exploring all paths elsewhere.
        depth += 1
        current = nexts[0]
    return depth


def _transitive_estimate(root: str, rels: list[Relationship], table_map: dict[str, Table], seen: set[str] | None = None) -> int:
    seen = set() if seen is None else seen
    if root in seen:
        return 0
    seen.add(root)
    base = table_map[root].stats.avg_row_bytes
    for rel in rels:
        if rel.parent_table != root or rel.effective_mapping not in {ForwardMapping.EMBED, ForwardMapping.EMBED_ARRAY}:
            continue
        multiplicity = 1 if rel.effective_mapping == ForwardMapping.EMBED else (rel.fanout.p99 if rel.fanout else 0)
        base += multiplicity * _transitive_estimate(rel.child_table, rels, table_map, set(seen))
    return base


def _embedded_depths(root: str, rels: list[Relationship]) -> list[tuple[int, Relationship]]:
    result: list[tuple[int, Relationship]] = []
    def walk(node: str, depth: int, seen: set[str]) -> None:
        for rel in rels:
            if rel.parent_table == node and rel.effective_mapping in {ForwardMapping.EMBED, ForwardMapping.EMBED_ARRAY}:
                result.append((depth + 1, rel))
                if rel.child_table not in seen:
                    walk(rel.child_table, depth + 1, seen | {rel.child_table})
    walk(root, 0, {root})
    return result


def propose_forward(ir: RelationalIR, th: Thresholds | None = None) -> list[Relationship]:
    """Propose one deterministic mapping per complete FK group (including composite FKs)."""
    thresholds = th or Thresholds()
    edges = build_fk_graph(ir, infer=thresholds.infer_implicit_fks, thresholds=thresholds)
    cycle_breakers = choose_cycle_breakers(edges)
    table_map = {table.qualified_name: table for table in ir.tables}
    # A table can be addressed by an unqualified name only when that name is unique.
    names_count: dict[str, int] = {}
    for table in ir.tables:
        names_count[table.table_name] = names_count.get(table.table_name, 0) + 1
    pure_tables = {table.qualified_name for table in ir.tables if _is_junction(table, edges)[0]}
    payload_tables = {table.qualified_name for table in ir.tables if _is_junction(table, edges)[1]}
    junction_arrays = {table_name: _junction_array_edge(edges, table_map[table_name]) for table_name in pure_tables}
    rels: list[Relationship] = []

    def graph_in_degree(target: str, excluding_parent: str) -> int:
        return sum(1 for candidate in edges if candidate.parent.qualified_name == target and candidate.child.qualified_name != excluding_parent)

    for edge in edges:
        child, parent, fk, fanout = edge.child, edge.parent, edge.fk, edge.fanout
        cardinality = _cardinality(child, fk, child.qualified_name in pure_tables)
        embedded_bytes = (fanout.p99 if cardinality != Cardinality.ONE_TO_ONE else 1) * child.stats.avg_row_bytes
        if edge.identity in cycle_breakers:
            rule, mapping = "H8", ForwardMapping.REFERENCE
            rationale = f"cycle-breaking back-edge selected deterministically; p99 fan-out {fanout.p99}"
        elif child.qualified_name in pure_tables:
            chosen = junction_arrays[child.qualified_name]
            if chosen is not None and edge.identity == chosen.identity:
                rule, mapping = "H2", ForwardMapping.REFERENCE_ARRAY
                rationale = "pure two-FK junction; reference array placed on the lower expected-degree side"
            else:
                rule, mapping = "H2", ForwardMapping.REFERENCE
                rationale = "pure junction remains a reference edge; no row duplication"
        elif child.qualified_name in payload_tables:
            rule, mapping = "H2b", ForwardMapping.REFERENCE
            rationale = "junction has payload columns and is retained as an entity collection"
        elif cardinality == Cardinality.ONE_TO_ONE:
            if graph_in_degree(child.qualified_name, parent.qualified_name):
                rule, mapping = "H3", ForwardMapping.REFERENCE
                rationale = "1:1 child is referenced by other tables and is a shared entity"
            else:
                rule, mapping = "H1", ForwardMapping.EMBED
                rationale = "child primary/unique key equals the complete FK column set"
        elif graph_in_degree(child.qualified_name, parent.qualified_name):
            rule, mapping = "H3", ForwardMapping.REFERENCE
            rationale = "child is referenced by other tables; embedding would duplicate a shared entity"
        elif fanout.p99 > thresholds.n_hard or _growth_unbounded(child, thresholds):
            rule, mapping = "H5", ForwardMapping.REFERENCE
            if fanout.p99 > thresholds.n_hard:
                rationale = f"p99 fan-out {fanout.p99} > N_hard ({thresholds.n_hard})"
            else:
                projected = (child.stats.growth_rows_per_day or 0) * thresholds.horizon_days
                rationale = f"projected growth over {thresholds.horizon_days} days ({projected:g}) exceeds N_hard"
        elif embedded_bytes > thresholds.s_max_bytes:
            rule, mapping = "H6", ForwardMapping.REFERENCE
            rationale = f"estimated embedded bytes {embedded_bytes} > S_max ({thresholds.s_max_bytes})"
        elif fanout.p95 <= thresholds.n_soft and fanout.p99 <= thresholds.n_hard:
            rule, mapping = "H4", ForwardMapping.EMBED_ARRAY
            rationale = f"p95 fan-out {fanout.p95} <= N_soft {thresholds.n_soft}; p99 {fanout.p99} <= N_hard {thresholds.n_hard}"
        else:
            rule, mapping = "DEFAULT", ForwardMapping.REFERENCE
            rationale = "fan-out is not sufficiently bounded for safe embedding"

        # H7 is an inference provenance rule; the underlying H-rule remains explicit in rationale.
        if fk.inferred and rule not in {"H8", "H2", "H2b"}:
            rationale = f"H7 inferred FK (confirmation required); {rule}: {rationale}"
            rule = "H7"
        parent_name = parent.qualified_name if names_count[parent.table_name] > 1 else parent.table_name
        child_name = child.qualified_name if names_count[child.table_name] > 1 else child.table_name
        rels.append(Relationship(
            rel_id=_relationship_id(edge), parent_table=parent_name, child_table=child_name,
            foreign_key=list(fk.columns), referenced_columns=list(fk.ref_columns), cardinality=cardinality,
            proposed_mapping=mapping, rule_id=rule, rationale=rationale, inferred=fk.inferred,
            fk_name=fk.name, fanout=fanout, estimated_embedded_bytes=embedded_bytes,
        ))

    # A child can be embedded under only one parent; choose the parent with stronger affinity.
    by_child: dict[str, list[Relationship]] = {}
    for rel in rels:
        if rel.effective_mapping in {ForwardMapping.EMBED, ForwardMapping.EMBED_ARRAY}:
            by_child.setdefault(rel.child_table, []).append(rel)
    for child_name, candidates in by_child.items():
        if len(candidates) <= 1:
            continue
        winner = sorted(candidates, key=lambda rel: (rel.fanout.p99 if rel.fanout else 0, rel.parent_table))[0]
        for rel in candidates:
            if rel is winner:
                continue
            rel.proposed_mapping = ForwardMapping.REFERENCE
            rel.rule_id = "H3"
            rel.rationale = f"child has multiple candidate parents; embedded under {winner.parent_table} (lower p99 affinity), this edge references"

    # Enforce transitive root-document size. Flip deepest, largest embedded edge until safe.
    roots = sorted(set(table_map) - {rel.child_table for rel in rels if rel.effective_mapping in {ForwardMapping.EMBED, ForwardMapping.EMBED_ARRAY}})
    for root in roots:
        while _transitive_estimate(root, rels, table_map) > thresholds.s_max_bytes:
            candidates = _embedded_depths(root, rels)
            if not candidates:
                break
            deepest = max(depth for depth, _ in candidates)
            candidates = [(depth, rel) for depth, rel in candidates if depth == deepest]
            _, selected = sorted(candidates, key=lambda pair: (-(pair[1].estimated_embedded_bytes), pair[1].child_table, pair[1].parent_table))[0]
            selected.proposed_mapping = ForwardMapping.REFERENCE
            selected.rule_id = "H6"
            selected.rationale = f"transitive root document estimate exceeded S_max ({thresholds.s_max_bytes}); deepest high-byte edge switched to REFERENCE"

    rels.sort(key=lambda rel: (rel.parent_table, rel.child_table, rel.fk_name or "", rel.rel_id))
    return rels


def apply_forward_overrides(relationships: list[Relationship], overrides: dict[str, str | None]) -> list[Relationship]:
    """Purely apply already-validated user values to a copied relationship collection."""
    result: list[Relationship] = []
    for relation in relationships:
        value = overrides.get(relation.rel_id, relation.user_override)
        result.append(relation.model_copy(update={"user_override": value}))
    return result
