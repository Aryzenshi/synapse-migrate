from __future__ import annotations

from synapse.domain.heuristics.forward import propose_forward
from synapse.domain.heuristics.reverse import propose_reverse, wilson_interval
from synapse.domain.heuristics.thresholds import Thresholds
from synapse.domain.ir.common import Cardinality, ForwardMapping
from synapse.domain.ir.document import (
    Collection,
    DocumentIR,
    FieldKind,
    FieldNode,
    IdProfile,
    ReverseAction,
    TypeObservation,
)
from synapse.domain.ir.relational import (
    Column,
    ForeignKey,
    FanoutStats,
    PrimaryKey,
    RelationalIR,
    Table,
    TableStats,
)


def test_forward_heuristic_keeps_composite_foreign_keys_ordered() -> None:
    parent = Table(
        table_name="orders",
        columns=[Column(name="tenant_id", sql_type="INT"), Column(name="id", sql_type="INT")],
        primary_key=PrimaryKey(columns=["tenant_id", "id"]),
        stats=TableStats(row_estimate=100, avg_row_bytes=96),
    )
    fk = ForeignKey(
        name="fk_line_items_orders",
        columns=["tenant_id", "order_id"],
        ref_table="orders",
        ref_columns=["tenant_id", "id"],
    )
    child = Table(
        table_name="line_items",
        columns=[
            Column(name="tenant_id", sql_type="INT"),
            Column(name="order_id", sql_type="INT"),
            Column(name="line_no", sql_type="INT"),
        ],
        primary_key=PrimaryKey(columns=["tenant_id", "order_id", "line_no"]),
        foreign_keys=[fk],
        stats=TableStats(
            row_estimate=200,
            avg_row_bytes=120,
            fanout_by_fk={
                "fk_line_items_orders": FanoutStats(
                    avg=2.0, p50=2, p95=3, p99=5, max=8, source="exact"
                )
            },
        ),
    )

    relationships = propose_forward(
        RelationalIR(tables=[parent, child]),
        Thresholds(infer_implicit_fks=False),
    )

    assert len(relationships) == 1
    relation = relationships[0]
    assert relation.foreign_key == ["tenant_id", "order_id"]
    assert relation.referenced_columns == ["tenant_id", "id"]
    assert relation.cardinality == Cardinality.ONE_TO_MANY
    assert relation.rule_id == "H4"
    assert relation.proposed_mapping == ForwardMapping.EMBED_ARRAY


def test_wilson_interval_handles_empty_and_boundary_samples() -> None:
    assert wilson_interval(0, 0) == (0.0, 1.0)
    lower, upper = wilson_interval(100, 100)
    assert 0.96 < lower < 1.0
    assert upper == 1.0
    lower_zero, upper_zero = wilson_interval(0, 100)
    assert lower_zero < 1e-12
    assert 0.0 < upper_zero < 0.05


def test_reverse_heuristic_proposes_surrogate_id_and_flattens_stable_object() -> None:
    city = FieldNode(
        path="address.city",
        name="city",
        kind=FieldKind.SCALAR,
        types=[TypeObservation(bson_type="string", count=100, rate=1.0)],
        sample_size=100,
        presence_rate=1.0,
        null_rate=0.0,
        absent_rate=0.0,
    )
    address = FieldNode(
        path="address",
        name="address",
        kind=FieldKind.OBJECT,
        types=[TypeObservation(bson_type="object", count=100, rate=1.0)],
        sample_size=100,
        presence_rate=1.0,
        null_rate=0.0,
        absent_rate=0.0,
        children=[city],
    )
    ir = DocumentIR(
        collections=[
            Collection(
                name="customers",
                id_profile=IdProfile(
                    types=[TypeObservation(bson_type="objectId", count=100, rate=1.0)], sample_size=100
                ),
                fields=[address],
            )
        ]
    )

    decisions = {decision.path: decision for decision in propose_reverse(ir)}

    assert decisions["_id"].proposed_action == ReverseAction.SURROGATE_PK
    assert decisions["address"].rule_id == "RH1"
    assert decisions["address"].proposed_action == ReverseAction.FLATTEN_COLUMNS
    assert decisions["address.city"].proposed_action == ReverseAction.PLAIN_COLUMN
