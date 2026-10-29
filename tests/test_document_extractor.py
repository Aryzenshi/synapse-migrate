"""Regression tests for resolving PyMongo database handles safely."""
from __future__ import annotations

from decimal import Decimal

from synapse.adapters.extractors.document import _FieldStats, _database_handle
from synapse.domain.ir.document import FieldKind, FieldNode, NumericStats, TypeObservation
from synapse.domain.typemap.bson_to_sql import bson_to_sql


class _FakeDatabase:
    def list_collection_names(self):
        return []

    def __getattr__(self, name: str):
        # PyMongo Database dynamically maps unknown attributes to collections.
        return _FakeCollection(self)


class _FakeCollection:
    def __init__(self, database: _FakeDatabase):
        self.database = database

    def __getattr__(self, name: str):
        # PyMongo Collection also exposes dynamic, non-callable attributes.
        return object()


def test_database_handle_prefers_database_method_over_dynamic_database_attribute() -> None:
    database = _FakeDatabase()

    assert _database_handle(database) is database


def test_database_handle_resolves_collection_to_its_parent_database() -> None:
    database = _FakeDatabase()
    collection = _FakeCollection(database)

    assert _database_handle(collection) is database


def test_nested_object_depth_is_recorded_without_iterating_the_integer_result() -> None:
    stats = _FieldStats("profile")

    stats.observe({"address": {"city": "Chennai"}})

    assert stats.max_depth == 2


def test_decimal_precision_covers_largest_integer_part_and_scale() -> None:
    stats = _FieldStats("total")
    stats.observe(Decimal("998"))
    stats.observe(Decimal("49.5"))

    assert stats.decimal_precision == 4
    assert stats.decimal_scale == 1
    node = FieldNode(
        path="total", name="total", kind=FieldKind.SCALAR,
        types=[TypeObservation(bson_type="decimal", count=2, rate=1.0)],
        sample_size=2, presence_rate=1.0, null_rate=0.0, absent_rate=0.0,
        numeric_stats=NumericStats(minimum=49.5, maximum=998, precision=stats.decimal_precision, scale=stats.decimal_scale),
    )
    assert bson_to_sql(node, dialect="postgresql").sql_type == "NUMERIC(4,1)"


def test_decimal_scale_zero_is_not_replaced_by_a_default_scale() -> None:
    node = FieldNode(
        path="count", name="count", kind=FieldKind.SCALAR,
        types=[TypeObservation(bson_type="decimal", count=1, rate=1.0)],
        sample_size=1, presence_rate=1.0, null_rate=0.0, absent_rate=0.0,
        numeric_stats=NumericStats(minimum=998, maximum=998, precision=3, scale=0),
    )
    assert bson_to_sql(node, dialect="postgresql").sql_type == "NUMERIC(3,0)"
