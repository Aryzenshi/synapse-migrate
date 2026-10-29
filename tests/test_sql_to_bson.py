from __future__ import annotations

import datetime as dt
import decimal

from bson.decimal128 import Decimal128

from synapse.domain.ir.relational import Column
from synapse.domain.typemap.sql_to_bson import to_bson_value


def test_sqlite_datetime_text_is_converted_to_bson_datetime() -> None:
    column = Column(name="created_at", sql_type="DATETIME", canonical_type="datetime")
    value = to_bson_value("2026-08-01T10:00:00", column)
    assert isinstance(value, dt.datetime)
    assert value == dt.datetime(2026, 8, 1, 10, 0, tzinfo=dt.timezone.utc)


def test_sqlite_json_text_is_parsed_to_document_value() -> None:
    column = Column(name="metadata", sql_type="JSON", canonical_type="json")
    assert to_bson_value('{"channel":"mobile"}', column) == {"channel": "mobile"}


def test_sqlite_integer_and_float_decimals_are_converted_to_decimal128() -> None:
    column = Column(name="total", sql_type="DECIMAL(18,2)", canonical_type="decimal")
    integer_value = to_bson_value(998, column)
    float_value = to_bson_value(49.5, column)
    assert isinstance(integer_value, Decimal128)
    assert isinstance(float_value, Decimal128)
    assert integer_value.to_decimal() == decimal.Decimal("998")
    assert float_value.to_decimal() == decimal.Decimal("49.5")
