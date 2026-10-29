from __future__ import annotations

import datetime as dt
import decimal
import uuid

import pytest

from synapse.etl.keys import (
    decode_cursor,
    decode_tuple,
    encode_cursor,
    encode_tuple,
    keyset_predicate,
    keyset_select_sql,
)


def test_composite_key_codec_round_trips_supported_ordered_values() -> None:
    moment = dt.datetime(2026, 8, 1, 12, 30, tzinfo=dt.timezone.utc)
    key = [17, "contains\x00separator", b"\x00\xff", decimal.Decimal("-12.3400"), moment, uuid.UUID(int=42)]

    assert decode_tuple(encode_tuple(key)) == tuple(key)


def test_decimal_key_encoding_round_trips_and_preserves_numeric_order() -> None:
    values = [decimal.Decimal(value) for value in ("-1000", "-1.2", "-0.001", "0", "0.001", "1.2", "1000")]
    decoded = [decode_tuple(encode_tuple([value]))[0] for value in values]

    assert decoded == values
    assert sorted(values, key=lambda value: encode_tuple([value])) == values


def test_typed_cursor_round_trips_composite_decimal_and_datetime_key() -> None:
    values = [42, decimal.Decimal("123456789.0123"), dt.datetime(2026, 10, 21, tzinfo=dt.timezone.utc)]

    assert decode_cursor(encode_cursor(values)) == values


def test_composite_keyset_uses_ansi_row_value_or_expanded_disjunction() -> None:
    row_sql, row_params = keyset_predicate(["tenant_id", "id"], [9, 27], dialect="postgresql")
    expanded_sql, expanded_params = keyset_predicate(
        ["tenant_id", "id"], [9, 27], dialect="postgresql", row_value=False
    )

    assert row_sql == '("tenant_id", "id") > (:last_0, :last_1)'
    assert row_params == {"last_0": 9, "last_1": 27}
    assert expanded_sql == '(("tenant_id" > :last_0) OR ("tenant_id" = :last_0 AND "id" > :last_1))'
    assert expanded_params == row_params


def test_keyset_select_orders_composite_keys_and_binds_batch_size() -> None:
    sql, params = keyset_select_sql(
        "shop.line_items", ["tenant_id", "order_id"], 250,
        last_values=[3, 900], dialect="postgresql", row_value=False,
    )

    assert '"tenant_id" = :last_0 AND "order_id" > :last_1' in sql
    assert 'ORDER BY "tenant_id", "order_id" LIMIT :batch' in sql
    assert params == {"batch": 250, "last_0": 3, "last_1": 900}


def test_keyset_rejects_null_cursor_components() -> None:
    with pytest.raises(ValueError, match="cannot be NULL"):
        keyset_predicate(["tenant_id", "id"], [1, None])
