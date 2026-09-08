from __future__ import annotations

import pytest

from fareline.m2 import types


def test_widening_is_only_declared_for_lossless_promotions() -> None:
    assert types.classify(types.INT32, types.INT64) == types.WIDENING
    assert types.classify(types.INT32, types.FLOAT64) == types.WIDENING
    assert types.classify(types.FLOAT32, types.FLOAT64) == types.WIDENING
    assert types.classify(types.INT64, types.INT64) == types.IDENTITY


def test_integer_to_double_is_guarded_rather_than_assumed_lossless() -> None:
    # A 64-bit integer is exact in a double only below 2^53, so the promotion
    # exists but has to be checked per value.
    assert types.classify(types.INT64, types.FLOAT64) == types.GUARDED
    assert types.guard_bound(types.INT64, types.FLOAT64) == 2**53
    assert types.guard_bound(types.INT32, types.INT64) is None


def test_narrowing_is_refused_in_both_directions_it_could_appear() -> None:
    assert types.classify(types.INT64, types.INT32) == types.NARROWING
    assert types.classify(types.FLOAT64, types.FLOAT32) == types.NARROWING
    assert types.classify(types.FLOAT64, types.INT64) == types.NARROWING
    assert types.classify(types.TIMESTAMP_NTZ, types.DATE) == types.NARROWING


def test_unrelated_types_are_incompatible_not_narrowing() -> None:
    assert types.classify(types.INT32, types.STRING) == types.INCOMPATIBLE
    assert types.classify(types.STRING, types.FLOAT64) == types.INCOMPATIBLE
    assert types.classify(types.BOOLEAN, types.TIMESTAMP_NTZ) == types.INCOMPATIBLE


def test_an_untyped_source_column_is_its_own_case() -> None:
    assert types.classify(types.UNTYPED_NULL, types.FLOAT64) == types.UNTYPED_NULL_CAST
    assert types.classify(types.UNTYPED_NULL, types.STRING) == types.UNTYPED_NULL_CAST


def test_reader_type_names_map_onto_one_vocabulary() -> None:
    assert types.from_duckdb("BIGINT") == types.from_spark("bigint") == types.INT64
    assert types.from_duckdb("DOUBLE") == types.from_spark("double") == types.FLOAT64
    assert types.from_duckdb("TIMESTAMP") == types.from_spark("timestamp_ntz")
    # DuckDB quotes the untyped column name; Spark calls the same thing void.
    assert types.from_duckdb('"NULL"') == types.from_spark("void") == types.UNTYPED_NULL


def test_an_unmapped_reader_type_fails_loudly() -> None:
    with pytest.raises(types.UnknownSourceType):
        types.from_duckdb("STRUCT(a INTEGER)")
    with pytest.raises(types.UnknownSourceType):
        types.from_spark("array<int>")
    with pytest.raises(types.UnknownSourceType):
        types.spark_sql_name(types.UNTYPED_NULL)
