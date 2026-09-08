"""Contract resolution against the real schemas M0 recorded, plus the cases
upstream has not produced but the contract still has to refuse.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from fareline.m2 import contracts, resolution, types

M0_EVIDENCE = Path(__file__).resolve().parents[1] / "evidence" / "m0" / "source_inventory.json"


def m0_schemas() -> dict[str, list[dict[str, Any]]]:
    return json.loads(M0_EVIDENCE.read_text(encoding="utf-8"))["schemas"]


def resolve_recorded(key: str) -> resolution.SchemaResolution:
    schema = m0_schemas()[key]
    service = key.split("_", 1)[0]
    contract = contracts.SERVICE_CONTRACTS[service]
    return resolution.resolve(contract, resolution.source_columns_from_duckdb(schema))


def column(name: str, type_token: str, ordinal: int = 0) -> resolution.SourceColumn:
    return resolution.SourceColumn(name, type_token, ordinal)


def yellow_source(**overrides: str) -> tuple[resolution.SourceColumn, ...]:
    """A minimal Yellow source that satisfies the contract, before overrides."""
    base = {
        "tpep_pickup_datetime": types.TIMESTAMP_NTZ,
        "tpep_dropoff_datetime": types.TIMESTAMP_NTZ,
        "PULocationID": types.INT64,
        "DOLocationID": types.INT64,
        "fare_amount": types.FLOAT64,
        "total_amount": types.FLOAT64,
    }
    base.update(overrides)
    return tuple(column(name, token, index) for index, (name, token) in enumerate(base.items()))


@pytest.mark.parametrize("key", sorted(m0_schemas()))
def test_every_recorded_upstream_schema_is_accepted(key: str) -> None:
    resolved = resolve_recorded(key)

    assert resolved.accepted, [item.as_document() for item in resolved.violations]
    # The contracts cover the whole published surface, so nothing real is
    # silently dropped on the way into the contracted tables.
    assert resolved.unmapped_source_columns == ()


def test_the_airport_fee_rename_resolves_to_one_canonical_column() -> None:
    before = resolve_recorded("yellow_2023_01").field("airport_fee")
    after = resolve_recorded("yellow_2023_07").field("airport_fee")

    assert before.source_name == "airport_fee"
    assert after.source_name == "Airport_fee"
    assert before.status == after.status == resolution.MAPPED
    assert before.promotion == after.promotion == types.IDENTITY


def test_a_file_carrying_both_spellings_is_ambiguous_not_resolved() -> None:
    source = yellow_source(airport_fee=types.FLOAT64, Airport_fee=types.FLOAT64)

    resolved = resolution.resolve(contracts.YELLOW, source)

    assert not resolved.accepted
    assert [item.rule for item in resolved.violations] == [resolution.AMBIGUOUS_CANONICAL_COLUMN]
    assert "Airport_fee" in resolved.violations[0].detail
    assert "airport_fee" in resolved.violations[0].detail


def test_integer_width_growth_is_accepted_as_widening() -> None:
    resolved = resolve_recorded("yellow_2024_01")

    for name in ("pickup_location_id", "dropoff_location_id", "vendor_id"):
        field = resolved.field(name)
        assert field.source_type == types.INT32
        assert field.promotion == types.WIDENING


def test_the_double_to_integer_change_stays_a_range_checked_promotion() -> None:
    older = resolve_recorded("yellow_2023_01")
    newer = resolve_recorded("yellow_2024_01")

    assert older.field("passenger_count").promotion == types.IDENTITY
    assert newer.field("passenger_count").promotion == types.GUARDED
    assert newer.field("passenger_count").guard_bound == 2**53
    assert {item.canonical_name for item in newer.guarded_fields} == {
        "passenger_count",
        "ratecode_id",
    }


def test_a_physically_untyped_column_is_cast_without_inventing_a_type() -> None:
    resolved = resolve_recorded("hvfhv_2019_02")

    for name in ("airport_fee", "wav_match_flag"):
        field = resolved.field(name)
        assert field.status == resolution.UNTYPED_NULL_SOURCE
        assert field.source_type == types.UNTYPED_NULL
        assert field.promotion == types.UNTYPED_NULL_CAST
    # The contract type is applied to typed nulls; the source type stays unknown.
    assert resolved.field("airport_fee").target_type == types.FLOAT64
    assert resolved.field("wav_match_flag").target_type == types.STRING
    assert resolved.accepted


def test_a_column_that_does_not_exist_yet_is_a_typed_null() -> None:
    older = resolve_recorded("hvfhv_2024_01").field("cbd_congestion_fee")
    newer = resolve_recorded("hvfhv_2025_01").field("cbd_congestion_fee")

    assert older.status == resolution.ABSENT_IN_SOURCE
    assert older.source_name is None
    assert newer.status == resolution.MAPPED


def test_a_missing_required_column_blocks_the_version() -> None:
    source = tuple(item for item in yellow_source() if item.name != "PULocationID")

    resolved = resolution.resolve(contracts.YELLOW, source)

    assert not resolved.accepted
    assert [item.rule for item in resolved.violations] == [resolution.MISSING_REQUIRED_COLUMN]
    assert resolved.violations[0].canonical_name == "pickup_location_id"


def test_narrowing_is_reported_instead_of_being_truncated() -> None:
    source = yellow_source(fare_amount=types.STRING, total_amount=types.FLOAT64)
    widened = yellow_source(trip_distance=types.FLOAT64, payment_type=types.FLOAT64)

    assert resolution.resolve(contracts.YELLOW, source).violations[0].rule == (
        resolution.INCOMPATIBLE_TYPE_DRIFT
    )
    narrowing = resolution.resolve(contracts.YELLOW, widened)
    assert [item.rule for item in narrowing.violations] == [resolution.NARROWING_TYPE_DRIFT]
    assert narrowing.violations[0].canonical_name == "payment_type"


def test_a_guarded_promotion_needs_the_contract_to_allow_it() -> None:
    # trip_time is a bigint measure that the contract does not opt into guarding.
    source = tuple(
        column(name, token, index)
        for index, (name, token) in enumerate(
            {
                "pickup_datetime": types.TIMESTAMP_NTZ,
                "dropoff_datetime": types.TIMESTAMP_NTZ,
                "PULocationID": types.INT64,
                "DOLocationID": types.INT64,
                "base_passenger_fare": types.FLOAT64,
                "driver_pay": types.FLOAT64,
                "trip_miles": types.INT64,
            }.items()
        )
    )

    resolved = resolution.resolve(contracts.HVFHV, source)

    assert [item.rule for item in resolved.violations] == [resolution.GUARDED_PROMOTION_NOT_ALLOWED]
    assert resolved.violations[0].canonical_name == "trip_miles"


def test_every_violation_is_reported_rather_than_only_the_first() -> None:
    source = tuple(item for item in yellow_source() if item.name not in {"PULocationID"})
    source = (*source, column("fare_amount_extra", types.STRING, 99))

    resolved = resolution.resolve(
        contracts.YELLOW,
        tuple(item for item in source if item.name != "total_amount"),
    )

    rules = sorted(item.canonical_name for item in resolved.violations)
    assert rules == ["pickup_location_id", "total_amount"]
    assert resolved.unmapped_source_columns == ("fare_amount_extra",)


def test_a_resolution_fingerprint_changes_only_with_the_resolution() -> None:
    first = resolve_recorded("yellow_2024_01")
    same = resolve_recorded("yellow_2024_01")
    other = resolve_recorded("yellow_2023_01")

    assert first.fingerprint == same.fingerprint
    assert first.fingerprint != other.fingerprint
