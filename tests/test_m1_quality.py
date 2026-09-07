from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from fareline.m1.quality import (
    SERVICE_SPECS,
    compare_alignment,
    compare_metrics,
    duckdb_alignment,
    duckdb_metrics,
    metric_names,
    metrics_sql,
    period_parts,
)


def write_yellow_fixture(path: Path) -> Path:
    """Five rows with one planted incident of each kind the M1 rules cover."""
    rows = [
        # in period, complete
        (
            "TIMESTAMP '2024-01-05 10:00:00'",
            "TIMESTAMP '2024-01-05 10:12:00'",
            "132",
            "48",
            "10.50",
            "2.00",
            "14.50",
        ),
        # pickup outside the declared period
        (
            "TIMESTAMP '2023-12-31 23:55:00'",
            "TIMESTAMP '2024-01-01 00:10:00'",
            "1",
            "2",
            "20.25",
            "0.00",
            "23.75",
        ),
        # dropoff before pickup
        (
            "TIMESTAMP '2024-01-06 09:00:00'",
            "TIMESTAMP '2024-01-06 08:30:00'",
            "3",
            "4",
            "5.00",
            "1.25",
            "8.25",
        ),
        # null zone keys
        (
            "TIMESTAMP '2024-01-07 12:00:00'",
            "TIMESTAMP '2024-01-07 12:20:00'",
            "NULL",
            "NULL",
            "7.75",
            "0.50",
            "10.75",
        ),
        # missing fare components stay null instead of becoming zero
        ("TIMESTAMP '2024-01-08 08:00:00'", "NULL", "10", "11", "NULL", "NULL", "NULL"),
    ]
    values = ", ".join(
        f"({pickup}, {dropoff}, CAST({pu} AS INTEGER), CAST({do} AS INTEGER), "
        f"CAST({fare} AS DOUBLE), CAST({tip} AS DOUBLE), CAST({total} AS DOUBLE))"
        for pickup, dropoff, pu, do, fare, tip, total in rows
    )
    with duckdb.connect(":memory:") as connection:
        connection.execute(
            f"COPY (SELECT * FROM (VALUES {values}) AS t(tpep_pickup_datetime, "
            "tpep_dropoff_datetime, PULocationID, DOLocationID, fare_amount, tip_amount, "
            f"total_amount)) TO '{path.as_posix()}' (FORMAT PARQUET)"
        )
    return path


def test_period_parts_rejects_impossible_months() -> None:
    assert period_parts("2024-01") == (2024, 1)
    with pytest.raises(ValueError, match="invalid period"):
        period_parts("2024-13")


def test_service_specs_keep_fare_components_separate() -> None:
    yellow = set(SERVICE_SPECS["yellow"].fare_components)
    hvfhv = set(SERVICE_SPECS["hvfhv"].fare_components)

    assert yellow == {"fare_amount", "tip_amount", "total_amount"}
    assert hvfhv == {"base_passenger_fare", "tips", "driver_pay"}
    assert yellow & hvfhv == set()


def test_metric_names_cover_every_declared_component() -> None:
    spec = SERVICE_SPECS["hvfhv"]
    names = metric_names(spec)

    for column in spec.fare_components:
        assert f"{column}_non_null_rows" in names
        assert f"{column}_sum_cents" in names
    assert "total_amount_sum_cents" not in names


def test_quality_rules_count_planted_incidents(tmp_path: Path) -> None:
    fixture = write_yellow_fixture(tmp_path / "yellow.parquet")

    metrics = duckdb_metrics(SERVICE_SPECS["yellow"], fixture, "2024-01")

    assert metrics["source_rows"] == 5
    assert metrics["pickup_out_of_period_rows"] == 1
    assert metrics["negative_duration_rows"] == 1
    assert metrics["unresolved_duration_rows"] == 1
    assert metrics["null_pickup_zone_rows"] == 1
    assert metrics["null_dropoff_zone_rows"] == 1


def test_monetary_components_keep_explicit_denominators(tmp_path: Path) -> None:
    fixture = write_yellow_fixture(tmp_path / "yellow.parquet")

    metrics = duckdb_metrics(SERVICE_SPECS["yellow"], fixture, "2024-01")

    # A missing component is excluded from its denominator, never read as zero.
    assert metrics["fare_amount_non_null_rows"] == 4
    assert metrics["fare_amount_sum_cents"] == 1050 + 2025 + 500 + 775
    assert metrics["tip_amount_non_null_rows"] == 4
    assert metrics["tip_amount_sum_cents"] == 200 + 0 + 125 + 50
    assert metrics["total_amount_sum_cents"] == 1450 + 2375 + 825 + 1075


def test_metrics_sql_reads_the_declared_period_only(tmp_path: Path) -> None:
    fixture = write_yellow_fixture(tmp_path / "yellow.parquet")

    december = duckdb_metrics(SERVICE_SPECS["yellow"], fixture, "2023-12")

    assert december["pickup_out_of_period_rows"] == 4


def test_metrics_sql_projects_one_aggregate_per_metric() -> None:
    spec = SERVICE_SPECS["yellow"]
    sql = metrics_sql(spec, "source_occurrences", "2024-01")

    for name in metric_names(spec):
        assert f" AS {name}" in sql
    assert "base_passenger_fare" not in sql
    assert sql.endswith("FROM source_occurrences")


def test_alignment_reads_physical_row_numbers(tmp_path: Path) -> None:
    fixture = write_yellow_fixture(tmp_path / "yellow.parquet")

    rows = duckdb_alignment(SERVICE_SPECS["yellow"], fixture)

    assert [row[0] for row in rows] == [0, 1, 2, 3, 4]
    assert rows[0][1].isoformat() == "2024-01-05T10:00:00"
    assert rows[3][2] is None


def test_compare_metrics_reports_every_field() -> None:
    comparison = compare_metrics({"source_rows": 10, "nulls": 0}, {"source_rows": 10, "nulls": 1})

    assert comparison["all_match"] is False
    assert comparison["metrics"]["source_rows"]["match"] is True
    assert comparison["metrics"]["nulls"] == {
        "delta_spark": 0,
        "source_duckdb": 1,
        "match": False,
    }


def test_compare_alignment_reports_the_first_divergent_ordinal() -> None:
    delta_rows = [(0, "a"), (1, "b"), (2, "c")]

    assert compare_alignment(delta_rows, list(delta_rows))["match"] is True
    mismatch = compare_alignment(delta_rows, [(0, "a"), (1, "x"), (2, "c")])
    assert mismatch["match"] is False
    assert mismatch["first_mismatch_ordinal"] == 1
    assert compare_alignment(delta_rows, delta_rows[:2])["match"] is False
    assert compare_alignment([], [])["match"] is False
