"""Service-specific quality rules and the DuckDB reference implementation.

The same SQL text is executed by Spark over the published source-occurrence table
and by DuckDB over the landed source file. Monetary components are summed as
integer cents so that the two engines can be compared exactly instead of within
a floating-point tolerance, and Yellow and HVFHV components are never mixed.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb


@dataclass(frozen=True)
class ServiceSpec:
    """Columns a service contract needs for the M1 quality rules."""

    service: str
    pickup_timestamp: str
    dropoff_timestamp: str
    pickup_zone: str
    dropoff_zone: str
    fare_components: tuple[str, ...]
    alignment_columns: tuple[str, ...]


SERVICE_SPECS: dict[str, ServiceSpec] = {
    "yellow": ServiceSpec(
        service="yellow",
        pickup_timestamp="tpep_pickup_datetime",
        dropoff_timestamp="tpep_dropoff_datetime",
        pickup_zone="PULocationID",
        dropoff_zone="DOLocationID",
        fare_components=("fare_amount", "tip_amount", "total_amount"),
        alignment_columns=("tpep_pickup_datetime", "PULocationID", "total_amount"),
    ),
    "hvfhv": ServiceSpec(
        service="hvfhv",
        pickup_timestamp="pickup_datetime",
        dropoff_timestamp="dropoff_datetime",
        pickup_zone="PULocationID",
        dropoff_zone="DOLocationID",
        fare_components=("base_passenger_fare", "tips", "driver_pay"),
        alignment_columns=("pickup_datetime", "PULocationID", "driver_pay"),
    ),
}


def period_parts(period: str) -> tuple[int, int]:
    year_text, month_text = period.split("-", 1)
    year, month = int(year_text), int(month_text)
    if month not in range(1, 13):
        raise ValueError(f"invalid period: {period}")
    return year, month


def metric_names(spec: ServiceSpec) -> tuple[str, ...]:
    names = [
        "source_rows",
        "null_pickup_zone_rows",
        "null_dropoff_zone_rows",
        "pickup_out_of_period_rows",
        "negative_duration_rows",
        "unresolved_duration_rows",
    ]
    for column in spec.fare_components:
        names.append(f"{column}_non_null_rows")
        names.append(f"{column}_sum_cents")
    return tuple(names)


def metrics_sql(spec: ServiceSpec, relation: str, period: str) -> str:
    """Build one portable aggregate that both Spark SQL and DuckDB accept."""
    year, month = period_parts(period)
    pickup, dropoff = spec.pickup_timestamp, spec.dropoff_timestamp
    projections = [
        "count(*) AS source_rows",
        f"sum(CASE WHEN {spec.pickup_zone} IS NULL THEN 1 ELSE 0 END) AS null_pickup_zone_rows",
        f"sum(CASE WHEN {spec.dropoff_zone} IS NULL THEN 1 ELSE 0 END) AS null_dropoff_zone_rows",
        (
            f"sum(CASE WHEN {pickup} IS NULL OR year({pickup}) <> {year} "
            f"OR month({pickup}) <> {month} THEN 1 ELSE 0 END) AS pickup_out_of_period_rows"
        ),
        (f"sum(CASE WHEN {dropoff} < {pickup} THEN 1 ELSE 0 END) AS negative_duration_rows"),
        (
            f"sum(CASE WHEN {pickup} IS NULL OR {dropoff} IS NULL THEN 1 ELSE 0 END) "
            "AS unresolved_duration_rows"
        ),
    ]
    for column in spec.fare_components:
        # count() excludes nulls, so the denominator stays explicit instead of
        # turning a missing component into zero.
        projections.append(f"count({column}) AS {column}_non_null_rows")
        projections.append(f"sum(CAST(round({column} * 100) AS BIGINT)) AS {column}_sum_cents")
    return "SELECT " + ", ".join(projections) + f" FROM {relation}"


def duckdb_metrics(
    spec: ServiceSpec, parquet_path: Path | str, period: str
) -> dict[str, int | None]:
    """Run the quality rules over the landed source file with DuckDB."""
    relation = f"read_parquet({_sql_literal(str(parquet_path))})"
    with duckdb.connect(":memory:") as connection:
        row = connection.execute(metrics_sql(spec, relation, period)).fetchone()
    if row is None:
        raise RuntimeError(f"no DuckDB metrics returned for {spec.service}")
    return _normalize(metric_names(spec), row)


def duckdb_alignment(spec: ServiceSpec, parquet_path: Path | str) -> list[tuple[Any, ...]]:
    """Read physical row numbers and probe columns in source order."""
    columns = ", ".join(spec.alignment_columns)
    query = (
        f"SELECT file_row_number, {columns} "
        f"FROM read_parquet({_sql_literal(str(parquet_path))}, file_row_number=true) "
        "ORDER BY file_row_number"
    )
    with duckdb.connect(":memory:") as connection:
        return [tuple(row) for row in connection.execute(query).fetchall()]


def compare_metrics(
    spark_metrics: dict[str, int | None], oracle_metrics: dict[str, int | None]
) -> dict[str, Any]:
    """Compare engine results field by field without hiding a mismatch."""
    comparisons = {}
    for name in sorted(set(spark_metrics) | set(oracle_metrics)):
        spark_value = spark_metrics.get(name)
        oracle_value = oracle_metrics.get(name)
        comparisons[name] = {
            "delta_spark": spark_value,
            "source_duckdb": oracle_value,
            "match": spark_value == oracle_value,
        }
    return {
        "all_match": all(item["match"] for item in comparisons.values()),
        "metrics": comparisons,
    }


def compare_alignment(
    delta_rows: list[tuple[Any, ...]], oracle_rows: list[tuple[Any, ...]]
) -> dict[str, Any]:
    """Check that each technical ordinal carries the same physical row."""
    first_mismatch: int | None = None
    for delta_row, oracle in zip(delta_rows, oracle_rows, strict=False):
        if delta_row != oracle:
            first_mismatch = int(delta_row[0])
            break
    matched = first_mismatch is None and len(delta_rows) == len(oracle_rows) and bool(delta_rows)
    return {
        "compared_rows": min(len(delta_rows), len(oracle_rows)),
        "delta_rows": len(delta_rows),
        "oracle_rows": len(oracle_rows),
        # Only the ordinal is reported; source values never leave memory.
        "first_mismatch_ordinal": first_mismatch,
        "match": matched,
    }


def _normalize(names: tuple[str, ...], row: tuple[Any, ...]) -> dict[str, int | None]:
    return {
        name: (None if value is None else int(value))
        for name, value in zip(names, row, strict=True)
    }


def _sql_literal(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"
