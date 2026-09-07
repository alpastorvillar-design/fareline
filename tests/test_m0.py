from __future__ import annotations

import argparse

import pytest

from fareline.m0 import annual_files, parse_period, schema_changes, source_file, summarize_inventory


def test_source_file_uses_official_naming() -> None:
    yellow = source_file("yellow", 2024, 1)
    hvfhv = source_file("hvfhv", 2025, 12)

    assert yellow.filename == "yellow_tripdata_2024-01.parquet"
    assert yellow.url.endswith(yellow.filename)
    assert hvfhv.filename == "fhvhv_tripdata_2025-12.parquet"


@pytest.mark.parametrize(
    ("service", "year", "month"),
    [("green", 2024, 1), ("yellow", 2018, 1), ("yellow", 2024, 0), ("yellow", 2024, 13)],
)
def test_source_file_rejects_out_of_scope_values(service: str, year: int, month: int) -> None:
    with pytest.raises(ValueError):
        source_file(service, year, month)


def test_annual_files_keeps_source_contracts_separate() -> None:
    files = annual_files(2024, ["yellow", "hvfhv"])

    assert len(files) == 24
    assert [item.service for item in files[:12]] == ["yellow"] * 12
    assert [item.service for item in files[12:]] == ["hvfhv"] * 12


def test_schema_changes_reports_add_remove_and_type_change() -> None:
    before = [
        {"name": "kept", "duckdb_type": "BIGINT"},
        {"name": "removed", "duckdb_type": "VARCHAR"},
        {"name": "changed", "duckdb_type": "INTEGER"},
    ]
    after = [
        {"name": "kept", "duckdb_type": "BIGINT"},
        {"name": "added", "duckdb_type": "DOUBLE"},
        {"name": "changed", "duckdb_type": "BIGINT"},
    ]

    assert schema_changes(before, after) == {
        "added": ["added"],
        "removed": ["removed"],
        "type_changes": [{"name": "changed", "before": "INTEGER", "after": "BIGINT"}],
    }


def test_inventory_summary_uses_exact_rows_and_bytes() -> None:
    files = [
        {
            "service": "yellow",
            "http": {"content_length_bytes": 10},
            "parquet": {"num_rows": 40},
        },
        {
            "service": "hvfhv",
            "http": {"content_length_bytes": 20},
            "parquet": {"num_rows": 70},
        },
    ]

    summary = summarize_inventory(files, target_rows=100)

    assert summary["combined"] == {"files": 2, "rows": 110, "bytes": 30}
    assert summary["target_met_by_metadata"] is True


def test_parse_period_rejects_non_iso_month() -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        parse_period("2024/01")
