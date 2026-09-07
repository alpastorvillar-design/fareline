"""Bounded source inspection for Fareline milestone M0."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
import urllib.error
import urllib.request
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

SOURCE_PAGE = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
NYC_TERMS = "https://www.nyc.gov/main/terms-of-use"
OPEN_DATA_FAQ = "https://opendata.cityofnewyork.us/faq/"
YELLOW_DICTIONARY = (
    "https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf"
)
HVFHV_DICTIONARY = (
    "https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_hvfhs.pdf"
)
TRIP_DATA_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"
SERVICE_PREFIXES = {"yellow": "yellow_tripdata", "hvfhv": "fhvhv_tripdata"}
USER_AGENT = "Fareline-M0/0.1 (+https://github.com/alpastorvillar-design/fareline)"


@dataclass(frozen=True)
class SourceFile:
    service: str
    year: int
    month: int
    filename: str
    url: str


def source_file(service: str, year: int, month: int) -> SourceFile:
    """Build one official TLC monthly Parquet URL."""
    if service not in SERVICE_PREFIXES:
        raise ValueError(f"Unsupported service: {service}")
    if year < 2019:
        raise ValueError("Fareline sources start in 2019 because HVFHV is required")
    if month not in range(1, 13):
        raise ValueError(f"Invalid month: {month}")
    filename = f"{SERVICE_PREFIXES[service]}_{year}-{month:02d}.parquet"
    return SourceFile(service, year, month, filename, f"{TRIP_DATA_BASE}/{filename}")


def annual_files(year: int, services: Sequence[str]) -> list[SourceFile]:
    return [source_file(service, year, month) for service in services for month in range(1, 13)]


def _request_with_retries(request: urllib.request.Request, attempts: int = 3):
    for attempt in range(1, attempts + 1):
        try:
            return urllib.request.urlopen(request, timeout=45)  # noqa: S310
        except (TimeoutError, urllib.error.URLError):
            if attempt == attempts:
                raise
            time.sleep(2 ** (attempt - 1))
    raise AssertionError("unreachable")


def remote_headers(item: SourceFile) -> dict[str, Any]:
    """Read object headers without downloading the Parquet body."""
    request = urllib.request.Request(
        item.url,
        method="HEAD",
        headers={"User-Agent": USER_AGENT, "Accept-Encoding": "identity"},
    )
    try:
        with _request_with_retries(request) as response:
            headers = response.headers
            status = response.status
    except urllib.error.HTTPError as error:
        if error.code not in {403, 405}:
            raise
        request = urllib.request.Request(
            item.url,
            headers={
                "User-Agent": USER_AGENT,
                "Accept-Encoding": "identity",
                "Range": "bytes=0-0",
            },
        )
        with _request_with_retries(request) as response:
            headers = response.headers
            status = response.status

    content_length = headers.get("Content-Length")
    content_range = headers.get("Content-Range")
    if content_range and "/" in content_range:
        content_length = content_range.rsplit("/", 1)[1]
    return {
        "status": status,
        "content_length_bytes": int(content_length) if content_length else None,
        "content_type": headers.get("Content-Type"),
        "etag": headers.get("ETag"),
        "last_modified": headers.get("Last-Modified"),
        "accept_ranges": headers.get("Accept-Ranges"),
    }


def prepare_duckdb() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(":memory:")
    connection.execute("INSTALL httpfs")
    connection.execute("LOAD httpfs")
    connection.execute("SET enable_http_metadata_cache = true")
    connection.execute("SET threads = 4")
    return connection


def parquet_summary(connection: duckdb.DuckDBPyConnection, item: SourceFile) -> dict[str, Any]:
    row = connection.execute(
        """
        SELECT num_rows, num_row_groups, created_by, format_version
        FROM parquet_file_metadata(?)
        """,
        [item.url],
    ).fetchone()
    if row is None:
        raise RuntimeError(f"No Parquet metadata returned for {item.url}")
    return {
        "num_rows": row[0],
        "num_row_groups": row[1],
        "created_by": row[2],
        "format_version": row[3],
    }


def parquet_schema(connection: duckdb.DuckDBPyConnection, item: SourceFile) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT name, type, repetition_type, converted_type, logical_type, duckdb_type
        FROM parquet_schema(?)
        WHERE type IS NOT NULL
        ORDER BY field_id, name
        """,
        [item.url],
    ).fetchall()
    keys = (
        "name",
        "physical_type",
        "repetition_type",
        "converted_type",
        "logical_type",
        "duckdb_type",
    )
    return [dict(zip(keys, row, strict=True)) for row in rows]


def schema_changes(
    before: Iterable[dict[str, Any]], after: Iterable[dict[str, Any]]
) -> dict[str, Any]:
    before_by_name = {column["name"]: column for column in before}
    after_by_name = {column["name"]: column for column in after}
    return {
        "added": sorted(after_by_name.keys() - before_by_name.keys()),
        "removed": sorted(before_by_name.keys() - after_by_name.keys()),
        "type_changes": [
            {
                "name": name,
                "before": before_by_name[name]["duckdb_type"],
                "after": after_by_name[name]["duckdb_type"],
            }
            for name in sorted(before_by_name.keys() & after_by_name.keys())
            if before_by_name[name]["duckdb_type"] != after_by_name[name]["duckdb_type"]
        ],
    }


def summarize_inventory(files: Iterable[dict[str, Any]], target_rows: int) -> dict[str, Any]:
    by_service: dict[str, dict[str, int]] = {}
    for item in files:
        totals = by_service.setdefault(item["service"], {"files": 0, "rows": 0, "bytes": 0})
        totals["files"] += 1
        totals["rows"] += int(item["parquet"]["num_rows"])
        totals["bytes"] += int(item["http"]["content_length_bytes"])
    combined_rows = sum(item["rows"] for item in by_service.values())
    combined_bytes = sum(item["bytes"] for item in by_service.values())
    return {
        "by_service": by_service,
        "combined": {
            "files": sum(item["files"] for item in by_service.values()),
            "rows": combined_rows,
            "bytes": combined_bytes,
        },
        "target_rows": target_rows,
        "target_met_by_metadata": combined_rows >= target_rows,
    }


def sample_remote_file(
    connection: duckdb.DuckDBPyConnection,
    item: SourceFile,
    sample_dir: Path,
    row_limit: int,
) -> dict[str, Any]:
    sample_dir.mkdir(parents=True, exist_ok=True)
    target = sample_dir / item.filename
    escaped_target = str(target.resolve()).replace("'", "''")
    connection.execute(
        f"COPY (SELECT * FROM read_parquet(?) LIMIT {row_limit}) "
        f"TO '{escaped_target}' (FORMAT PARQUET, COMPRESSION ZSTD)",
        [item.url],
    )
    digest = hashlib.sha256(target.read_bytes()).hexdigest()
    rows = connection.execute("SELECT count(*) FROM read_parquet(?)", [str(target)]).fetchone()[0]
    return {
        "service": item.service,
        "year": item.year,
        "month": item.month,
        "rows": rows,
        "bytes": target.stat().st_size,
        "sha256": digest,
        "path": str(target.as_posix()),
        "redistributed": False,
    }


def inspect_sources(
    inventory_year: int,
    schema_periods: Sequence[tuple[int, int]],
    services: Sequence[str],
    target_rows: int,
    sample_rows: int,
    sample_dir: Path,
) -> dict[str, Any]:
    connection = prepare_duckdb()
    file_results: list[dict[str, Any]] = []
    for item in annual_files(inventory_year, services):
        file_results.append(
            {
                **asdict(item),
                "http": remote_headers(item),
                "parquet": parquet_summary(connection, item),
            }
        )

    schemas: dict[str, list[dict[str, Any]]] = {}
    samples: list[dict[str, Any]] = []
    for year, month in schema_periods:
        for service in services:
            item = source_file(service, year, month)
            key = f"{service}_{year}_{month:02d}"
            schemas[key] = parquet_schema(connection, item)
            if sample_rows:
                samples.append(sample_remote_file(connection, item, sample_dir, sample_rows))

    changes: dict[str, Any] = {}
    if len(schema_periods) >= 2:
        first_year, first_month = schema_periods[0]
        last_year, last_month = schema_periods[-1]
        for service in services:
            changes[service] = schema_changes(
                schemas[f"{service}_{first_year}_{first_month:02d}"],
                schemas[f"{service}_{last_year}_{last_month:02d}"],
            )

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "scope": {
            "inventory_year": inventory_year,
            "services": list(services),
            "schema_periods": [f"{year}-{month:02d}" for year, month in schema_periods],
            "sample_rows_per_file": sample_rows,
            "method": "HTTP headers and Parquet footers; bounded row samples are not published",
        },
        "sources": {
            "trip_record_page": SOURCE_PAGE,
            "yellow_dictionary": YELLOW_DICTIONARY,
            "hvfhv_dictionary": HVFHV_DICTIONARY,
            "nyc_terms": NYC_TERMS,
            "open_data_faq": OPEN_DATA_FAQ,
        },
        "files": file_results,
        "summary": summarize_inventory(file_results, target_rows),
        "schemas": schemas,
        "schema_changes": changes,
        "samples": samples,
        "redistribution_policy": (
            "No TLC row data is committed; only derived metadata is published."
        ),
    }


def parse_period(value: str) -> tuple[int, int]:
    try:
        year_text, month_text = value.split("-", 1)
        year, month = int(year_text), int(month_text)
    except ValueError as error:
        raise argparse.ArgumentTypeError("period must use YYYY-MM") from error
    try:
        source_file("yellow", year, month)
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from error
    return year, month


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory-year", type=int, default=2024)
    parser.add_argument(
        "--schema-period",
        action="append",
        type=parse_period,
        default=None,
        help="repeatable YYYY-MM probe; defaults to 2024-01 and 2025-01",
    )
    parser.add_argument(
        "--services", nargs="+", choices=sorted(SERVICE_PREFIXES), default=["yellow", "hvfhv"]
    )
    parser.add_argument("--target-rows", type=int, default=100_000_000)
    parser.add_argument("--sample-rows", type=int, default=0)
    parser.add_argument("--sample-dir", type=Path, default=Path("data/samples"))
    parser.add_argument("--output", type=Path, default=Path("evidence/m0/source_inventory.json"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.sample_rows < 0:
        raise SystemExit("--sample-rows must be non-negative")
    periods = args.schema_period or [(2024, 1), (2025, 1)]
    report = inspect_sources(
        inventory_year=args.inventory_year,
        schema_periods=periods,
        services=args.services,
        target_rows=args.target_rows,
        sample_rows=args.sample_rows,
        sample_dir=args.sample_dir,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report["summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
