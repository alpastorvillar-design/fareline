"""Bounded source inspection for Fareline milestone M0."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import platform
import time
import urllib.error
import urllib.request
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from fareline import __version__

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
TAXI_ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
SERVICE_PREFIXES = {"yellow": "yellow_tripdata", "hvfhv": "fhvhv_tripdata"}
SERVICE_START_PERIODS = {"yellow": (2019, 1), "hvfhv": (2019, 2)}
REQUIRED_ZONE_COLUMNS = {"LocationID", "Borough", "Zone", "service_zone"}
USER_AGENT = f"Fareline/{__version__} (+https://github.com/alpastorvillar-design/fareline)"


@dataclass(frozen=True)
class SourceFile:
    service: str
    year: int
    month: int
    filename: str
    url: str


@dataclass(frozen=True)
class ReferenceFile:
    name: str
    url: str


TAXI_ZONE_LOOKUP = ReferenceFile("taxi_zone_lookup", TAXI_ZONE_LOOKUP_URL)


def source_file(service: str, year: int, month: int) -> SourceFile:
    """Build one official TLC monthly Parquet URL."""
    if service not in SERVICE_PREFIXES:
        raise ValueError(f"Unsupported service: {service}")
    if month not in range(1, 13):
        raise ValueError(f"Invalid month: {month}")
    if (year, month) < SERVICE_START_PERIODS[service]:
        first_year, first_month = SERVICE_START_PERIODS[service]
        raise ValueError(f"{service} Fareline sources start at {first_year}-{first_month:02d}")
    filename = f"{SERVICE_PREFIXES[service]}_{year}-{month:02d}.parquet"
    return SourceFile(service, year, month, filename, f"{TRIP_DATA_BASE}/{filename}")


def annual_files(year: int, services: Sequence[str]) -> list[SourceFile]:
    return [source_file(service, year, month) for service in services for month in range(1, 13)]


def _request_with_retries(request: urllib.request.Request, attempts: int = 3):
    for attempt in range(1, attempts + 1):
        try:
            return urllib.request.urlopen(request, timeout=45)
        except urllib.error.HTTPError:
            raise
        except (TimeoutError, urllib.error.URLError):
            if attempt == attempts:
                raise
            time.sleep(2 ** (attempt - 1))
    raise AssertionError("unreachable")


def remote_headers(item: SourceFile | ReferenceFile) -> dict[str, Any]:
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
        SELECT row_number() OVER () - 1 AS source_ordinal,
               name, type, repetition_type, converted_type, logical_type, duckdb_type
        FROM parquet_schema(?)
        WHERE type IS NOT NULL
        ORDER BY source_ordinal
        """,
        [item.url],
    ).fetchall()
    keys = (
        "source_ordinal",
        "name",
        "physical_type",
        "repetition_type",
        "converted_type",
        "logical_type",
        "duckdb_type",
    )
    return [dict(zip(keys, row, strict=True)) for row in rows]


def schema_fingerprint(columns: Sequence[dict[str, Any]]) -> str:
    """Hash a full ordered physical/logical schema without source data."""
    payload = json.dumps(columns, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def inspect_zone_lookup(
    item: ReferenceFile = TAXI_ZONE_LOOKUP,
    max_bytes: int = 1_000_000,
) -> dict[str, Any]:
    """Download and summarize the small zone dimension without retaining rows."""
    request = urllib.request.Request(
        item.url,
        headers={"User-Agent": USER_AGENT, "Accept-Encoding": "identity"},
    )
    with _request_with_retries(request) as response:
        declared_length = response.headers.get("Content-Length")
        if declared_length and int(declared_length) > max_bytes:
            raise ValueError(f"Reference file exceeds {max_bytes} bytes: {item.url}")
        content = response.read(max_bytes + 1)
        if len(content) > max_bytes:
            raise ValueError(f"Reference file exceeds {max_bytes} bytes: {item.url}")
        http = {
            "status": response.status,
            "content_length_bytes": len(content),
            "content_type": response.headers.get("Content-Type"),
            "etag": response.headers.get("ETag"),
            "last_modified": response.headers.get("Last-Modified"),
        }

    reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig")))
    columns = reader.fieldnames or []
    missing_columns = sorted(REQUIRED_ZONE_COLUMNS - set(columns))
    if missing_columns:
        raise ValueError(f"Zone lookup is missing required columns: {missing_columns}")
    location_ids: set[int] = set()
    duplicate_location_ids = 0
    invalid_location_ids = 0
    row_count = 0
    for row in reader:
        row_count += 1
        try:
            location_id = int((row.get("LocationID") or "").strip())
        except ValueError:
            invalid_location_ids += 1
            continue
        if location_id in location_ids:
            duplicate_location_ids += 1
        location_ids.add(location_id)

    return {
        **asdict(item),
        "http": http,
        "content": {
            "sha256": hashlib.sha256(content).hexdigest(),
            "rows": row_count,
            "columns": columns,
        },
        "quality": {
            "duplicate_location_ids": duplicate_location_ids,
            "invalid_location_ids": invalid_location_ids,
            "missing_required_columns": missing_columns,
        },
        "redistributed": False,
    }


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
    sample_periods: Sequence[tuple[int, int]],
) -> dict[str, Any]:
    connection = prepare_duckdb()
    file_results: list[dict[str, Any]] = []
    annual_schema_inventory: dict[str, dict[str, Any]] = {}
    for item in annual_files(inventory_year, services):
        schema = parquet_schema(connection, item)
        fingerprint = schema_fingerprint(schema)
        file_results.append(
            {
                **asdict(item),
                "http": remote_headers(item),
                "parquet": parquet_summary(connection, item),
                "schema": {
                    "column_count": len(schema),
                    "fingerprint": fingerprint,
                },
            }
        )
        service_schemas = annual_schema_inventory.setdefault(
            item.service,
            {"periods": [], "unique_schemas": {}},
        )
        service_schemas["periods"].append(
            {
                "period": f"{item.year}-{item.month:02d}",
                "column_count": len(schema),
                "fingerprint": fingerprint,
            }
        )
        service_schemas["unique_schemas"].setdefault(fingerprint, schema)

    for service_schemas in annual_schema_inventory.values():
        service_schemas["unique_schema_count"] = len(service_schemas["unique_schemas"])

    schemas: dict[str, list[dict[str, Any]]] = {}
    samples: list[dict[str, Any]] = []
    for year, month in schema_periods:
        for service in services:
            item = source_file(service, year, month)
            key = f"{service}_{year}_{month:02d}"
            schemas[key] = parquet_schema(connection, item)

    if sample_rows:
        for year, month in sample_periods:
            for service in services:
                samples.append(
                    sample_remote_file(
                        connection,
                        source_file(service, year, month),
                        sample_dir,
                        sample_rows,
                    )
                )

    changes: dict[str, Any] = {}
    change_history: dict[str, list[dict[str, Any]]] = {}
    if len(schema_periods) >= 2:
        first_year, first_month = schema_periods[0]
        last_year, last_month = schema_periods[-1]
        for service in services:
            changes[service] = schema_changes(
                schemas[f"{service}_{first_year}_{first_month:02d}"],
                schemas[f"{service}_{last_year}_{last_month:02d}"],
            )
            change_history[service] = []
            for before, after in zip(schema_periods, schema_periods[1:], strict=False):
                before_year, before_month = before
                after_year, after_month = after
                change_history[service].append(
                    {
                        "before": f"{before_year}-{before_month:02d}",
                        "after": f"{after_year}-{after_month:02d}",
                        **schema_changes(
                            schemas[f"{service}_{before_year}_{before_month:02d}"],
                            schemas[f"{service}_{after_year}_{after_month:02d}"],
                        ),
                    }
                )

    reference_files = [inspect_zone_lookup()]
    connection.close()

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope": {
            "inventory_year": inventory_year,
            "services": list(services),
            "schema_periods": [f"{year}-{month:02d}" for year, month in schema_periods],
            "sample_rows_per_file": sample_rows,
            "sample_periods": [f"{year}-{month:02d}" for year, month in sample_periods],
            "method": (
                "HTTP headers, Parquet footers, bounded row samples, and an in-memory "
                "zone lookup; source rows are not published"
            ),
        },
        "runtime": {
            "python_version": platform.python_version(),
            "duckdb_version": duckdb.__version__,
        },
        "sources": {
            "trip_record_page": SOURCE_PAGE,
            "yellow_dictionary": YELLOW_DICTIONARY,
            "hvfhv_dictionary": HVFHV_DICTIONARY,
            "nyc_terms": NYC_TERMS,
            "open_data_faq": OPEN_DATA_FAQ,
            "taxi_zone_lookup": TAXI_ZONE_LOOKUP_URL,
        },
        "files": file_results,
        "reference_files": reference_files,
        "summary": summarize_inventory(file_results, target_rows),
        "annual_schema_inventory": annual_schema_inventory,
        "schemas": schemas,
        "schema_changes": changes,
        "schema_change_history": change_history,
        "samples": samples,
        "redistribution_policy": (
            "No TLC trip or zone rows are committed; only derived metadata is published."
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
    parser.add_argument(
        "--sample-period",
        action="append",
        type=parse_period,
        default=None,
        help="repeatable YYYY-MM sample period; defaults to 2024-01 and 2025-01",
    )
    parser.add_argument("--sample-dir", type=Path, default=Path("data/samples"))
    parser.add_argument("--output", type=Path, default=Path("evidence/m0/source_inventory.json"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.sample_rows < 0:
        raise SystemExit("--sample-rows must be non-negative")
    periods = args.schema_period or [(2024, 1), (2025, 1)]
    sample_periods = args.sample_period or [(2024, 1), (2025, 1)]
    report = inspect_sources(
        inventory_year=args.inventory_year,
        schema_periods=periods,
        services=args.services,
        target_rows=args.target_rows,
        sample_rows=args.sample_rows,
        sample_dir=args.sample_dir,
        sample_periods=sample_periods,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report["summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
