from __future__ import annotations

import argparse
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import duckdb
import pytest

import fareline.m0 as m0
from fareline.m0 import (
    ReferenceFile,
    SourceFile,
    annual_files,
    inspect_zone_lookup,
    parquet_schema,
    parquet_summary,
    parse_period,
    schema_changes,
    schema_fingerprint,
    source_file,
    summarize_inventory,
)


class FakeResponse:
    def __init__(
        self,
        *,
        status: int,
        headers: dict[str, str],
        content: bytes = b"",
    ) -> None:
        self.status = status
        self.headers = headers
        self.content = content

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def read(self, limit: int = -1) -> bytes:
        return self.content if limit < 0 else self.content[:limit]


def test_source_file_uses_official_naming() -> None:
    yellow = source_file("yellow", 2024, 1)
    hvfhv = source_file("hvfhv", 2025, 12)

    assert yellow.filename == "yellow_tripdata_2024-01.parquet"
    assert yellow.url.endswith(yellow.filename)
    assert hvfhv.filename == "fhvhv_tripdata_2025-12.parquet"


@pytest.mark.parametrize(
    ("service", "year", "month"),
    [
        ("green", 2024, 1),
        ("yellow", 2018, 1),
        ("yellow", 2024, 0),
        ("yellow", 2024, 13),
        ("hvfhv", 2019, 1),
    ],
)
def test_source_file_rejects_out_of_scope_values(service: str, year: int, month: int) -> None:
    with pytest.raises(ValueError):
        source_file(service, year, month)


def test_annual_files_keeps_source_contracts_separate() -> None:
    files = annual_files(2024, ["yellow", "hvfhv"])

    assert len(files) == 24
    assert [item.service for item in files[:12]] == ["yellow"] * 12
    assert [item.service for item in files[12:]] == ["hvfhv"] * 12


def test_hvfhv_starts_in_february_2019() -> None:
    assert source_file("hvfhv", 2019, 2).filename == "fhvhv_tripdata_2019-02.parquet"


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


def test_inventory_summary_reports_target_not_met() -> None:
    files = [
        {
            "service": "yellow",
            "http": {"content_length_bytes": 10},
            "parquet": {"num_rows": 40},
        }
    ]

    assert summarize_inventory(files, target_rows=100)["target_met_by_metadata"] is False


def test_parse_period_rejects_non_iso_month() -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        parse_period("2024/01")


def test_http_errors_are_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    def fail(request: urllib.request.Request, timeout: int) -> Any:
        nonlocal calls
        calls += 1
        raise urllib.error.HTTPError(request.full_url, 404, "not found", None, None)

    monkeypatch.setattr(m0.urllib.request, "urlopen", fail)
    request = urllib.request.Request("https://example.invalid/missing")

    with pytest.raises(urllib.error.HTTPError):
        m0._request_with_retries(request)

    assert calls == 1


def test_remote_headers_uses_content_range_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    requests: list[urllib.request.Request] = []

    def respond(request: urllib.request.Request) -> FakeResponse:
        requests.append(request)
        if len(requests) == 1:
            raise urllib.error.HTTPError(request.full_url, 405, "method not allowed", None, None)
        return FakeResponse(
            status=206,
            headers={
                "Content-Length": "1",
                "Content-Range": "bytes 0-0/12331",
                "Content-Type": "text/csv",
                "ETag": '"zone-etag"',
            },
        )

    monkeypatch.setattr(m0, "_request_with_retries", respond)
    result = m0.remote_headers(ReferenceFile("zones", "https://example.invalid/zones.csv"))

    assert requests[0].get_method() == "HEAD"
    assert requests[1].get_header("Range") == "bytes=0-0"
    assert result["status"] == 206
    assert result["content_length_bytes"] == 12331


def test_parquet_metadata_readers_preserve_source_order(tmp_path: Path) -> None:
    target = tmp_path / "tiny.parquet"
    escaped_target = str(target).replace("'", "''")
    connection = duckdb.connect(":memory:")
    connection.execute(
        f"COPY (SELECT * FROM (VALUES (1, 'x'), (2, 'y')) AS rows(z, a)) "
        f"TO '{escaped_target}' (FORMAT PARQUET)"
    )
    item = SourceFile("yellow", 2024, 1, target.name, str(target))

    summary = parquet_summary(connection, item)
    schema = parquet_schema(connection, item)

    assert summary["num_rows"] == 2
    assert [column["name"] for column in schema] == ["z", "a"]
    assert [column["source_ordinal"] for column in schema] == [0, 1]
    fingerprint = schema_fingerprint(schema)
    assert len(fingerprint) == 64
    assert fingerprint != schema_fingerprint(list(reversed(schema)))
    connection.close()


def test_zone_lookup_summary_does_not_retain_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    content = (
        b"LocationID,Borough,Zone,service_zone\n"
        b"1,Manhattan,Newark Airport,EWR\n"
        b"2,Queens,Jamaica Bay,Boro Zone\n"
    )
    response = FakeResponse(
        status=200,
        headers={"Content-Length": str(len(content)), "Content-Type": "text/csv"},
        content=content,
    )
    monkeypatch.setattr(m0, "_request_with_retries", lambda _: response)

    result = inspect_zone_lookup()

    assert result["content"]["rows"] == 2
    assert result["content"]["columns"] == ["LocationID", "Borough", "Zone", "service_zone"]
    assert result["quality"] == {
        "duplicate_location_ids": 0,
        "invalid_location_ids": 0,
        "missing_required_columns": [],
    }
    assert result["redistributed"] is False
    assert "rows_data" not in result


def test_zone_lookup_rejects_missing_contract_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    content = b"LocationID,Zone\n1,Newark Airport\n"
    response = FakeResponse(
        status=200,
        headers={"Content-Length": str(len(content)), "Content-Type": "text/csv"},
        content=content,
    )
    monkeypatch.setattr(m0, "_request_with_retries", lambda _: response)

    with pytest.raises(ValueError, match="missing required columns"):
        inspect_zone_lookup()
