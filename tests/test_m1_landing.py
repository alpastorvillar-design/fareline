from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import duckdb
import pytest

import fareline.m1.landing as landing_module
from fareline.m1.landing import (
    ArtifactIdentity,
    ArtifactRejected,
    LandingStore,
    acquire,
    fetch_https,
    fetch_local_file,
    validate_parquet,
    validate_zone_lookup,
    version_id,
)

ZONE_CSV = (
    "LocationID,Borough,Zone,service_zone\n"
    "1,EWR,Newark Airport,EWR\n"
    "2,Queens,Jamaica Bay,Boro Zone\n"
)


class FakeResponse:
    def __init__(self, content: bytes, headers: dict[str, str], status: int = 200) -> None:
        self.content = content
        self.headers = headers
        self.status = status
        self._offset = 0

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            chunk, self._offset = self.content[self._offset :], len(self.content)
            return chunk
        chunk = self.content[self._offset : self._offset + size]
        self._offset += len(chunk)
        return chunk


def trip_identity(completeness: str = "bounded_sample") -> ArtifactIdentity:
    return ArtifactIdentity(
        artifact_kind="trip_records",
        filename="yellow_tripdata_2024-01.parquet",
        logical_url="https://example.invalid/yellow_tripdata_2024-01.parquet",
        service="yellow",
        period="2024-01",
        completeness=completeness,
    )


def write_parquet(path: Path, rows: int, offset: int = 0) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(":memory:") as connection:
        connection.execute(
            f"COPY (SELECT i + {offset} AS n FROM range(0, {rows}) t(i)) "
            f"TO '{path.as_posix()}' (FORMAT PARQUET)"
        )
    return path


def land(store: LandingStore, source: Path, identity: ArtifactIdentity, run_id: str) -> Any:
    return acquire(
        store,
        identity,
        fetch_local_file(source),
        validate_parquet,
        run_id=run_id,
    )


def test_version_id_binds_logical_identity_to_content() -> None:
    assert version_id("trip_records:bounded_sample:yellow:2024-01", "a" * 64) == version_id(
        "trip_records:bounded_sample:yellow:2024-01", "a" * 64
    )
    assert version_id("trip_records:bounded_sample:yellow:2024-01", "a" * 64) != version_id(
        "trip_records:bounded_sample:yellow:2024-01", "b" * 64
    )
    assert version_id("trip_records:bounded_sample:yellow:2024-01", "a" * 64) != version_id(
        "trip_records:bounded_sample:hvfhv:2024-01", "a" * 64
    )


def test_completeness_is_part_of_logical_identity() -> None:
    bounded = trip_identity("bounded_sample")
    complete = trip_identity("complete_object")

    assert bounded.logical_id == "trip_records:bounded_sample:yellow:2024-01"
    assert complete.logical_id == "trip_records:complete_object:yellow:2024-01"
    assert bounded.logical_id != complete.logical_id


def test_publishing_stores_an_immutable_version(tmp_path: Path) -> None:
    store = LandingStore(tmp_path / "landing")
    source = write_parquet(tmp_path / "source" / "yellow_tripdata_2024-01.parquet", 25)
    identity = trip_identity()

    result = land(store, source, identity, "run-1")

    assert result.state == "published"
    assert result.artifact_path.is_file()
    assert result.artifact_path.read_bytes() == source.read_bytes()
    manifest = result.manifest
    assert manifest["publication_state"] == "published"
    assert manifest["content_length_bytes"] == source.stat().st_size
    assert manifest["content_profile"] == {
        "format": "parquet",
        "rows": 25,
        "row_groups": 1,
        "format_version": manifest["content_profile"]["format_version"],
        "columns": 1,
    }
    assert (store.root / manifest["relative_path"]).is_file()
    assert manifest["upstream"] == {}


def test_local_origin_never_records_a_private_path(tmp_path: Path) -> None:
    store = LandingStore(tmp_path / "landing")
    source = write_parquet(tmp_path / "private" / "deep" / "yellow_tripdata_2024-01.parquet", 5)

    manifest = land(store, source, trip_identity(), "run-1").manifest

    assert manifest["transport"] == {
        "origin_kind": "local_file",
        "origin_reference": "yellow_tripdata_2024-01.parquet",
    }
    assert "private" not in json.dumps(manifest)


def test_identical_content_replays_without_rewriting_history(tmp_path: Path) -> None:
    store = LandingStore(tmp_path / "landing")
    source = write_parquet(tmp_path / "source" / "yellow_tripdata_2024-01.parquet", 10)
    identity = trip_identity()

    first = land(store, source, identity, "run-1")
    manifest_path = store.manifest_path(identity, first.version_id)
    original = manifest_path.read_bytes()

    second = land(store, source, identity, "run-2")

    assert second.state == "replayed"
    assert second.version_id == first.version_id
    assert manifest_path.read_bytes() == original
    assert len(store.versions(identity)) == 1
    assert [event["state"] for event in store.events()] == ["published", "replayed"]


@pytest.mark.parametrize("damage", ["missing", "corrupt"])
def test_replay_repairs_a_missing_or_corrupt_landed_file(tmp_path: Path, damage: str) -> None:
    store = LandingStore(tmp_path / "landing")
    source = write_parquet(tmp_path / "source" / "yellow_tripdata_2024-01.parquet", 10)
    identity = trip_identity()
    first = land(store, source, identity, "run-1")
    expected = source.read_bytes()

    if damage == "missing":
        first.artifact_path.unlink()
    else:
        first.artifact_path.write_bytes(b"damaged")

    repaired = land(store, source, identity, "run-2")

    assert repaired.state == "repaired"
    assert repaired.version_id == first.version_id
    assert repaired.artifact_path.read_bytes() == expected
    assert len(store.versions(identity)) == 1
    assert [event["state"] for event in store.events()] == ["published", "repaired"]


def test_changed_content_creates_a_second_version(tmp_path: Path) -> None:
    store = LandingStore(tmp_path / "landing")
    source = tmp_path / "source" / "yellow_tripdata_2024-01.parquet"
    identity = trip_identity()

    write_parquet(source, 10)
    first = land(store, source, identity, "run-1")
    write_parquet(source, 10, offset=1000)
    second = land(store, source, identity, "run-2")

    assert second.state == "published"
    assert second.version_id != first.version_id
    assert store.manifest_path(identity, first.version_id).is_file()
    assert first.artifact_path.is_file()
    assert {record["version_id"] for record in store.versions(identity)} == {
        first.version_id,
        second.version_id,
    }


def test_unreadable_artifact_is_rejected_and_leaves_no_partial(tmp_path: Path) -> None:
    store = LandingStore(tmp_path / "landing")
    source = tmp_path / "source" / "yellow_tripdata_2024-01.parquet"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"not a parquet file")
    identity = trip_identity()

    with pytest.raises(ArtifactRejected, match="unreadable Parquet"):
        land(store, source, identity, "run-1")

    assert list(store.incoming_root.iterdir()) == []
    assert store.versions(identity) == []
    assert not (store.files_root / identity.slug).exists()
    assert [event["state"] for event in store.events()] == ["rejected"]


def test_empty_artifact_is_rejected(tmp_path: Path) -> None:
    store = LandingStore(tmp_path / "landing")
    source = tmp_path / "source" / "yellow_tripdata_2024-01.parquet"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"")

    with pytest.raises(ArtifactRejected, match="empty"):
        land(store, source, trip_identity(), "run-1")

    assert list(store.incoming_root.iterdir()) == []


def test_size_mismatch_is_rejected(tmp_path: Path) -> None:
    store = LandingStore(tmp_path / "landing")
    source = write_parquet(tmp_path / "source" / "yellow_tripdata_2024-01.parquet", 5)

    with pytest.raises(ArtifactRejected, match="expected 1 bytes"):
        acquire(
            store,
            trip_identity(),
            fetch_local_file(source),
            validate_parquet,
            run_id="run-1",
            expected_bytes=1,
        )

    assert list(store.incoming_root.iterdir()) == []


def test_expected_content_hash_is_enforced_before_publication(tmp_path: Path) -> None:
    store = LandingStore(tmp_path / "landing")
    source = write_parquet(tmp_path / "source" / "yellow_tripdata_2024-01.parquet", 5)
    identity = trip_identity()

    with pytest.raises(ArtifactRejected, match="expected SHA-256"):
        acquire(
            store,
            identity,
            fetch_local_file(source),
            validate_parquet,
            run_id="run-1",
            expected_sha256="0" * 64,
        )

    assert store.versions(identity) == []
    assert not (store.files_root / identity.slug).exists()
    assert [event["state"] for event in store.events()] == ["rejected"]


def test_inconsistent_existing_manifest_is_rejected(tmp_path: Path) -> None:
    store = LandingStore(tmp_path / "landing")
    source = write_parquet(tmp_path / "source" / "yellow_tripdata_2024-01.parquet", 5)
    identity = trip_identity()
    first = land(store, source, identity, "run-1")
    manifest_path = store.manifest_path(identity, first.version_id)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["content_length_bytes"] += 1
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ArtifactRejected, match="manifest is inconsistent"):
        land(store, source, identity, "run-2")

    assert [event["state"] for event in store.events()] == ["published", "rejected"]


def test_missing_source_is_rejected(tmp_path: Path) -> None:
    store = LandingStore(tmp_path / "landing")

    with pytest.raises(ArtifactRejected, match="missing"):
        land(store, tmp_path / "absent.parquet", trip_identity(), "run-1")


def test_https_acquisition_records_advisory_transport_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    content = ZONE_CSV.encode()
    response = FakeResponse(
        content,
        {"Content-Length": str(len(content)), "ETag": '"zone-etag"', "Last-Modified": "Mon"},
    )
    monkeypatch.setattr(landing_module.urllib.request, "urlopen", lambda *_, **__: response)
    store = LandingStore(tmp_path / "landing")
    identity = ArtifactIdentity(
        artifact_kind="zone_lookup",
        filename="taxi_zone_lookup.csv",
        logical_url="https://example.invalid/taxi_zone_lookup.csv",
    )

    result = acquire(
        store,
        identity,
        fetch_https(identity.logical_url),
        validate_zone_lookup,
        run_id="run-1",
    )

    assert result.state == "published"
    assert result.manifest["transport"]["source_etag"] == '"zone-etag"'
    assert result.manifest["content_sha256"] != '"zone-etag"'
    assert result.manifest["content_profile"]["rows"] == 2


def test_https_download_cap_rejects_large_declared_objects(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    response = FakeResponse(b"x" * 10, {"Content-Length": "999999999"})
    monkeypatch.setattr(landing_module.urllib.request, "urlopen", lambda *_, **__: response)
    store = LandingStore(tmp_path / "landing")
    identity = ArtifactIdentity(
        artifact_kind="zone_lookup",
        filename="taxi_zone_lookup.csv",
        logical_url="https://example.invalid/taxi_zone_lookup.csv",
    )

    with pytest.raises(ArtifactRejected, match="exceeds cap"):
        acquire(
            store,
            identity,
            fetch_https(identity.logical_url, max_bytes=1024),
            validate_zone_lookup,
            run_id="run-1",
        )

    assert list(store.incoming_root.iterdir()) == []


def test_https_download_cap_rejects_undeclared_overrun(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    response = FakeResponse(b"x" * 4096, {})
    monkeypatch.setattr(landing_module.urllib.request, "urlopen", lambda *_, **__: response)
    store = LandingStore(tmp_path / "landing")
    identity = ArtifactIdentity(
        artifact_kind="zone_lookup",
        filename="taxi_zone_lookup.csv",
        logical_url="https://example.invalid/taxi_zone_lookup.csv",
    )

    with pytest.raises(ArtifactRejected, match="download exceeded cap"):
        acquire(
            store,
            identity,
            fetch_https(identity.logical_url, max_bytes=64),
            validate_zone_lookup,
            run_id="run-1",
        )


def test_zone_lookup_contract_rules(tmp_path: Path) -> None:
    good = tmp_path / "zones.csv"
    good.write_text(ZONE_CSV, encoding="utf-8")
    assert validate_zone_lookup(good)["rows"] == 2

    missing = tmp_path / "missing.csv"
    missing.write_text("LocationID,Zone\n1,Newark Airport\n", encoding="utf-8")
    with pytest.raises(ArtifactRejected, match="missing required columns"):
        validate_zone_lookup(missing)

    duplicated = tmp_path / "duplicated.csv"
    duplicated.write_text(ZONE_CSV + "1,EWR,Newark Airport,EWR\n", encoding="utf-8")
    with pytest.raises(ArtifactRejected, match="duplicate"):
        validate_zone_lookup(duplicated)


def test_parquet_without_rows_is_rejected(tmp_path: Path) -> None:
    empty = write_parquet(tmp_path / "empty.parquet", 0)

    with pytest.raises(ArtifactRejected, match="no rows"):
        validate_parquet(empty)


def test_land_command_runs_without_spark(tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    """Acquisition must stay usable outside a spark-submit application."""
    from fareline.m1 import cli

    source = tmp_path / "source"
    write_parquet(source / "yellow_tripdata_2024-01.parquet", 12)
    argv = [
        "--landing-root",
        str(tmp_path / "landing"),
        "--source-dir",
        str(source),
        "--service",
        "yellow",
        "--skip-zone-lookup",
        "land",
    ]

    assert cli.main(argv) == 0
    published = json.loads(capsys.readouterr().out)
    assert published["yellow"]["state"] == "published"

    assert cli.main(argv) == 0
    assert json.loads(capsys.readouterr().out)["yellow"]["state"] == "replayed"
    assert "pyspark" not in sys.modules


def test_local_trip_path_refuses_complete_object_claim(tmp_path: Path) -> None:
    from fareline.m1 import sources

    with pytest.raises(ValueError, match="complete_object requires"):
        sources.land_artifacts(
            LandingStore(tmp_path / "landing"),
            source_dir=tmp_path / "source",
            period="2024-01",
            services=("yellow",),
            completeness="complete_object",
            acquire_zone_lookup=False,
            run_id="run-1",
        )


def test_m0_sample_hash_is_a_binding_precondition(tmp_path: Path) -> None:
    from fareline.m1 import sources

    source_dir = tmp_path / "source"
    write_parquet(source_dir / "yellow_tripdata_2024-01.parquet", 5)
    evidence = tmp_path / "m0.json"
    evidence.write_text(
        json.dumps(
            {"samples": [{"service": "yellow", "year": 2024, "month": 1, "sha256": "0" * 64}]}
        ),
        encoding="utf-8",
    )

    with pytest.raises(ArtifactRejected, match="expected SHA-256"):
        sources.land_artifacts(
            LandingStore(tmp_path / "landing"),
            source_dir=source_dir,
            period="2024-01",
            services=("yellow",),
            completeness="bounded_sample",
            acquire_zone_lookup=False,
            run_id="run-1",
            m0_evidence_path=evidence,
        )
