from __future__ import annotations

import json
from pathlib import Path

import pytest

from fareline.m1 import landing
from fareline.m1.sources import zone_identity
from fareline.m2 import zones

LOOKUP = (
    "LocationID,Borough,Zone,service_zone\n"
    "1,EWR,Newark Airport,EWR\n"
    "132,Queens,JFK Airport,Airports\n"
    "264,Unknown,NV,\n"
    "265,Unknown,NA,\n"
)


def land_lookup(root: Path, text: str, run_id: str) -> landing.AcquisitionResult:
    source = root / f"{run_id}.csv"
    source.write_text(text, encoding="utf-8")
    return landing.acquire(
        landing.LandingStore(root / "landing"),
        zone_identity(),
        landing.fetch_local_file(source),
        landing.validate_zone_lookup,
        run_id=run_id,
    )


def test_a_lookup_becomes_a_dimension_ordered_by_key(tmp_path: Path) -> None:
    land_lookup(tmp_path, LOOKUP, "run-1")

    dimension = zones.load(landing.LandingStore(tmp_path / "landing"))

    assert [row[0] for row in dimension.rows] == [1, 132, 264, 265]
    assert dimension.rows[1] == (132, "Queens", "JFK Airport", "Airports")
    # The published sentinels are real lookup rows, so they resolve normally.
    assert 264 in dimension.location_ids and 265 in dimension.location_ids


def test_the_joined_version_is_recorded_and_can_be_pinned(tmp_path: Path) -> None:
    first = land_lookup(tmp_path, LOOKUP, "run-1")
    second = land_lookup(tmp_path, LOOKUP + "300,Bronx,New Zone,Boro Zone\n", "run-2")
    store = landing.LandingStore(tmp_path / "landing")

    latest = zones.load(store)
    pinned = zones.load(store, first.version_id)

    assert latest.version_id == second.version_id
    assert len(latest.rows) == 5
    assert pinned.version_id == first.version_id
    assert len(pinned.rows) == 4
    assert pinned.as_document()["zone_lookup_version_id"] == first.version_id


def test_pinning_a_version_that_was_never_landed_fails(tmp_path: Path) -> None:
    land_lookup(tmp_path, LOOKUP, "run-1")
    store = landing.LandingStore(tmp_path / "landing")

    with pytest.raises(zones.ZoneLookupUnavailable, match="is not landed"):
        zones.load(store, "0" * 64)


def test_no_landed_lookup_is_an_explicit_failure(tmp_path: Path) -> None:
    with pytest.raises(zones.ZoneLookupUnavailable, match="no taxi zone lookup"):
        zones.load(landing.LandingStore(tmp_path / "landing"))


def test_a_missing_landed_file_is_not_silently_treated_as_empty(tmp_path: Path) -> None:
    result = land_lookup(tmp_path, LOOKUP, "run-1")
    result.artifact_path.unlink()

    with pytest.raises(zones.ZoneLookupUnavailable, match="landed zone lookup file is missing"):
        zones.load(landing.LandingStore(tmp_path / "landing"))


def test_a_repeated_location_id_cannot_become_a_dimension(tmp_path: Path) -> None:
    path = tmp_path / "zones.csv"
    path.write_text(LOOKUP + "1,EWR,Newark Airport,EWR\n", encoding="utf-8")

    with pytest.raises(zones.ZoneLookupUnavailable, match="repeats LocationID"):
        zones.read_zone_csv(path)


def test_a_non_integer_key_cannot_become_a_dimension(tmp_path: Path) -> None:
    path = tmp_path / "zones.csv"
    path.write_text(LOOKUP + "abc,Bronx,Somewhere,Boro Zone\n", encoding="utf-8")

    with pytest.raises(zones.ZoneLookupUnavailable, match="non-integer LocationID"):
        zones.read_zone_csv(path)


def test_the_landed_manifest_still_describes_the_dimension(tmp_path: Path) -> None:
    result = land_lookup(tmp_path, LOOKUP, "run-1")
    store = landing.LandingStore(tmp_path / "landing")

    dimension = zones.load(store)
    manifest = json.loads(
        store.manifest_path(zone_identity(), result.version_id).read_text(encoding="utf-8")
    )

    assert dimension.content_sha256 == manifest["content_sha256"]
    assert dimension.as_document()["zone_rows"] == manifest["content_profile"]["rows"]
