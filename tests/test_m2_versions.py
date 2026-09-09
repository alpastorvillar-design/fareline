from __future__ import annotations

import json
from pathlib import Path

import pytest

from fareline.m2 import versions

LOGICAL_ID = "trip_records:bounded_sample:yellow:2024-01"


def manifest(version_id: str, published_at: str) -> dict[str, str]:
    return {"version_id": version_id, "published_at_utc": published_at}


def decide(manifests: list[dict[str, str]], accepted: dict[str, tuple[str, ...]] | None = None):
    return versions.decide(LOGICAL_ID, "yellow", "2024-01", manifests, accepted or {})


def test_the_newest_accepted_manifest_becomes_active() -> None:
    decision = decide(
        [
            manifest("aaa", "2026-01-01T00:00:00+00:00"),
            manifest("bbb", "2026-02-01T00:00:00+00:00"),
        ]
    )

    assert decision.active_version_id == "bbb"
    states = {item.version_id: item.state for item in decision.candidates}
    assert states == {"bbb": versions.ACTIVE, "aaa": versions.SUPERSEDED}


def test_selection_does_not_depend_on_the_order_manifests_arrive_in() -> None:
    ordered = [
        manifest("aaa", "2026-01-01T00:00:00+00:00"),
        manifest("bbb", "2026-02-01T00:00:00+00:00"),
        manifest("ccc", "2026-03-01T00:00:00+00:00"),
    ]

    forward = decide(list(ordered))
    backward = decide(list(reversed(ordered)))

    assert forward.as_document() == backward.as_document()


def test_a_publication_time_tie_is_broken_by_the_version_id() -> None:
    same_moment = "2026-02-01T00:00:00+00:00"

    decision = decide([manifest("aaa", same_moment), manifest("zzz", same_moment)])

    assert decision.active_version_id == "zzz"


def test_a_rejected_newest_version_hands_the_role_back_to_the_previous_one() -> None:
    # An incremental run that simply stopped here would keep rows that a full
    # rebuild never produces, so the older accepted version stays active.
    decision = decide(
        [
            manifest("aaa", "2026-01-01T00:00:00+00:00"),
            manifest("bbb", "2026-02-01T00:00:00+00:00"),
        ],
        {"bbb": ("narrowing_type_drift:payment_type",)},
    )

    assert decision.active_version_id == "aaa"
    states = {item.version_id: item.state for item in decision.candidates}
    assert states == {"bbb": versions.REJECTED, "aaa": versions.ACTIVE}


def test_an_artifact_whose_versions_are_all_rejected_has_no_active_version() -> None:
    decision = decide(
        [manifest("aaa", "2026-01-01T00:00:00+00:00")],
        {"aaa": ("missing_required_column:total_amount",)},
    )

    assert decision.active_version_id is None
    assert decision.candidates[0].state == versions.REJECTED


def test_the_ledger_records_activation_and_supersession_once(tmp_path: Path) -> None:
    ledger = versions.VersionLedger(tmp_path / "version_ledger.jsonl")
    first = decide([manifest("aaa", "2026-01-01T00:00:00+00:00")])

    created = versions.reconcile(
        ledger, [first], run_id="run-1", contract_versions={"yellow": "yellow/v1"}
    )
    repeated = versions.reconcile(
        ledger, [first], run_id="run-2", contract_versions={"yellow": "yellow/v1"}
    )

    assert [item["action"] for item in created] == [versions.ACTIVATED_EVENT]
    assert repeated == []
    assert ledger.state() == {LOGICAL_ID: "aaa"}


def test_a_new_version_supersedes_without_rewriting_history(tmp_path: Path) -> None:
    ledger = versions.VersionLedger(tmp_path / "version_ledger.jsonl")
    versions.reconcile(
        ledger,
        [decide([manifest("aaa", "2026-01-01T00:00:00+00:00")])],
        run_id="run-1",
        contract_versions={"yellow": "yellow/v1"},
    )

    second = versions.reconcile(
        ledger,
        [
            decide(
                [
                    manifest("aaa", "2026-01-01T00:00:00+00:00"),
                    manifest("bbb", "2026-02-01T00:00:00+00:00"),
                ]
            )
        ],
        run_id="run-2",
        contract_versions={"yellow": "yellow/v1"},
    )

    actions = {item["version_id"]: item["action"] for item in second}
    assert actions == {"bbb": versions.ACTIVATED_EVENT, "aaa": versions.SUPERSEDED_EVENT}
    superseded = next(item for item in second if item["version_id"] == "aaa")
    assert superseded["superseded_by_version_id"] == "bbb"
    # The first activation is still on disk: the ledger only ever grows.
    lines = (tmp_path / "version_ledger.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 3
    assert json.loads(lines[0])["action"] == versions.ACTIVATED_EVENT
    assert ledger.state() == {LOGICAL_ID: "bbb"}


def test_a_rejection_records_its_violations(tmp_path: Path) -> None:
    ledger = versions.VersionLedger(tmp_path / "version_ledger.jsonl")

    events = versions.reconcile(
        ledger,
        [
            decide(
                [manifest("aaa", "2026-01-01T00:00:00+00:00")],
                {"aaa": ("incompatible_type_drift:wav_match_flag",)},
            )
        ],
        run_id="run-1",
        contract_versions={"yellow": "yellow/v1"},
    )

    assert events[0]["action"] == versions.REJECTED_EVENT
    assert events[0]["violations"] == ["incompatible_type_drift:wav_match_flag"]
    assert ledger.state() == {LOGICAL_ID: None}


def test_the_ledger_records_a_real_reactivation_after_a_rejection(tmp_path: Path) -> None:
    ledger = versions.VersionLedger(tmp_path / "version_ledger.jsonl")
    accepted = decide([manifest("aaa", "2026-01-01T00:00:00+00:00")])
    rejected = decide(
        [manifest("aaa", "2026-01-01T00:00:00+00:00")],
        {"aaa": ("incompatible_type_drift:wav_match_flag",)},
    )

    versions.reconcile(
        ledger, [accepted], run_id="run-1", contract_versions={"yellow": "yellow/v1"}
    )
    versions.reconcile(
        ledger, [rejected], run_id="run-2", contract_versions={"yellow": "yellow/v1"}
    )
    reactivated = versions.reconcile(
        ledger, [accepted], run_id="run-3", contract_versions={"yellow": "yellow/v1"}
    )

    assert [event["action"] for event in reactivated] == [versions.ACTIVATED_EVENT]
    assert ledger.state() == {LOGICAL_ID: "aaa"}


def test_publication_marker_is_deterministic_atomic_and_selects_latest(tmp_path: Path) -> None:
    catalog = versions.PublicationCatalog(tmp_path / "warehouse")
    first = decide([manifest("aaa", "2026-01-01T00:00:00+00:00")])
    second = decide(
        [
            manifest("aaa", "2026-01-01T00:00:00+00:00"),
            manifest("bbb", "2026-02-01T00:00:00+00:00"),
        ]
    )
    fingerprint = "f" * 64
    zone = "z" * 64

    action, first_marker = catalog.publish(
        first,
        first.candidates[0],
        contract_version="yellow/v1",
        contract_fingerprint=fingerprint,
        zone_lookup_version_id=zone,
    )
    replay_action, replay_marker = catalog.publish(
        first,
        first.candidates[0],
        contract_version="yellow/v1",
        contract_fingerprint=fingerprint,
        zone_lookup_version_id=zone,
    )
    newest = next(candidate for candidate in second.candidates if candidate.version_id == "bbb")
    catalog.publish(
        second,
        newest,
        contract_version="yellow/v1",
        contract_fingerprint=fingerprint,
        zone_lookup_version_id=zone,
    )

    visible = catalog.visible_derivations(
        LOGICAL_ID,
        contract_fingerprint=fingerprint,
        zone_lookup_version_id=zone,
    )
    assert action == "published"
    assert replay_action == "unchanged"
    assert replay_marker == first_marker
    assert visible["contracted"] == {
        newest_marker := versions.derivation_id("bbb", fingerprint, zone)
    }
    assert visible["quarantine"] == {newest_marker}
    assert visible["incidents"] == {newest_marker}
    assert not list((tmp_path / "warehouse" / "publication_catalog").rglob("*.part"))


def test_a_rejected_marker_is_visible_only_to_incidents(tmp_path: Path) -> None:
    catalog = versions.PublicationCatalog(tmp_path / "warehouse")
    decision = decide(
        [manifest("bad", "2026-01-01T00:00:00+00:00")],
        {"bad": ("incompatible_type_drift:wav_match_flag",)},
    )
    fingerprint = "f" * 64
    zone = "z" * 64
    catalog.publish(
        decision,
        decision.candidates[0],
        contract_version="yellow/v1",
        contract_fingerprint=fingerprint,
        zone_lookup_version_id=zone,
    )

    visible = catalog.visible_derivations(
        LOGICAL_ID,
        contract_fingerprint=fingerprint,
        zone_lookup_version_id=zone,
    )
    derived_id = versions.derivation_id("bad", fingerprint, zone)
    assert visible == {
        "contracted": set(),
        "quarantine": set(),
        "incidents": {derived_id},
    }


def boundary(zone: str, **fingerprints: str) -> versions.PublicationBoundary:
    return versions.PublicationBoundary(
        zone_lookup_version_id=zone,
        contract_fingerprints=tuple(sorted(fingerprints.items())),
    )


def publish(
    catalog: versions.PublicationCatalog,
    version_id: str,
    *,
    fingerprint: str,
    zone: str,
    service: str = "yellow",
    period: str = "2024-01",
    logical_id: str | None = None,
) -> None:
    decision = versions.decide(
        logical_id or LOGICAL_ID,
        service,
        period,
        [manifest(version_id, "2026-01-01T00:00:00+00:00")],
        {},
    )
    catalog.publish(
        decision,
        decision.candidates[0],
        contract_version=f"{service}/v1",
        contract_fingerprint=fingerprint,
        zone_lookup_version_id=zone,
    )


def test_the_boundary_survives_a_document_round_trip(tmp_path: Path) -> None:
    catalog = versions.PublicationCatalog(tmp_path / "warehouse")
    target = boundary("z1", yellow="f1", hvfhv="f2")

    catalog.install_boundary(target)

    assert catalog.boundary() == target
    assert catalog.boundary().fingerprint("hvfhv") == "f2"


def test_an_absent_boundary_reads_as_no_published_view(tmp_path: Path) -> None:
    catalog = versions.PublicationCatalog(tmp_path / "warehouse")

    assert catalog.boundary() is None


def test_installing_the_same_boundary_twice_reports_it_unchanged(tmp_path: Path) -> None:
    catalog = versions.PublicationCatalog(tmp_path / "warehouse")
    target = boundary("z1", yellow="f1")

    first = catalog.install_boundary(target)
    second = catalog.install_boundary(target)

    assert (first, second) == ("installed", "unchanged")
    assert not list((tmp_path / "warehouse").rglob("*.part"))


def test_the_catalog_enumerates_every_artifact_it_knows_about(tmp_path: Path) -> None:
    catalog = versions.PublicationCatalog(tmp_path / "warehouse")
    publish(catalog, "aaa", fingerprint="f1", zone="z1")
    publish(
        catalog,
        "bbb",
        fingerprint="f2",
        zone="z1",
        service="hvfhv",
        period="2024-02",
        logical_id="trip_records:bounded_sample:hvfhv:2024-02",
    )

    known = catalog.known_artifacts()

    assert [(item.service, item.period) for item in known] == [
        ("hvfhv", "2024-02"),
        ("yellow", "2024-01"),
    ]


def test_a_first_publication_is_not_a_migration(tmp_path: Path) -> None:
    catalog = versions.PublicationCatalog(tmp_path / "warehouse")
    target = boundary("z1", yellow="f1")

    transition = versions.plan_transition(catalog, target=target, covered=set())

    assert not transition.is_migration
    assert transition.complete
    assert transition.missing == ()


def test_a_new_zone_lookup_version_requires_every_published_artifact(tmp_path: Path) -> None:
    # The condition the M2 review measured: a newer zone lookup silently emptied
    # the published view because no coverage was ever required.
    catalog = versions.PublicationCatalog(tmp_path / "warehouse")
    catalog.install_boundary(boundary("z1", yellow="f1", hvfhv="f2"))
    publish(catalog, "aaa", fingerprint="f1", zone="z1")
    publish(
        catalog,
        "bbb",
        fingerprint="f2",
        zone="z1",
        service="hvfhv",
        period="2024-02",
        logical_id="trip_records:bounded_sample:hvfhv:2024-02",
    )

    transition = versions.plan_transition(
        catalog, target=boundary("z2", yellow="f1", hvfhv="f2"), covered={LOGICAL_ID}
    )

    assert transition.is_migration
    assert transition.changed == ("zone_lookup_version_id",)
    assert transition.missing == ("trip_records:bounded_sample:hvfhv:2024-02",)
    assert not transition.complete


def test_a_contract_revision_requires_only_the_artifacts_of_that_service(tmp_path: Path) -> None:
    catalog = versions.PublicationCatalog(tmp_path / "warehouse")
    catalog.install_boundary(boundary("z1", yellow="f1", hvfhv="f2"))
    publish(catalog, "aaa", fingerprint="f1", zone="z1")
    publish(
        catalog,
        "bbb",
        fingerprint="f2",
        zone="z1",
        service="hvfhv",
        period="2024-02",
        logical_id="trip_records:bounded_sample:hvfhv:2024-02",
    )

    transition = versions.plan_transition(
        catalog, target=boundary("z1", yellow="f9", hvfhv="f2"), covered={LOGICAL_ID}
    )

    assert transition.changed == ("contract_fingerprint:yellow",)
    assert transition.required == (LOGICAL_ID,)
    assert transition.complete


def test_a_migration_that_covers_everything_previously_published_completes(tmp_path: Path) -> None:
    catalog = versions.PublicationCatalog(tmp_path / "warehouse")
    catalog.install_boundary(boundary("z1", yellow="f1"))
    publish(catalog, "aaa", fingerprint="f1", zone="z1")
    publish(catalog, "aaa", fingerprint="f1", zone="z2")

    target = boundary("z2", yellow="f1")
    transition = versions.plan_transition(catalog, target=target, covered=catalog.covered(target))

    assert transition.is_migration
    assert transition.complete
    assert catalog.covered(target) == {LOGICAL_ID}


def test_a_service_added_after_the_first_boundary_needs_no_back_coverage(tmp_path: Path) -> None:
    catalog = versions.PublicationCatalog(tmp_path / "warehouse")
    catalog.install_boundary(boundary("z1", yellow="f1"))
    publish(catalog, "aaa", fingerprint="f1", zone="z1")

    transition = versions.plan_transition(
        catalog, target=boundary("z1", yellow="f1", hvfhv="f2"), covered=set()
    )

    assert transition.changed == ("contract_fingerprint:hvfhv",)
    assert transition.required == ()
    assert transition.complete


def test_the_view_keeps_the_boundary_context_when_a_newer_lookup_is_landed(tmp_path: Path) -> None:
    catalog = versions.PublicationCatalog(tmp_path / "warehouse")
    installed = boundary("z1", yellow="f1")
    catalog.install_boundary(installed)
    publish(catalog, "aaa", fingerprint="f1", zone="z1")

    visible = catalog.visible_derivations(
        LOGICAL_ID,
        contract_fingerprint=installed.fingerprint("yellow"),
        zone_lookup_version_id=installed.zone_lookup_version_id,
    )

    assert visible["contracted"] == {versions.derivation_id("aaa", "f1", "z1")}


def test_a_corrupt_marker_names_the_file_and_says_it_is_unreadable(tmp_path: Path) -> None:
    catalog = versions.PublicationCatalog(tmp_path / "warehouse")
    publish(catalog, "aaa", fingerprint="f1", zone="z1")
    marker = next((tmp_path / "warehouse" / "publication_catalog").rglob("*.json"))
    marker.write_text("{ truncated", encoding="utf-8")

    with pytest.raises(versions.PublicationMarkerUnreadable) as error:
        catalog.entries(LOGICAL_ID)

    assert marker.name in str(error.value)


def test_a_conflicting_marker_is_reported_as_a_conflict_not_as_corruption(tmp_path: Path) -> None:
    catalog = versions.PublicationCatalog(tmp_path / "warehouse")
    publish(catalog, "aaa", fingerprint="f1", zone="z1")
    marker = next((tmp_path / "warehouse" / "publication_catalog").rglob("*.json"))
    document = json.loads(marker.read_text(encoding="utf-8"))
    document["contract_version"] = "yellow/v2"
    marker.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    with pytest.raises(versions.PublicationMarkerConflict) as error:
        publish(catalog, "aaa", fingerprint="f1", zone="z1")

    assert marker.name in str(error.value)


def test_an_incident_id_separates_its_components_unambiguously() -> None:
    # Joining with an empty separator made ("ab", "c") and ("a", "bc") the same
    # incident. The canonical encoding has to keep them apart.
    assert versions.incident_id(["ab", "c"]) != versions.incident_id(["a", "bc"])
    assert versions.incident_id(["a", None]) != versions.incident_id(["a", "null"])
    assert versions.incident_id(["a", "b"]) == versions.incident_id(["a", "b"])


def test_a_catalog_without_a_boundary_but_with_markers_is_refused(tmp_path: Path) -> None:
    # An M2 warehouse stored one table per service. M2.1 stores one per contract
    # fingerprint, so its markers would point at rows that are not where the new
    # layout looks for them.
    catalog = versions.PublicationCatalog(tmp_path / "warehouse")
    publish(catalog, "aaa", fingerprint="f1", zone="z1")

    with pytest.raises(versions.PublicationLayoutError):
        versions.check_layout(catalog)


def test_a_catalog_with_a_boundary_passes_the_layout_check(tmp_path: Path) -> None:
    catalog = versions.PublicationCatalog(tmp_path / "warehouse")
    versions.prepare_layout(catalog)
    catalog.install_boundary(boundary("z1", yellow="f1"))
    publish(catalog, "aaa", fingerprint="f1", zone="z1")

    versions.check_layout(catalog)


def test_an_empty_warehouse_passes_the_layout_check(tmp_path: Path) -> None:
    versions.check_layout(versions.PublicationCatalog(tmp_path / "warehouse"))


def test_an_interrupted_first_publication_remains_recoverable(tmp_path: Path) -> None:
    """Markers written before the first boundary are M2.1 recovery state, not legacy M2."""
    catalog = versions.PublicationCatalog(tmp_path / "warehouse")
    versions.prepare_layout(catalog)
    publish(catalog, "aaa", fingerprint="f1", zone="z1")

    # Simulate a crash after a complete artifact marker but before the
    # dataset-wide publication boundary is installed.
    versions.check_layout(catalog)
    assert catalog.boundary() is None
