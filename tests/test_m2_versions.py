from __future__ import annotations

import json
from pathlib import Path

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
