"""Deterministic selection of the current source-file version, and its ledger.

Selection is a pure function of the landing manifests and the contract: the
active version of a logical artifact is the newest manifest the contract
accepts. That matters for correctness, not only for tidiness. If a rejected
newer file merely stopped a run, an incremental table would keep rows a full
rebuild would never produce, and the two would stop being comparable.

The ledger records what each run decided. It is append-only and is never read
back as an input to the build.
"""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ACTIVE = "active"
SUPERSEDED = "superseded"
REJECTED = "rejected"

ACTIVATED_EVENT = "activated"
SUPERSEDED_EVENT = "superseded"
REJECTED_EVENT = "rejected"

PUBLICATION_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class CandidateVersion:
    version_id: str
    published_at_utc: str
    state: str
    violations: tuple[str, ...]

    def as_document(self) -> dict[str, Any]:
        return {
            "version_id": self.version_id,
            "published_at_utc": self.published_at_utc,
            "state": self.state,
            "violations": list(self.violations),
        }


@dataclass(frozen=True)
class VersionDecision:
    logical_id: str
    service: str
    period: str
    active_version_id: str | None
    candidates: tuple[CandidateVersion, ...]

    def as_document(self) -> dict[str, Any]:
        return {
            "logical_id": self.logical_id,
            "service": self.service,
            "period": self.period,
            "active_version_id": self.active_version_id,
            "candidates": [item.as_document() for item in self.candidates],
        }


def sort_key(manifest: dict[str, Any]) -> tuple[str, str]:
    # Publication time orders the history; the version id breaks ties so two
    # manifests written in the same clock tick still order identically on every
    # host and in every run.
    return (manifest["published_at_utc"], manifest["version_id"])


def decide(
    logical_id: str,
    service: str,
    period: str,
    manifests: list[dict[str, Any]],
    accepted: dict[str, tuple[str, ...]],
) -> VersionDecision:
    """Choose the active version from every landed manifest of one artifact.

    ``accepted`` maps a version id to its blocking schema violations; an empty
    tuple means the contract accepts that version.
    """
    ordered = sorted(manifests, key=sort_key, reverse=True)
    candidates: list[CandidateVersion] = []
    active: str | None = None
    for manifest in ordered:
        version = manifest["version_id"]
        violations = accepted.get(version, ())
        if violations:
            state = REJECTED
        elif active is None:
            state = ACTIVE
            active = version
        else:
            state = SUPERSEDED
        candidates.append(
            CandidateVersion(version, manifest["published_at_utc"], state, tuple(violations))
        )
    return VersionDecision(logical_id, service, period, active, tuple(candidates))


class VersionLedger:
    """Append-only publication and supersession record for source versions."""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)

    def events(self) -> list[dict[str, Any]]:
        if not self.path.is_file():
            return []
        text = self.path.read_text(encoding="utf-8")
        return [json.loads(line) for line in text.splitlines() if line.strip()]

    def append(self, events: list[dict[str, Any]]) -> None:
        if not events:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            for event in events:
                handle.write(json.dumps(event, sort_keys=True) + "\n")

    def state(self) -> dict[str, str | None]:
        """Replay the ledger into the version each logical artifact ended on."""
        state: dict[str, str | None] = {}
        for event in self.events():
            logical_id, version = event["logical_id"], event["version_id"]
            if event["action"] == ACTIVATED_EVENT:
                state[logical_id] = version
            elif (
                event["action"] in {SUPERSEDED_EVENT, REJECTED_EVENT}
                and state.get(logical_id) == version
            ):
                state[logical_id] = None
            else:
                state.setdefault(logical_id, None)
        return state


def reconcile(
    ledger: VersionLedger,
    decisions: list[VersionDecision],
    *,
    run_id: str,
    contract_versions: dict[str, str],
    now: str | None = None,
) -> list[dict[str, Any]]:
    """Append only what changed, so replaying an unchanged input writes nothing."""
    latest = {(event["logical_id"], event["version_id"]): event for event in ledger.events()}
    timestamp = now or datetime.now(timezone.utc).isoformat()
    events: list[dict[str, Any]] = []
    for decision in decisions:
        for candidate in decision.candidates:
            action = {
                ACTIVE: ACTIVATED_EVENT,
                SUPERSEDED: SUPERSEDED_EVENT,
                REJECTED: REJECTED_EVENT,
            }[candidate.state]
            event = {
                "event_at_utc": timestamp,
                "run_id": run_id,
                "logical_id": decision.logical_id,
                "service": decision.service,
                "period": decision.period,
                "version_id": candidate.version_id,
                "action": action,
                "contract_version": contract_versions.get(decision.service),
            }
            if action == SUPERSEDED_EVENT:
                event["superseded_by_version_id"] = decision.active_version_id
            if action == REJECTED_EVENT:
                event["violations"] = list(candidate.violations)
            previous = latest.get((decision.logical_id, candidate.version_id))
            comparable = {
                key: value for key, value in event.items() if key not in {"event_at_utc", "run_id"}
            }
            previous_comparable = {
                key: value
                for key, value in (previous or {}).items()
                if key not in {"event_at_utc", "run_id"}
            }
            if previous_comparable == comparable:
                continue
            events.append(event)
            latest[(decision.logical_id, candidate.version_id)] = event
    ledger.append(events)
    return events


def derivation_id(
    source_version_id: str, contract_fingerprint: str, zone_lookup_version_id: str
) -> str:
    """Identify one deterministic derivation of a landed source version."""
    payload = "\n".join((source_version_id, contract_fingerprint, zone_lookup_version_id)).encode()
    return hashlib.sha256(payload).hexdigest()


class PublicationCatalog:
    """Immutable completion markers forming the published view of derived data.

    A marker is created only after every Delta write required for its candidate
    has completed. Readers select derivations through these markers rather than
    by scanning every physical row in the history tables. A crash can therefore
    leave repairable, invisible data behind, but cannot expose a half-published
    version.
    """

    def __init__(self, warehouse_root: Path | str) -> None:
        self.root = Path(warehouse_root) / "publication_catalog"

    @staticmethod
    def _logical_key(logical_id: str) -> str:
        return hashlib.sha256(logical_id.encode()).hexdigest()

    def marker_path(self, logical_id: str, derived_id: str) -> Path:
        return self.root / self._logical_key(logical_id) / f"{derived_id}.json"

    def publish(
        self,
        decision: VersionDecision,
        candidate: CandidateVersion,
        *,
        contract_version: str,
        contract_fingerprint: str,
        zone_lookup_version_id: str,
    ) -> tuple[str, dict[str, Any]]:
        """Atomically expose one fully written active or rejected derivation."""
        if candidate.state not in {ACTIVE, REJECTED}:
            raise ValueError(f"cannot publish candidate state {candidate.state}")
        derived_id = derivation_id(
            candidate.version_id, contract_fingerprint, zone_lookup_version_id
        )
        document = {
            "schema_version": PUBLICATION_SCHEMA_VERSION,
            "logical_id": decision.logical_id,
            "service": decision.service,
            "period": decision.period,
            "source_version_id": candidate.version_id,
            "source_published_at_utc": candidate.published_at_utc,
            "state": candidate.state,
            "violations": list(candidate.violations),
            "contract_version": contract_version,
            "contract_fingerprint": contract_fingerprint,
            "zone_lookup_version_id": zone_lookup_version_id,
            "derivation_id": derived_id,
        }
        payload = json.dumps(document, indent=2, sort_keys=True).encode() + b"\n"
        target = self.marker_path(decision.logical_id, derived_id)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.is_file():
            if target.read_bytes() != payload:
                raise RuntimeError(f"publication marker conflicts with {target.name}")
            return "unchanged", document

        temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.part")
        try:
            with temporary.open("xb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            # Same-directory replacement is the visibility boundary. Concurrent
            # writers of the same deterministic document are harmless.
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)
        return "published", document

    def entries(self, logical_id: str) -> list[dict[str, Any]]:
        directory = self.root / self._logical_key(logical_id)
        if not directory.is_dir():
            return []
        entries = [
            json.loads(path.read_text(encoding="utf-8"))
            for path in sorted(directory.glob("*.json"))
        ]
        for entry in entries:
            if entry.get("logical_id") != logical_id:
                raise RuntimeError("publication catalog logical-id hash collision")
        return entries

    def visible_derivations(
        self,
        logical_id: str,
        *,
        contract_fingerprint: str,
        zone_lookup_version_id: str,
    ) -> dict[str, set[str]]:
        """Return derivations visible in each table for one logical artifact."""
        entries = [
            entry
            for entry in self.entries(logical_id)
            if entry["contract_fingerprint"] == contract_fingerprint
            and entry["zone_lookup_version_id"] == zone_lookup_version_id
        ]
        active = [entry for entry in entries if entry["state"] == ACTIVE]
        selected = max(
            active,
            key=lambda entry: (
                entry["source_published_at_utc"],
                entry["source_version_id"],
            ),
            default=None,
        )
        active_ids = set() if selected is None else {selected["derivation_id"]}
        rejected_ids = {entry["derivation_id"] for entry in entries if entry["state"] == REJECTED}
        return {
            "contracted": active_ids,
            "quarantine": active_ids,
            "incidents": active_ids | rejected_ids,
        }
