"""Deterministic selection of the current source-file version, and its ledger.

Selection is a pure function of the landing manifests and the contract: the
active version of a logical artifact is the newest manifest the contract
accepts. That matters for correctness, not only for tidiness. If a rejected
newer file merely stopped a run, an incremental table would keep rows a full
rebuild would never produce, and the two would stop being comparable.

The ledger records what each run decided. It is append-only and is never read
back as an input to the build.

Visibility is a second, separate mechanism. An immutable marker states that one
derivation is complete; a single dataset-wide boundary states which derivation
context readers are currently on. Adding markers never moves the boundary, so a
newly landed lookup or a revised contract cannot change what readers see until a
run has covered every artifact the previous boundary published.
"""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from collections.abc import Sequence
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
BOUNDARY_SCHEMA_VERSION = 1
LAYOUT_SCHEMA_VERSION = 1

ZONE_DIMENSION = "zone_lookup_version_id"
CONTRACT_DIMENSION = "contract_fingerprint"


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


class PublicationMarkerUnreadable(RuntimeError):
    """A publication marker exists but cannot be parsed."""


class PublicationMarkerConflict(RuntimeError):
    """A publication marker exists and describes a different derivation."""


class PublicationCoverageError(RuntimeError):
    """A context change would leave previously published artifacts unpublished."""


class PublicationLayoutError(RuntimeError):
    """The warehouse was written by a layout this code cannot read."""


@dataclass(frozen=True)
class KnownArtifact:
    """One logical artifact the publication catalog has heard of."""

    logical_id: str
    service: str
    period: str


@dataclass(frozen=True)
class PublicationBoundary:
    """The derivation context readers are on, for the whole dataset.

    The zone lookup version is dataset-wide because analytical products join
    across services and must not mix two versions of the same dimension. The
    contract fingerprint is per service, because revising the Yellow contract is
    no reason to re-derive HVFHV.
    """

    zone_lookup_version_id: str
    contract_fingerprints: tuple[tuple[str, str], ...]

    def fingerprint(self, service: str) -> str | None:
        return dict(self.contract_fingerprints).get(service)

    def as_document(self) -> dict[str, Any]:
        return {
            "schema_version": BOUNDARY_SCHEMA_VERSION,
            "zone_lookup_version_id": self.zone_lookup_version_id,
            "contract_fingerprints": dict(self.contract_fingerprints),
        }

    @classmethod
    def from_document(cls, document: dict[str, Any]) -> PublicationBoundary:
        return cls(
            zone_lookup_version_id=document["zone_lookup_version_id"],
            contract_fingerprints=tuple(sorted(document["contract_fingerprints"].items())),
        )


@dataclass(frozen=True)
class PublicationTransition:
    """What installing a target boundary would change, and what it still needs."""

    previous: PublicationBoundary | None
    target: PublicationBoundary
    changed: tuple[str, ...]
    required: tuple[str, ...]
    missing: tuple[str, ...]

    @property
    def is_migration(self) -> bool:
        return self.previous is not None and bool(self.changed)

    @property
    def complete(self) -> bool:
        return not self.missing

    def as_document(self) -> dict[str, Any]:
        return {
            "previous_context": None if self.previous is None else self.previous.as_document(),
            "target_context": self.target.as_document(),
            "changed_dimensions": list(self.changed),
            "is_migration": self.is_migration,
            "required_logical_ids": list(self.required),
            "missing_logical_ids": list(self.missing),
            "complete": self.complete,
        }


def changed_dimensions(
    previous: PublicationBoundary | None, target: PublicationBoundary
) -> tuple[str, ...]:
    """Name every context dimension the target boundary would move."""
    if previous is None:
        return ()
    changed: list[str] = []
    if previous.zone_lookup_version_id != target.zone_lookup_version_id:
        changed.append(ZONE_DIMENSION)
    for service, fingerprint in target.contract_fingerprints:
        if previous.fingerprint(service) != fingerprint:
            changed.append(f"{CONTRACT_DIMENSION}:{service}")
    return tuple(sorted(changed))


def check_layout(catalog: PublicationCatalog) -> None:
    """Refuse a warehouse not identified as the per-contract table layout.

    The layout marker is separate from the publication boundary. It is installed
    before the first derived write, so a crash after an artifact marker but
    before the first boundary remains distinguishable from a legacy M2
    warehouse and can be replayed safely.
    """
    layout = catalog.layout()
    if layout is None:
        if not catalog.known_artifacts() and catalog.boundary() is None:
            return
        raise PublicationLayoutError(
            f"{catalog.warehouse_root} contains publication state but no supported layout marker; "
            "it predates recoverable per-contract tables, so build into a new warehouse root"
        )
    expected = {
        "schema_version": LAYOUT_SCHEMA_VERSION,
        "table_layout": "contract-fingerprint",
    }
    if layout != expected:
        raise PublicationLayoutError(
            f"{catalog.layout_path} describes an unsupported publication layout"
        )


def prepare_layout(catalog: PublicationCatalog) -> str:
    """Install the layout identity before the first derived write."""
    check_layout(catalog)
    if catalog.layout() is not None:
        return "unchanged"
    return catalog.install_layout()


def plan_transition(
    catalog: PublicationCatalog,
    *,
    target: PublicationBoundary,
    covered: set[str],
) -> PublicationTransition:
    """Decide whether the target boundary may be installed, and what is missing.

    A dimension change affects the artifacts it can retract: a new zone lookup
    version affects every service, a contract revision only its own. Every
    affected artifact that the previous boundary published must be published
    again under the target context before the boundary may move.
    """
    previous = catalog.boundary()
    changed = changed_dimensions(previous, target)
    if previous is None or not changed:
        return PublicationTransition(previous, target, changed, (), ())

    zone_changed = ZONE_DIMENSION in changed
    affected = {
        item.split(":", 1)[1] for item in changed if item.startswith(f"{CONTRACT_DIMENSION}:")
    }
    required = [
        artifact.logical_id
        for artifact in catalog.known_artifacts()
        if (zone_changed or artifact.service in affected)
        and catalog.has_marker(
            artifact.logical_id,
            contract_fingerprint=previous.fingerprint(artifact.service),
            zone_lookup_version_id=previous.zone_lookup_version_id,
        )
    ]
    missing = tuple(sorted(item for item in required if item not in covered))
    return PublicationTransition(previous, target, changed, tuple(sorted(required)), missing)


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


def incident_id(components: Sequence[str | None]) -> str:
    """Identify one incident from the components that distinguish it.

    The components are JSON-encoded before hashing, so no combination of values
    can produce the identifier of a different combination. Spark builds the same
    encoding with ``to_json`` over an array for row-scope incidents.
    """
    payload = json.dumps(list(components), separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def _read_document(path: Path) -> dict[str, Any]:
    """Read one catalog document, naming the file when it cannot be parsed."""
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise PublicationMarkerUnreadable(f"{path.name} is not readable JSON: {error}") from error


def _install(path: Path, payload: bytes, *, description: str, overwrite: bool = False) -> str:
    """Put exact bytes at ``path`` through one same-directory rename.

    The rename is the visibility boundary: a reader observes the previous
    content or the new one, never a partial file.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_file():
        if path.read_bytes() == payload:
            return "unchanged"
        if not overwrite:
            # Parsing first separates a damaged file from a genuine
            # disagreement about what the derivation is.
            _read_document(path)
            raise PublicationMarkerConflict(
                f"{description} {path.name} already describes a different derivation"
            )

    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.part")
    try:
        with temporary.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)
    return "installed" if overwrite else "published"


def _fsync_directory(directory: Path) -> None:
    """Persist the rename itself where the platform supports it."""
    try:
        handle = os.open(directory, os.O_RDONLY)
    except OSError:  # pragma: no cover - Windows cannot open a directory this way
        return
    try:
        os.fsync(handle)
    except OSError:  # pragma: no cover - some filesystems refuse to sync a directory
        pass
    finally:
        os.close(handle)


class PublicationCatalog:
    """Immutable completion markers forming the published view of derived data.

    A marker is created only after every Delta write required for its candidate
    has completed. Readers select derivations through these markers rather than
    by scanning every physical row in the history tables. A crash can therefore
    leave repairable, invisible data behind, but cannot expose a half-published
    version.

    ``state`` records what the candidate was when its marker was written, and is
    never rewritten afterwards. Two markers of one artifact can therefore both
    read ``active``. Only ``visible_derivations`` answers what is active now;
    reading the marker files directly does not.
    """

    def __init__(self, warehouse_root: Path | str) -> None:
        self.warehouse_root = Path(warehouse_root)
        self.root = self.warehouse_root / "publication_catalog"
        self.boundary_path = self.warehouse_root / "publication_boundary.json"
        self.layout_path = self.warehouse_root / "publication_layout.json"

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
        # Concurrent writers of the same deterministic document are harmless.
        return _install(target, payload, description="publication marker"), document

    def boundary(self) -> PublicationBoundary | None:
        """The context readers are currently on, or ``None`` before the first run."""
        if not self.boundary_path.is_file():
            return None
        return PublicationBoundary.from_document(_read_document(self.boundary_path))

    def layout(self) -> dict[str, Any] | None:
        """The physical publication layout, independent of reader visibility."""
        if not self.layout_path.is_file():
            return None
        return _read_document(self.layout_path)

    def install_layout(self) -> str:
        """Identify this warehouse before any per-contract data is written."""
        document = {
            "schema_version": LAYOUT_SCHEMA_VERSION,
            "table_layout": "contract-fingerprint",
        }
        payload = json.dumps(document, indent=2, sort_keys=True).encode() + b"\n"
        return _install(self.layout_path, payload, description="publication layout")

    def install_boundary(self, boundary: PublicationBoundary) -> str:
        """Move every reader to a new context in one atomic replacement."""
        payload = json.dumps(boundary.as_document(), indent=2, sort_keys=True).encode() + b"\n"
        return _install(
            self.boundary_path, payload, description="publication boundary", overwrite=True
        )

    def known_artifacts(self) -> tuple[KnownArtifact, ...]:
        """Every logical artifact with at least one marker, whatever its context.

        Coverage has to be judged against the whole catalog. Enumerating only the
        artifacts a run asked for is what let a context change retract the rest
        without anything noticing.
        """
        if not self.root.is_dir():
            return ()
        found: dict[str, KnownArtifact] = {}
        for directory in sorted(self.root.iterdir()):
            for path in sorted(directory.glob("*.json")):
                entry = _read_document(path)
                found[entry["logical_id"]] = KnownArtifact(
                    entry["logical_id"], entry["service"], entry["period"]
                )
                break
        return tuple(sorted(found.values(), key=lambda item: (item.service, item.period)))

    def has_marker(
        self,
        logical_id: str,
        *,
        contract_fingerprint: str | None,
        zone_lookup_version_id: str,
    ) -> bool:
        if contract_fingerprint is None:
            return False
        return any(
            entry["contract_fingerprint"] == contract_fingerprint
            and entry["zone_lookup_version_id"] == zone_lookup_version_id
            for entry in self.entries(logical_id)
        )

    def covered(self, boundary: PublicationBoundary) -> set[str]:
        """Logical artifacts that already have a marker under the given context."""
        return {
            artifact.logical_id
            for artifact in self.known_artifacts()
            if self.has_marker(
                artifact.logical_id,
                contract_fingerprint=boundary.fingerprint(artifact.service),
                zone_lookup_version_id=boundary.zone_lookup_version_id,
            )
        }

    def entries(self, logical_id: str) -> list[dict[str, Any]]:
        directory = self.root / self._logical_key(logical_id)
        if not directory.is_dir():
            return []
        entries = [_read_document(path) for path in sorted(directory.glob("*.json"))]
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
