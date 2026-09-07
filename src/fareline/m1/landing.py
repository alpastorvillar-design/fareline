"""Versioned landing for bounded Fareline source artifacts.

A landed artifact version is one immutable file plus one immutable manifest.
Identity is the logical artifact plus the SHA-256 of the bytes actually
acquired, so replaying unchanged content is a no-op and changed content creates
a new version without rewriting history.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import shutil
import urllib.request
import uuid
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from fareline import __version__
from fareline.m0 import REQUIRED_ZONE_COLUMNS

CHUNK_BYTES = 1 << 20

# M1 is only allowed to download the small official zone lookup. The cap is a
# hard stop against acquiring a monthly trip Parquet object by accident.
DEFAULT_MAX_DOWNLOAD_BYTES = 4 << 20

USER_AGENT = f"Fareline/{__version__} (+https://github.com/alpastorvillar-design/fareline)"

PUBLISHED = "published"
REPLAYED = "replayed"
REPAIRED = "repaired"
REJECTED = "rejected"


class ArtifactRejected(RuntimeError):
    """An acquired artifact failed a landing precondition and was not published."""


@dataclass(frozen=True)
class ArtifactIdentity:
    """Logical identity of an artifact, independent of any single acquisition."""

    artifact_kind: str
    filename: str
    logical_url: str
    service: str | None = None
    period: str | None = None
    # "complete_object" for a byte-for-byte copy of the upstream object,
    # "bounded_sample" for the M0 row-limited derivative, "synthetic_fixture"
    # for generated test data. Never inferred from the bytes.
    completeness: str = "complete_object"

    @property
    def logical_id(self) -> str:
        # Completeness is part of logical identity: a bounded derivative, a
        # synthetic fixture and the complete upstream object are different
        # datasets even when they name the same service and period.
        parts = [self.artifact_kind, self.completeness]
        if self.service:
            parts.append(self.service)
        if self.period:
            parts.append(self.period)
        return ":".join(parts)

    @property
    def slug(self) -> str:
        return self.logical_id.replace(":", "__")


@dataclass(frozen=True)
class AcquisitionResult:
    state: str
    version_id: str
    manifest: dict[str, Any]
    artifact_path: Path


def version_id(logical_id: str, content_sha256: str) -> str:
    """Derive a stable version identifier from logical identity and content."""
    return hashlib.sha256(f"{logical_id}\n{content_sha256}".encode()).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(CHUNK_BYTES), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


class LandingStore:
    """Filesystem layout for landed artifacts, manifests and the run ledger."""

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)
        self.files_root = self.root / "files"
        self.manifest_root = self.root / "manifest"
        self.incoming_root = self.root / "_incoming"
        self.events_path = self.root / "events.jsonl"

    def initialize(self) -> None:
        for directory in (self.files_root, self.manifest_root, self.incoming_root):
            directory.mkdir(parents=True, exist_ok=True)

    def manifest_path(self, identity: ArtifactIdentity, version: str) -> Path:
        return self.manifest_root / identity.slug / f"{version}.json"

    def artifact_path(self, identity: ArtifactIdentity, version: str) -> Path:
        return self.files_root / identity.slug / version / identity.filename

    def versions(self, identity: ArtifactIdentity) -> list[dict[str, Any]]:
        directory = self.manifest_root / identity.slug
        if not directory.is_dir():
            return []
        records = [
            json.loads(item.read_text(encoding="utf-8")) for item in directory.glob("*.json")
        ]
        return sorted(records, key=lambda record: record["published_at_utc"])

    def append_event(self, event: dict[str, Any]) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        with self.events_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, sort_keys=True) + "\n")

    def events(self) -> list[dict[str, Any]]:
        if not self.events_path.is_file():
            return []
        text = self.events_path.read_text(encoding="utf-8")
        return [json.loads(line) for line in text.splitlines() if line.strip()]


Fetch = Callable[[Path], dict[str, Any]]
Validate = Callable[[Path], dict[str, Any]]


def fetch_local_file(source: Path | str) -> Fetch:
    """Copy an artifact from a path already present on this host."""
    source_path = Path(source)

    def fetch(target: Path) -> dict[str, Any]:
        if not source_path.is_file():
            raise ArtifactRejected(f"source artifact is missing: {source_path.name}")
        shutil.copyfile(source_path, target)
        # Only the file name is recorded. An absolute local path would leak the
        # operator's private filesystem layout into published evidence.
        return {"origin_kind": "local_file", "origin_reference": source_path.name}

    return fetch


def fetch_https(url: str, max_bytes: int = DEFAULT_MAX_DOWNLOAD_BYTES) -> Fetch:
    """Download an artifact over HTTPS under an explicit byte cap."""

    def fetch(target: Path) -> dict[str, Any]:
        request = urllib.request.Request(
            url,
            headers={"User-Agent": USER_AGENT, "Accept-Encoding": "identity"},
        )
        with urllib.request.urlopen(request, timeout=60) as response:
            declared = response.headers.get("Content-Length")
            if declared and int(declared) > max_bytes:
                raise ArtifactRejected(f"declared {declared} bytes exceeds cap {max_bytes}")
            written = 0
            with target.open("wb") as handle:
                while True:
                    chunk = response.read(CHUNK_BYTES)
                    if not chunk:
                        break
                    written += len(chunk)
                    if written > max_bytes:
                        raise ArtifactRejected(f"download exceeded cap {max_bytes} bytes")
                    handle.write(chunk)
            return {
                "origin_kind": "https",
                "origin_reference": url,
                "http_status": response.status,
                # Advisory transport metadata; never used as a content checksum.
                "source_etag": response.headers.get("ETag"),
                "source_last_modified": response.headers.get("Last-Modified"),
            }

    return fetch


def validate_parquet(path: Path) -> dict[str, Any]:
    """Reject a Parquet artifact whose footer or schema cannot be read."""
    try:
        with duckdb.connect(":memory:") as connection:
            metadata = connection.execute(
                "SELECT num_rows, num_row_groups, format_version FROM parquet_file_metadata(?)",
                [str(path)],
            ).fetchone()
            columns = connection.execute(
                "SELECT count(*) FROM parquet_schema(?) WHERE type IS NOT NULL",
                [str(path)],
            ).fetchone()
    except duckdb.Error as error:
        raise ArtifactRejected(f"unreadable Parquet artifact: {error}") from error
    if metadata is None or columns is None:
        raise ArtifactRejected("Parquet artifact exposed no footer metadata")
    if int(metadata[0]) == 0:
        raise ArtifactRejected("Parquet artifact contains no rows")
    return {
        "format": "parquet",
        "rows": int(metadata[0]),
        "row_groups": int(metadata[1]),
        "format_version": metadata[2],
        "columns": int(columns[0]),
    }


def validate_zone_lookup(path: Path) -> dict[str, Any]:
    """Reject a zone reference that breaks the published dimension contract."""
    reader = csv.DictReader(io.StringIO(path.read_text(encoding="utf-8-sig")))
    columns = reader.fieldnames or []
    missing = sorted(REQUIRED_ZONE_COLUMNS - set(columns))
    if missing:
        raise ArtifactRejected(f"zone lookup is missing required columns: {missing}")
    seen: set[int] = set()
    duplicates = 0
    invalid = 0
    rows = 0
    for row in reader:
        rows += 1
        try:
            location_id = int((row.get("LocationID") or "").strip())
        except ValueError:
            invalid += 1
            continue
        if location_id in seen:
            duplicates += 1
        seen.add(location_id)
    if rows == 0:
        raise ArtifactRejected("zone lookup contains no rows")
    if duplicates or invalid:
        raise ArtifactRejected(
            f"zone lookup has {duplicates} duplicate and {invalid} invalid location ids"
        )
    return {"format": "csv", "rows": rows, "columns": columns, "location_ids": len(seen)}


def acquire(
    store: LandingStore,
    identity: ArtifactIdentity,
    fetch: Fetch,
    validate: Validate,
    *,
    run_id: str,
    expected_bytes: int | None = None,
    expected_sha256: str | None = None,
    upstream: dict[str, Any] | None = None,
) -> AcquisitionResult:
    """Acquire one artifact version into landing, or observe a replay.

    Bytes are written to a temporary name inside the landing root, hashed and
    validated, and only then renamed into an immutable version directory. A
    rejected acquisition removes its partial file and is recorded in the ledger.
    """
    store.initialize()
    acquired_at = _now_utc()
    incoming = store.incoming_root / f"{uuid.uuid4().hex}.part"
    try:
        transport = fetch(incoming)
        size = incoming.stat().st_size
        if size == 0:
            raise ArtifactRejected("acquired artifact is empty")
        if expected_bytes is not None and size != expected_bytes:
            raise ArtifactRejected(f"expected {expected_bytes} bytes, acquired {size}")
        digest = sha256_file(incoming)
        if expected_sha256 is not None and digest != expected_sha256:
            raise ArtifactRejected(f"expected SHA-256 {expected_sha256}, acquired {digest}")
        profile = validate(incoming)

        version = version_id(identity.logical_id, digest)
        manifest_path = store.manifest_path(identity, version)
        target = store.artifact_path(identity, version)
        if manifest_path.is_file():
            existing = _read_existing_manifest(
                manifest_path,
                identity,
                version,
                digest,
                size,
                target.relative_to(store.root).as_posix(),
                profile,
            )
            if target.is_file() and target.stat().st_size == size and sha256_file(target) == digest:
                store.append_event(_event(run_id, identity, version, digest, REPLAYED))
                return AcquisitionResult(REPLAYED, version, existing, target)
            if target.exists() and not target.is_file():
                raise ArtifactRejected(f"landed artifact path is not a file for version {version}")

            # A valid manifest with a missing or damaged immutable file is not
            # a replay. The newly fetched, validated bytes repair that exact
            # content-addressed version and the ledger records the mutation.
            target.parent.mkdir(parents=True, exist_ok=True)
            os.replace(incoming, target)
            store.append_event(_event(run_id, identity, version, digest, REPAIRED))
            return AcquisitionResult(REPAIRED, version, existing, target)

        target.parent.mkdir(parents=True, exist_ok=True)
        # Atomic within the landing volume: readers never observe a partial file.
        os.replace(incoming, target)

        manifest = {
            **asdict(identity),
            "version_id": version,
            "logical_id": identity.logical_id,
            "content_sha256": digest,
            "content_length_bytes": size,
            "acquired_at_utc": acquired_at,
            "published_at_utc": _now_utc(),
            "publication_state": PUBLISHED,
            "relative_path": target.relative_to(store.root).as_posix(),
            "content_profile": profile,
            "transport": transport,
            "upstream": upstream or {},
        }
        _write_json_atomic(manifest_path, manifest)
        store.append_event(_event(run_id, identity, version, digest, PUBLISHED))
        return AcquisitionResult(PUBLISHED, version, manifest, target)
    except ArtifactRejected as error:
        store.append_event(_event(run_id, identity, None, None, REJECTED, str(error)))
        raise
    finally:
        # A published artifact was consumed by os.replace; anything left is partial.
        incoming.unlink(missing_ok=True)


def _read_existing_manifest(
    path: Path,
    identity: ArtifactIdentity,
    version: str,
    digest: str,
    size: int,
    relative_path: str,
    content_profile: dict[str, Any],
) -> dict[str, Any]:
    """Read an immutable manifest and reject inconsistent replay metadata."""
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ArtifactRejected(f"cannot read existing manifest for {version}: {error}") from error

    expected = {
        **asdict(identity),
        "version_id": version,
        "logical_id": identity.logical_id,
        "content_sha256": digest,
        "content_length_bytes": size,
        "relative_path": relative_path,
        "publication_state": PUBLISHED,
        "content_profile": content_profile,
    }
    mismatches = {
        key: {"expected": value, "observed": manifest.get(key)}
        for key, value in expected.items()
        if manifest.get(key) != value
    }
    if mismatches:
        raise ArtifactRejected(
            f"existing manifest is inconsistent for version {version}: {mismatches}"
        )
    return manifest


def _event(
    run_id: str,
    identity: ArtifactIdentity,
    version: str | None,
    digest: str | None,
    state: str,
    reason: str | None = None,
) -> dict[str, Any]:
    event = {
        "event_at_utc": _now_utc(),
        "run_id": run_id,
        "logical_id": identity.logical_id,
        "version_id": version,
        "content_sha256": digest,
        "state": state,
    }
    if reason is not None:
        event["reason"] = reason
    return event
