"""Where derived tables live, and which directories a run is allowed to delete.

Kept free of Spark so both can be checked without a session. The rebuild root is
the only directory an M2 run removes and it arrives as free text on the command
line, so it is verified against the directories that hold real data before
anything is deleted.
"""

from __future__ import annotations

import json
import shutil
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

CONTRACTED_RELATIVE_PATHS = {
    "yellow": "contracted_trips/yellow_trip",
    "hvfhv": "contracted_trips/hvfhv_trip",
}
QUARANTINE_RELATIVE_PATHS = {
    "yellow": "quarantine/yellow_trip",
    "hvfhv": "quarantine/hvfhv_trip",
}
INCIDENT_RELATIVE_PATHS = {
    "yellow": "quality_incidents/yellow_trip",
    "hvfhv": "quality_incidents/hvfhv_trip",
}

REBUILD_MARKER_NAME = ".fareline-rebuild-root.json"
REBUILD_MARKER_SCHEMA_VERSION = 1


class UnsafeRebuildRoot(RuntimeError):
    """The requested rebuild root overlaps a directory that holds real data."""


@dataclass(frozen=True)
class TablePaths:
    contracted: Path
    quarantine: Path
    incidents: Path

    def all(self) -> tuple[Path, ...]:
        return (self.contracted, self.quarantine, self.incidents)


def table_paths(warehouse_root: Path | str, service: str, contract_fingerprint: str) -> TablePaths:
    """Locate the three derived tables of one service under one contract.

    A contract revision that adds a column or changes a type cannot be appended
    to the tables the previous revision wrote: Delta refuses the metadata change,
    and forcing it with ``mergeSchema`` would blur two output schemas into one.
    Each fingerprint therefore owns its own tables, the earlier history stays
    readable exactly as it was written, and the publication boundary decides
    which fingerprint readers are on.
    """
    root = Path(warehouse_root)
    leaf = f"contract={contract_fingerprint}"
    return TablePaths(
        contracted=root / CONTRACTED_RELATIVE_PATHS[service] / leaf,
        quarantine=root / QUARANTINE_RELATIVE_PATHS[service] / leaf,
        incidents=root / INCIDENT_RELATIVE_PATHS[service] / leaf,
    )


def as_uri(path: Path | str) -> str:
    """Render a local path as a ``file://`` URI Spark can open.

    A Windows path needs the extra slash: ``file://C:/x`` parses ``C:`` as the
    URI authority, which is not a host and not a drive.
    """
    text = str(PurePosixPath(str(path).replace("\\", "/")))
    if not text.startswith("/"):
        text = "/" + text
    return "file://" + text


def repository_root(start: Path | str) -> Path | None:
    """The enclosing Git checkout, if the given directory is inside one.

    A checkout holds versioned working state, so it is protected. The copy of
    the sources inside the container image is not a checkout and is not treated
    as one, which is what keeps the default rebuild root usable there.
    """
    current = Path(start).resolve()
    for candidate in (current, *current.parents):
        if (candidate / ".git").exists():
            return candidate
    return None


def _overlaps(left: Path, right: Path) -> bool:
    return left == right or left.is_relative_to(right) or right.is_relative_to(left)


def verify_rebuild_root(rebuild_root: Path | str, protected: Iterable[Path | str]) -> Path:
    """Refuse a rebuild root that could take real data with it.

    Both directions matter. A rebuild root inside the warehouse deletes part of
    it; a rebuild root above the warehouse deletes all of it.
    """
    resolved = Path(rebuild_root).resolve()
    if resolved == Path(resolved.anchor):
        raise UnsafeRebuildRoot(f"the rebuild root may not be the filesystem root {resolved}")
    for item in protected:
        candidate = Path(item).resolve()
        if _overlaps(resolved, candidate):
            raise UnsafeRebuildRoot(
                f"the rebuild root {resolved} overlaps {candidate}, which holds real data"
            )
    return resolved


def rebuild_marker_path(rebuild_root: Path | str) -> Path:
    return Path(rebuild_root).resolve() / REBUILD_MARKER_NAME


def prepare_rebuild_root(rebuild_root: Path | str, protected: Iterable[Path | str]) -> Path:
    """Claim an empty scratch root, or verify that Fareline already owns it.

    Path-overlap checks protect the known project roots, but a free-text path
    can still name some unrelated directory. Fareline therefore never removes a
    non-empty directory unless a marker from an earlier rebuild identifies it as
    disposable scratch space.
    """
    resolved = verify_rebuild_root(rebuild_root, protected)
    if resolved.exists() and not resolved.is_dir():
        raise UnsafeRebuildRoot(f"the rebuild root {resolved} is not a directory")

    marker = rebuild_marker_path(resolved)
    expected = {
        "schema_version": REBUILD_MARKER_SCHEMA_VERSION,
        "purpose": "fareline-rebuild-scratch",
    }
    if resolved.exists():
        if marker.is_file():
            try:
                observed = json.loads(marker.read_text(encoding="utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError) as error:
                raise UnsafeRebuildRoot(
                    f"the rebuild root marker {marker} is unreadable"
                ) from error
            if observed != expected:
                raise UnsafeRebuildRoot(
                    f"the rebuild root marker {marker} does not identify supported scratch space"
                )
            return resolved
        if any(resolved.iterdir()):
            raise UnsafeRebuildRoot(
                f"the rebuild root {resolved} is not owned by Fareline and is not empty"
            )
    else:
        resolved.mkdir(parents=True)

    marker.write_text(json.dumps(expected, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return resolved


def clear_rebuild_root(rebuild_root: Path | str, protected: Iterable[Path | str]) -> None:
    """Empty owned rebuild scratch, failing loudly if it cannot be reset.

    Ignoring removal errors would leave rows from a previous run inside the
    rebuild and turn the equivalence check into a comparison of two histories.
    """
    resolved = prepare_rebuild_root(rebuild_root, protected)
    shutil.rmtree(resolved)
    if resolved.exists():
        raise UnsafeRebuildRoot(f"the rebuild root {resolved} could not be removed")
    prepare_rebuild_root(resolved, protected)
