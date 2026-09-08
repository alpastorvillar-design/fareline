"""The taxi zone dimension, always bound to one explicit lookup version.

The lookup is 265 rows, so it is read on the driver and broadcast rather than
scanned as a distributed input. Every contracted row records the exact zone
lookup version it was joined against, and a key the lookup does not contain
produces an incident instead of a fabricated zone.
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fareline.m1 import landing
from fareline.m2 import versions

ZONE_COLUMNS = ("location_id", "borough", "zone_name", "service_zone")


class ZoneLookupUnavailable(RuntimeError):
    """No usable taxi zone lookup version is landed."""


@dataclass(frozen=True)
class ZoneDimension:
    version_id: str
    content_sha256: str
    rows: tuple[tuple[int, str, str, str], ...]

    @property
    def location_ids(self) -> frozenset[int]:
        return frozenset(row[0] for row in self.rows)

    def as_document(self) -> dict[str, Any]:
        return {
            "zone_lookup_version_id": self.version_id,
            "zone_lookup_content_sha256": self.content_sha256,
            "zone_rows": len(self.rows),
        }


def read_zone_csv(path: Path | str) -> tuple[tuple[int, str, str, str], ...]:
    """Parse a landed lookup file into the dimension tuple, ordered by key."""
    text = Path(path).read_text(encoding="utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    rows: dict[int, tuple[int, str, str, str]] = {}
    for record in reader:
        raw = (record.get("LocationID") or "").strip()
        try:
            location_id = int(raw)
        except ValueError as error:
            raise ZoneLookupUnavailable(f"zone lookup has a non-integer LocationID {raw!r}") from (
                error
            )
        if location_id in rows:
            raise ZoneLookupUnavailable(f"zone lookup repeats LocationID {location_id}")
        rows[location_id] = (
            location_id,
            (record.get("Borough") or "").strip(),
            (record.get("Zone") or "").strip(),
            (record.get("service_zone") or "").strip(),
        )
    if not rows:
        raise ZoneLookupUnavailable("zone lookup contains no rows")
    return tuple(rows[key] for key in sorted(rows))


def select_version(
    store: landing.LandingStore, requested_version_id: str | None = None
) -> dict[str, Any]:
    """Resolve which landed lookup version this run joins against.

    A caller may pin a version; otherwise the newest landed one is chosen by the
    same deterministic rule the trip artifacts use. Either way the chosen id is
    recorded, so a join is never against an implicit 'latest'.
    """
    from fareline.m1.sources import zone_identity

    manifests = store.versions(zone_identity())
    if not manifests:
        raise ZoneLookupUnavailable("no taxi zone lookup version has been landed")
    if requested_version_id is not None:
        for manifest in manifests:
            if manifest["version_id"] == requested_version_id:
                return manifest
        raise ZoneLookupUnavailable(f"zone lookup version {requested_version_id} is not landed")
    return max(manifests, key=versions.sort_key)


def load(store: landing.LandingStore, requested_version_id: str | None = None) -> ZoneDimension:
    manifest = select_version(store, requested_version_id)
    path = store.root / manifest["relative_path"]
    if not path.is_file():
        raise ZoneLookupUnavailable(
            f"landed zone lookup file is missing for {manifest['version_id']}"
        )
    return ZoneDimension(
        version_id=manifest["version_id"],
        content_sha256=manifest["content_sha256"],
        rows=read_zone_csv(path),
    )
