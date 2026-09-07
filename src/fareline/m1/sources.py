"""Which artifacts the M1 slice acquires, and how they map to TLC identity.

This module deliberately depends only on Python and DuckDB so that acquisition
can run outside a Spark submission.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fareline.m0 import TAXI_ZONE_LOOKUP, source_file
from fareline.m1 import landing, quality

TRIP_ARTIFACT_KIND = "trip_records"
ZONE_ARTIFACT_KIND = "zone_lookup"
LOCAL_TRIP_COMPLETENESS = {"bounded_sample", "synthetic_fixture"}


def trip_identity(service: str, period: str, completeness: str) -> landing.ArtifactIdentity:
    year, month = quality.period_parts(period)
    item = source_file(service, year, month)
    return landing.ArtifactIdentity(
        artifact_kind=TRIP_ARTIFACT_KIND,
        filename=item.filename,
        logical_url=item.url,
        service=service,
        period=period,
        completeness=completeness,
    )


def zone_identity() -> landing.ArtifactIdentity:
    return landing.ArtifactIdentity(
        artifact_kind=ZONE_ARTIFACT_KIND,
        filename="taxi_zone_lookup.csv",
        logical_url=TAXI_ZONE_LOOKUP.url,
        completeness="complete_object",
    )


def land_artifacts(
    store: landing.LandingStore,
    *,
    source_dir: Path,
    period: str,
    services: tuple[str, ...],
    completeness: str,
    acquire_zone_lookup: bool,
    run_id: str,
    m0_evidence_path: Path | None = None,
) -> dict[str, landing.AcquisitionResult]:
    """Acquire every artifact of the slice once and report each outcome."""
    if completeness not in LOCAL_TRIP_COMPLETENESS:
        raise ValueError(
            "local trip inputs may be bounded_sample or synthetic_fixture; "
            "complete_object requires a separately verified upstream acquisition path"
        )
    m0_hashes = _m0_sample_hashes(m0_evidence_path)
    results: dict[str, landing.AcquisitionResult] = {}
    for service in services:
        identity = trip_identity(service, period, completeness)
        expected_sample_hash = (
            m0_hashes.get((service, period)) if completeness == "bounded_sample" else None
        )
        if (
            completeness == "bounded_sample"
            and m0_evidence_path is not None
            and expected_sample_hash is None
        ):
            raise landing.ArtifactRejected(
                f"M0 evidence has no bounded-sample hash for {service} {period}"
            )
        results[service] = landing.acquire(
            store,
            identity,
            landing.fetch_local_file(Path(source_dir) / identity.filename),
            landing.validate_parquet,
            run_id=run_id,
            expected_sha256=expected_sample_hash,
            upstream=_upstream(identity, expected_sample_hash),
        )
    if acquire_zone_lookup:
        identity = zone_identity()
        results[ZONE_ARTIFACT_KIND] = landing.acquire(
            store,
            identity,
            landing.fetch_https(identity.logical_url),
            landing.validate_zone_lookup,
            run_id=run_id,
            upstream=_upstream(identity, None),
        )
    return results


def _upstream(identity: landing.ArtifactIdentity, m0_sample_sha256: str | None) -> dict[str, Any]:
    return {
        "logical_url": identity.logical_url,
        "period": identity.period,
        "acquired_scope": identity.completeness,
        # The digest covers the bytes acquired locally. For a bounded sample it
        # is not the digest of the remote monthly object.
        "hash_covers_upstream_object": identity.completeness == "complete_object",
        "m0_sample_sha256": m0_sample_sha256,
    }


def _m0_sample_hashes(path: Path | None) -> dict[tuple[str, str], str]:
    """Bind a landed bounded sample to the hash M0 recorded for it, if available."""
    if path is None or not path.is_file():
        return {}
    report = json.loads(path.read_text(encoding="utf-8"))
    return {
        (item["service"], f"{item['year']}-{item['month']:02d}"): item["sha256"]
        for item in report.get("samples", [])
    }
