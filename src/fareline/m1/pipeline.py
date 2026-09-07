"""Orchestration for the M1 vertical slice.

The slice runs acquisition and ingestion twice on purpose: the second pass must
change nothing observable, which is the M1 definition of idempotency.
"""

from __future__ import annotations

import json
import platform
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from fareline import __version__
from fareline.m1 import landing, occurrences, quality, sources
from fareline.m1.sources import trip_identity


@dataclass(frozen=True)
class SliceOptions:
    source_dir: Path
    landing_root: Path
    warehouse_root: Path
    evidence_path: Path
    period: str = "2024-01"
    services: tuple[str, ...] = ("yellow", "hvfhv")
    completeness: str = "bounded_sample"
    acquire_zone_lookup: bool = True
    max_alignment_rows: int = 250_000
    m0_evidence_path: Path | None = None
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex)


def _land_for(options: SliceOptions, store: landing.LandingStore) -> dict[str, Any]:
    return sources.land_artifacts(
        store,
        source_dir=options.source_dir,
        period=options.period,
        services=options.services,
        completeness=options.completeness,
        acquire_zone_lookup=options.acquire_zone_lookup,
        run_id=options.run_id,
        m0_evidence_path=options.m0_evidence_path,
    )


def ingest_artifacts(
    spark: Any, options: SliceOptions, landed: dict[str, Any]
) -> dict[str, occurrences.IngestResult]:
    return {
        service: occurrences.ingest_version(
            spark,
            service,
            options.warehouse_root,
            landed[service].artifact_path,
            landed[service].manifest,
            options.run_id,
        )
        for service in options.services
    }


def table_state(spark: Any, options: SliceOptions) -> dict[str, Any]:
    """Re-read every occurrence table and compare it against landing manifests."""
    store = landing.LandingStore(options.landing_root)
    state: dict[str, Any] = {}
    for service in options.services:
        identity = trip_identity(service, options.period, options.completeness)
        records = store.versions(identity)
        state[service] = occurrences.verify_table(
            spark,
            service,
            options.warehouse_root,
            [record["version_id"] for record in records],
            sum(record["content_profile"]["rows"] for record in records),
            options.completeness,
        )
    return state


def compare_service(
    spark: Any, options: SliceOptions, service: str, landed: dict[str, Any]
) -> dict[str, Any]:
    """Compare published occurrences against the DuckDB oracle over the landed file."""
    spec = quality.SERVICE_SPECS[service]
    manifest = landed[service].manifest
    artifact_path = landed[service].artifact_path
    version_id = manifest["version_id"]

    engine_metrics = occurrences.spark_metrics(
        spark, spec, options.warehouse_root, version_id, options.period
    )
    oracle_metrics = quality.duckdb_metrics(spec, artifact_path, options.period)
    comparison = quality.compare_metrics(engine_metrics, oracle_metrics)

    rows = int(manifest["content_profile"]["rows"])
    if rows <= options.max_alignment_rows:
        alignment = quality.compare_alignment(
            occurrences.spark_alignment(spark, spec, options.warehouse_root, version_id),
            quality.duckdb_alignment(spec, artifact_path),
        )
        alignment["checked"] = True
    else:
        alignment = {
            "checked": False,
            "reason": f"{rows} rows exceed the {options.max_alignment_rows} row comparison bound",
        }
    return {
        "version_id": version_id,
        "period": options.period,
        "fare_components": list(spec.fare_components),
        "metrics": comparison,
        "ordinal_alignment": alignment,
    }


@dataclass(frozen=True)
class SliceObservations:
    """Everything one slice run measured, in the order it was measured."""

    runtime: dict[str, Any]
    first_landing: dict[str, landing.AcquisitionResult]
    first_ingest: dict[str, occurrences.IngestResult]
    state_after_first: dict[str, Any]
    replay_landing: dict[str, landing.AcquisitionResult]
    replay_ingest: dict[str, occurrences.IngestResult]
    state_after_replay: dict[str, Any]
    oracle: dict[str, Any]
    timings: dict[str, float]


def run_slice(options: SliceOptions) -> dict[str, Any]:
    """Run acquisition, ingestion, replay, verification and the oracle once."""
    if options.completeness != "bounded_sample":
        raise ValueError("the measured M1 slice requires bounded_sample trip artifacts")
    if options.m0_evidence_path is None or not options.m0_evidence_path.is_file():
        raise ValueError("the measured M1 slice requires the M0 sample evidence file")

    timings: dict[str, float] = {}
    store = landing.LandingStore(options.landing_root)

    started = time.perf_counter()
    first_landing = _land_for(options, store)
    timings["land_first_pass"] = round(time.perf_counter() - started, 3)

    started = time.perf_counter()
    spark = occurrences.build_session(f"fareline-m1-{options.run_id[:8]}", options.warehouse_root)
    spark.sparkContext.setLogLevel("WARN")
    timings["spark_session"] = round(time.perf_counter() - started, 3)

    try:
        runtime = _runtime(spark)

        started = time.perf_counter()
        first_ingest = ingest_artifacts(spark, options, first_landing)
        timings["ingest_first_pass"] = round(time.perf_counter() - started, 3)

        started = time.perf_counter()
        state_after_first = table_state(spark, options)
        timings["verify_first_pass"] = round(time.perf_counter() - started, 3)

        started = time.perf_counter()
        replay_landing = _land_for(options, store)
        replay_ingest = ingest_artifacts(spark, options, replay_landing)
        timings["replay_pass"] = round(time.perf_counter() - started, 3)

        started = time.perf_counter()
        state_after_replay = table_state(spark, options)
        timings["verify_replay"] = round(time.perf_counter() - started, 3)

        started = time.perf_counter()
        oracle = {
            service: compare_service(spark, options, service, first_landing)
            for service in options.services
        }
        timings["oracle"] = round(time.perf_counter() - started, 3)

        report = _report(
            options,
            store,
            SliceObservations(
                runtime=runtime,
                first_landing=first_landing,
                first_ingest=first_ingest,
                state_after_first=state_after_first,
                replay_landing=replay_landing,
                replay_ingest=replay_ingest,
                state_after_replay=state_after_replay,
                oracle=oracle,
                timings=timings,
            ),
        )
    finally:
        spark.stop()

    options.evidence_path.parent.mkdir(parents=True, exist_ok=True)
    options.evidence_path.write_text(
        json.dumps(report, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8"
    )
    return report


def _runtime(spark: Any) -> dict[str, Any]:
    import delta

    packages = spark.sparkContext.getConf().get("spark.jars.packages", None)
    jar_version = packages.rsplit(":", 1)[-1] if packages else None
    if jar_version is not None and jar_version != delta.__version__:
        raise RuntimeError(
            f"Delta jar coordinate {packages} does not match the Python package "
            f"{delta.__version__}; the image pins are inconsistent"
        )
    return {
        "fareline_version": __version__,
        "spark_version": spark.version,
        "spark_master": spark.sparkContext.getConf().get("spark.master", "unknown"),
        "delta_python_version": delta.__version__,
        "delta_jar_packages": packages,
        "java_version": spark._jvm.System.getProperty("java.version"),
        "python_version": platform.python_version(),
        "duckdb_version": duckdb.__version__,
        "probe_executor_hosts": occurrences.probe_executor_hosts(spark),
    }


def _report(
    options: SliceOptions,
    store: landing.LandingStore,
    observed: SliceObservations,
) -> dict[str, Any]:
    replay_landing = observed.replay_landing
    replay_ingest = observed.replay_ingest
    state_after_first = observed.state_after_first
    state_after_replay = observed.state_after_replay
    table_unchanged = state_after_first == state_after_replay
    replay_is_noop = (
        all(result.state == landing.REPLAYED for result in replay_landing.values())
        and all(result.state == landing.REPLAYED for result in replay_ingest.values())
        and table_unchanged
    )
    return {
        "milestone": "m1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "run_id": options.run_id,
        "scope": {
            "period": options.period,
            "services": list(options.services),
            "trip_artifact_completeness": options.completeness,
            "zone_lookup_acquired": options.acquire_zone_lookup,
            "executed_on": "locally acquired bounded TLC samples and the official zone lookup",
            "not_executed": [
                "full monthly or annual TLC objects",
                "contracted service tables, analytical products, dimensional models and Power BI",
                "version supersession and incremental-versus-rebuild equivalence",
                "multi-driver concurrency and object storage",
                "cloud infrastructure of any provider",
            ],
        },
        "runtime": observed.runtime,
        "landing": {
            "root": str(options.landing_root),
            "artifacts": {
                key: _artifact_evidence(result, replay_landing.get(key))
                for key, result in observed.first_landing.items()
            },
            "ledger_events": {
                "current_run": _event_counts(
                    [event for event in store.events() if event["run_id"] == options.run_id]
                ),
                "all_runs": _event_counts(store.events()),
            },
        },
        "source_occurrences": {
            service: {
                **state_after_first[service],
                "first_pass_state": observed.first_ingest[service].state,
                "first_pass_rows_written": observed.first_ingest[service].rows_written,
                "replay_state": replay_ingest[service].state,
                "delta_version_after_replay": state_after_replay[service]["delta_version"],
            }
            for service in options.services
        },
        "replay": {
            "landing_states": {key: result.state for key, result in replay_landing.items()},
            "source_occurrence_states": {
                service: result.state for service, result in replay_ingest.items()
            },
            "table_state_unchanged": table_unchanged,
            "no_op": replay_is_noop,
        },
        "quality": observed.oracle,
        "timings_seconds": observed.timings,
        "limits": [
            "The slice processes bounded samples of 1,000 rows per service, not a "
            "complete month and not 100 million rows.",
            "Idempotency is demonstrated for a single writer on a local filesystem; "
            "concurrent drivers and object storage are not covered.",
            "Every acquired version stays published; supersession and rebuild "
            "equivalence are M2 work.",
            "The SHA-256 of a bounded sample does not describe the remote monthly "
            "object it was derived from.",
        ],
    }


def _artifact_evidence(result: Any, replay: Any | None) -> dict[str, Any]:
    manifest = result.manifest
    return {
        "logical_id": manifest["logical_id"],
        "version_id": manifest["version_id"],
        "content_sha256": manifest["content_sha256"],
        "content_length_bytes": manifest["content_length_bytes"],
        "content_profile": manifest["content_profile"],
        "publication_state": manifest["publication_state"],
        "relative_path": manifest["relative_path"],
        "origin_kind": manifest["transport"]["origin_kind"],
        "upstream": manifest["upstream"],
        "first_pass_state": result.state,
        "replay_state": None if replay is None else replay.state,
    }


def _event_counts(events: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for event in events:
        counts[event["state"]] = counts.get(event["state"], 0) + 1
    return counts
