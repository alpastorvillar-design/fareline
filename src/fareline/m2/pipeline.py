"""Orchestration for the M2 contracted slice.

One run lands the configured artifacts, decides which source version is current,
maintains the contracted tables incrementally, rebuilds the same tables from
scratch into an isolated root, and compares the two. It then replays the
incremental pass to show that a second run changes nothing.
"""

from __future__ import annotations

import json
import platform
import shutil
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
from pyspark.sql import functions as F

from fareline import __version__
from fareline.m1 import landing, occurrences, quality, sources
from fareline.m1.sources import trip_identity
from fareline.m2 import build, contracts, equivalence, localtime, resolution, versions, zones

KEY_COLUMNS = ["source_file_version_id", "source_row_ordinal"]
INCIDENT_KEY_COLUMNS = ["incident_id"]

INCREMENTAL = "incremental"
REBUILD = "rebuild"

TABLE_KINDS = ("contracted", "quarantine", "incidents")


class EvidenceMissing(RuntimeError):
    """A configured bounded sample has no recorded acquisition hash."""


@dataclass(frozen=True)
class ArtifactRequest:
    service: str
    period: str


@dataclass(frozen=True)
class SliceOptions:
    source_dir: Path
    landing_root: Path
    warehouse_root: Path
    rebuild_root: Path
    evidence_path: Path
    sample_evidence_paths: tuple[Path, ...]
    artifacts: tuple[ArtifactRequest, ...]
    completeness: str = "bounded_sample"
    zone_lookup_version_id: str | None = None
    max_digest_rows: int = equivalence.DEFAULT_MAX_DIGEST_ROWS
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex)

    @property
    def services(self) -> tuple[str, ...]:
        return tuple(sorted({item.service for item in self.artifacts}))


@dataclass(frozen=True)
class VersionPlan:
    service: str
    period: str
    manifest: dict[str, Any]
    resolved: resolution.SchemaResolution
    state: str


def evidence_index(paths: tuple[Path, ...]) -> dict[tuple[str, str], Path]:
    """Map each recorded bounded sample to the file that records its hash.

    A measured run refuses to publish a sample whose provenance is not written
    down somewhere, so acquisition can always be checked against a hash that
    existed before the run started.
    """
    index: dict[tuple[str, str], Path] = {}
    for path in paths:
        if not path.is_file():
            continue
        report = json.loads(path.read_text(encoding="utf-8"))
        for sample in report.get("samples", []):
            index[(sample["service"], f"{sample['year']}-{sample['month']:02d}")] = path
    return index


def land(options: SliceOptions, store: landing.LandingStore) -> dict[str, Any]:
    """Acquire every configured artifact once; the zone lookup only once."""
    index = evidence_index(options.sample_evidence_paths)
    landed: dict[str, Any] = {}
    for position, request in enumerate(options.artifacts):
        evidence = index.get((request.service, request.period))
        if evidence is None:
            raise EvidenceMissing(
                f"no recorded acquisition hash for {request.service} {request.period}"
            )
        results = sources.land_artifacts(
            store,
            source_dir=options.source_dir,
            period=request.period,
            services=(request.service,),
            completeness=options.completeness,
            acquire_zone_lookup=position == 0,
            run_id=options.run_id,
            m0_evidence_path=evidence,
        )
        for key, result in results.items():
            if key == sources.ZONE_ARTIFACT_KIND:
                landed[key] = result
            else:
                landed[f"{key}:{request.period}"] = result
    return landed


def plan(spark: Any, options: SliceOptions, store: landing.LandingStore) -> list[VersionPlan]:
    """Resolve every landed candidate and decide which one is current."""
    planned: list[VersionPlan] = []
    for request in options.artifacts:
        contract = contracts.SERVICE_CONTRACTS[request.service]
        identity = trip_identity(request.service, request.period, options.completeness)
        manifests = store.versions(identity)
        resolutions = {
            manifest["version_id"]: _resolve_candidate(
                spark, contract, store.root / manifest["relative_path"]
            )
            for manifest in manifests
        }
        decision = versions.decide(
            identity.logical_id,
            request.service,
            request.period,
            manifests,
            {version: _violation_keys(resolved) for version, resolved in resolutions.items()},
        )
        by_version = {manifest["version_id"]: manifest for manifest in manifests}
        planned.extend(
            VersionPlan(
                service=request.service,
                period=request.period,
                manifest=by_version[candidate.version_id],
                resolved=resolutions[candidate.version_id],
                state=candidate.state,
            )
            for candidate in decision.candidates
        )
    return planned


def _resolve_candidate(
    spark: Any, contract: contracts.ServiceContract, artifact: Path
) -> resolution.SchemaResolution:
    """Resolve one candidate, turning an unreadable file into a rejection."""
    try:
        return resolution.resolve(contract, build.source_schema(spark, artifact))
    except Exception as error:  # noqa: BLE001 - any reader failure is a rejection
        # A candidate nobody can describe must not stop the other artifacts of
        # the run; it is refused with the reason the reader gave.
        return resolution.unreadable(contract, f"{type(error).__name__}: {error}"[:300])


def _violation_keys(resolved: resolution.SchemaResolution) -> tuple[str, ...]:
    return tuple(f"{item.rule}:{item.canonical_name}" for item in resolved.violations)


def decisions_for(
    options: SliceOptions, planned: list[VersionPlan]
) -> list[versions.VersionDecision]:
    """Re-express the plan as one decision per logical artifact, for the ledger."""
    decisions: list[versions.VersionDecision] = []
    for request in options.artifacts:
        identity = trip_identity(request.service, request.period, options.completeness)
        items = [
            item
            for item in planned
            if item.service == request.service and item.period == request.period
        ]
        if not items:
            continue
        active = next((item for item in items if item.state == versions.ACTIVE), None)
        decisions.append(
            versions.VersionDecision(
                logical_id=identity.logical_id,
                service=request.service,
                period=request.period,
                active_version_id=None if active is None else active.manifest["version_id"],
                candidates=tuple(
                    versions.CandidateVersion(
                        version_id=item.manifest["version_id"],
                        published_at_utc=item.manifest["published_at_utc"],
                        state=item.state,
                        violations=_violation_keys(item.resolved),
                    )
                    for item in items
                ),
            )
        )
    return decisions


def _transaction_id(service: str, kind: str, derived_id: str) -> str:
    return f"fareline-m2:{service}:{kind}:{derived_id}"


def _intervals(year: int, month: int, zone_name: str) -> tuple[localtime.LocalInterval, ...]:
    # A missing time-zone database makes the civil-time rules unevaluable. It
    # is a run failure, never evidence that the month had zero affected rows.
    return localtime.transitions(year, month, zone_name)


def write_version(
    spark: Any,
    warehouse_root: Path,
    item: VersionPlan,
    store: landing.LandingStore,
    zone_dimension: zones.ZoneDimension,
    zone_frame: Any,
) -> build.VersionOutcome:
    """Project one active version into the three derived tables."""
    contract = contracts.SERVICE_CONTRACTS[item.service]
    paths = build.table_paths(warehouse_root, item.service)
    artifact = store.root / item.manifest["relative_path"]
    year, month = quality.period_parts(item.period)
    derived_id = versions.derivation_id(
        item.manifest["version_id"], contract.fingerprint, zone_dimension.version_id
    )

    evaluated = build.contracted_frame(
        build.read_source_version(spark, artifact),
        contract,
        item.resolved,
        item.manifest,
        zone_frame,
        _intervals(year, month, contract.local_time_zone),
        derived_id,
    ).cache()
    try:
        published, quarantined, incidents = build.split(
            evaluated, contract, zone_dimension.version_id
        )
        published_rows = published.count()
        quarantined_rows = quarantined.count()
        incident_rows = incidents.count()
        source_rows = int(item.manifest["content_profile"]["rows"])
        if published_rows + quarantined_rows != source_rows:
            raise RuntimeError(
                f"{item.service} {item.period} split {published_rows} published and "
                f"{quarantined_rows} quarantined rows, but the version has {source_rows}"
            )

        version_id = item.manifest["version_id"]
        for frame, path, kind in (
            (published, paths.contracted, "contracted"),
            (quarantined, paths.quarantine, "quarantine"),
            (incidents, paths.incidents, "incidents"),
        ):
            build.write_partition(frame, path, _transaction_id(item.service, kind, derived_id))
    finally:
        evaluated.unpersist()

    return build.VersionOutcome(
        service=item.service,
        period=item.period,
        version_id=version_id,
        derivation_id=derived_id,
        state=item.state,
        action="written",
        contracted_rows=published_rows,
        quarantined_rows=quarantined_rows,
        incident_rows=incident_rows,
        source_rows=source_rows,
    )


def write_rejection(
    spark: Any,
    warehouse_root: Path,
    item: VersionPlan,
    zone_dimension: zones.ZoneDimension,
) -> build.VersionOutcome:
    """Record why a version was refused, without reading a single source row."""
    contract = contracts.SERVICE_CONTRACTS[item.service]
    paths = build.table_paths(warehouse_root, item.service)
    version_id = item.manifest["version_id"]
    derived_id = versions.derivation_id(version_id, contract.fingerprint, zone_dimension.version_id)
    build.write_partition(
        build.version_incidents(spark, contract, item.manifest, item.resolved, derived_id),
        paths.incidents,
        _transaction_id(item.service, "incidents", derived_id),
    )
    return build.VersionOutcome(
        service=item.service,
        period=item.period,
        version_id=version_id,
        derivation_id=derived_id,
        state=item.state,
        action="rejected",
        contracted_rows=0,
        quarantined_rows=0,
        incident_rows=len(item.resolved.violations),
        source_rows=int(item.manifest["content_profile"]["rows"]),
    )


def _unwritten(item: VersionPlan, action: str, derived_id: str) -> build.VersionOutcome:
    return build.VersionOutcome(
        service=item.service,
        period=item.period,
        version_id=item.manifest["version_id"],
        derivation_id=derived_id,
        state=item.state,
        action=action,
        contracted_rows=0,
        quarantined_rows=0,
        incident_rows=0,
        source_rows=int(item.manifest["content_profile"]["rows"]),
    )


def apply_plan(
    spark: Any,
    options: SliceOptions,
    warehouse_root: Path,
    planned: list[VersionPlan],
    store: landing.LandingStore,
    zone_dimension: zones.ZoneDimension,
    zone_frame: Any,
) -> dict[str, Any]:
    """Materialise candidates, then expose only fully written derivations.

    Delta commits are atomic per table, not across the three tables belonging to
    a service. The immutable publication marker is therefore written last. A
    reader using the catalog sees the preceding active derivation until every
    required write for its replacement has succeeded.
    """
    outcomes: list[build.VersionOutcome] = []
    publication_markers: list[dict[str, Any]] = []
    catalog = versions.PublicationCatalog(warehouse_root)
    decision_index = {
        (decision.service, decision.period): decision
        for decision in decisions_for(options, planned)
    }

    for service in options.services:
        items = [item for item in planned if item.service == service]
        for item in sorted(items, key=lambda entry: (entry.period, entry.manifest["version_id"])):
            contract = contracts.SERVICE_CONTRACTS[item.service]
            derived_id = versions.derivation_id(
                item.manifest["version_id"], contract.fingerprint, zone_dimension.version_id
            )
            decision = decision_index[(item.service, item.period)]
            candidate = next(
                entry
                for entry in decision.candidates
                if entry.version_id == item.manifest["version_id"]
            )
            marker_exists = catalog.marker_path(decision.logical_id, derived_id).is_file()

            if marker_exists:
                outcomes.append(_unwritten(item, "already_published", derived_id))
            elif item.state == versions.ACTIVE:
                outcomes.append(
                    write_version(spark, warehouse_root, item, store, zone_dimension, zone_frame)
                )
            elif item.state == versions.REJECTED:
                outcomes.append(write_rejection(spark, warehouse_root, item, zone_dimension))
            else:
                outcomes.append(_unwritten(item, "superseded_not_published", derived_id))

            if item.state in {versions.ACTIVE, versions.REJECTED}:
                marker_action, marker = catalog.publish(
                    decision,
                    candidate,
                    contract_version=contract.contract_version,
                    contract_fingerprint=contract.fingerprint,
                    zone_lookup_version_id=zone_dimension.version_id,
                )
                publication_markers.append({"action": marker_action, **marker})

    return {
        "outcomes": [item.as_document() for item in outcomes],
        "publication_markers": publication_markers,
        "physical_history_removed": False,
    }


def measure_tables(
    spark: Any,
    options: SliceOptions,
    warehouse_root: Path,
    zone_lookup_version_id: str,
) -> dict[str, Any]:
    """Measure the marker-visible view and prove every physical file exists."""
    keys = {
        "contracted": KEY_COLUMNS,
        "quarantine": KEY_COLUMNS,
        "incidents": INCIDENT_KEY_COLUMNS,
    }
    catalog = versions.PublicationCatalog(warehouse_root)
    state: dict[str, Any] = {}
    for service in options.services:
        paths = build.table_paths(warehouse_root, service)
        visible = {kind: set() for kind in TABLE_KINDS}
        contract = contracts.SERVICE_CONTRACTS[service]
        for request in options.artifacts:
            if request.service != service:
                continue
            logical_id = trip_identity(service, request.period, options.completeness).logical_id
            for kind, ids in catalog.visible_derivations(
                logical_id,
                contract_fingerprint=contract.fingerprint,
                zone_lookup_version_id=zone_lookup_version_id,
            ).items():
                visible[kind].update(ids)
        state[service] = {
            kind: measure_table(spark, options, path, keys[kind], visible[kind])
            for kind, path in zip(TABLE_KINDS, paths.all(), strict=True)
        }
    return state


def measure_table(
    spark: Any,
    options: SliceOptions,
    path: Path,
    keys: list[str],
    visible_derivation_ids: set[str] | None = None,
) -> dict[str, Any]:
    """Measure one table, refusing a log that points at files nobody can read."""
    if not (path / "_delta_log").is_dir():
        raise RuntimeError(f"{path} has no Delta log")
    on_disk = sorted(item for item in path.rglob("*.parquet") if "_delta_log" not in item.parts)
    # An empty table legitimately has no data files, so what matters is whether
    # every file the log points at is really there. Checking it before the scan
    # turns a corrupt table into a clear failure instead of a reader exception.
    referenced = build.referenced_data_files(spark, path)
    missing = [item for item in referenced if not item.is_file()]
    if missing:
        raise RuntimeError(
            f"{path} references {len(missing)} of {len(referenced)} data files that do not "
            f"exist on disk, starting with {missing[0].name}"
        )
    physical = spark.read.format("delta").load(build.as_uri(path))
    frame = physical
    if visible_derivation_ids is not None:
        frame = physical.where(F.col("derivation_id").isin(sorted(visible_derivation_ids)))
    measured = equivalence.measure(frame, keys, max_rows=options.max_digest_rows)
    return {
        **measured,
        "visible_derivations": sorted(visible_derivation_ids or ()),
        "physical_rows": physical.count(),
        "delta_version": build.delta_version(spark, path),
        "referenced_data_files": len(referenced),
        "data_files_on_disk": len(on_disk),
        "data_file_bytes": sum(item.stat().st_size for item in referenced),
    }


def logical_state(measured: dict[str, Any]) -> dict[str, Any]:
    """Strip physical facts so two correct runs can be compared."""
    physical = {
        "physical_rows",
        "delta_version",
        "referenced_data_files",
        "data_files_on_disk",
        "data_file_bytes",
    }
    return {
        service: {
            kind: {name: value for name, value in table.items() if name not in physical}
            for kind, table in tables.items()
        }
        for service, tables in measured.items()
    }


def _runtime(spark: Any) -> dict[str, Any]:
    import delta

    return {
        "fareline_version": __version__,
        "spark_version": spark.version,
        "spark_master": spark.sparkContext.getConf().get("spark.master", "unknown"),
        "delta_python_version": delta.__version__,
        "java_version": spark._jvm.System.getProperty("java.version"),
        "python_version": platform.python_version(),
        "duckdb_version": duckdb.__version__,
        "probe_executor_hosts": occurrences.probe_executor_hosts(spark),
    }


def run_slice(options: SliceOptions) -> dict[str, Any]:
    """Land, plan, build incrementally, rebuild, compare, and replay."""
    timings: dict[str, float] = {}
    store = landing.LandingStore(options.landing_root)
    ledger_path = Path(options.warehouse_root) / "version_ledger.jsonl"
    ledger = versions.VersionLedger(ledger_path)
    contract_versions = {
        service: contracts.SERVICE_CONTRACTS[service].contract_version
        for service in options.services
    }

    started = time.perf_counter()
    landed = land(options, store)
    timings["land"] = round(time.perf_counter() - started, 3)

    started = time.perf_counter()
    spark = occurrences.build_session(f"fareline-m2-{options.run_id[:8]}", options.warehouse_root)
    spark.sparkContext.setLogLevel("WARN")
    build.use_physical_column_names(spark)
    timings["spark_session"] = round(time.perf_counter() - started, 3)

    try:
        runtime = _runtime(spark)
        zone_dimension = zones.load(store, options.zone_lookup_version_id)
        zone_dataframe = build.zone_frame(spark, zone_dimension)

        started = time.perf_counter()
        planned = plan(spark, options, store)
        timings["plan"] = round(time.perf_counter() - started, 3)

        decisions = decisions_for(options, planned)

        started = time.perf_counter()
        incremental = apply_plan(
            spark,
            options,
            Path(options.warehouse_root),
            planned,
            store,
            zone_dimension,
            zone_dataframe,
        )
        # The audit ledger advances only after the publication markers exist.
        # A failed build therefore cannot claim activation it never exposed.
        ledger_events = versions.reconcile(
            ledger, decisions, run_id=options.run_id, contract_versions=contract_versions
        )
        incremental_state = measure_tables(
            spark, options, Path(options.warehouse_root), zone_dimension.version_id
        )
        timings["incremental"] = round(time.perf_counter() - started, 3)

        started = time.perf_counter()
        shutil.rmtree(options.rebuild_root, ignore_errors=True)
        rebuild = apply_plan(
            spark,
            options,
            Path(options.rebuild_root),
            planned,
            store,
            zone_dimension,
            zone_dataframe,
        )
        rebuild_state = measure_tables(
            spark, options, Path(options.rebuild_root), zone_dimension.version_id
        )
        timings["rebuild"] = round(time.perf_counter() - started, 3)

        started = time.perf_counter()
        replay = apply_plan(
            spark,
            options,
            Path(options.warehouse_root),
            planned,
            store,
            zone_dimension,
            zone_dataframe,
        )
        replay_state = measure_tables(
            spark, options, Path(options.warehouse_root), zone_dimension.version_id
        )
        replay_ledger_events = versions.reconcile(
            ledger, decisions, run_id=options.run_id, contract_versions=contract_versions
        )
        timings["replay"] = round(time.perf_counter() - started, 3)

        report = _report(
            options,
            store,
            ledger=ledger,
            runtime=runtime,
            landed=landed,
            planned=planned,
            zone_dimension=zone_dimension,
            ledger_events=ledger_events,
            replay_ledger_events=replay_ledger_events,
            incremental=incremental,
            incremental_state=incremental_state,
            rebuild=rebuild,
            rebuild_state=rebuild_state,
            replay=replay,
            replay_state=replay_state,
            timings=timings,
        )
    finally:
        spark.stop()

    options.evidence_path.parent.mkdir(parents=True, exist_ok=True)
    options.evidence_path.write_text(
        json.dumps(report, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8"
    )
    return report


def _report(options: SliceOptions, store: landing.LandingStore, **observed: Any) -> dict[str, Any]:
    incremental_state = observed["incremental_state"]
    rebuild_state = observed["rebuild_state"]
    replay_state = observed["replay_state"]

    # Only logical facts are compared. Delta commit counts, file counts and file
    # bytes legitimately differ between an incremental history and a rebuild,
    # and requiring them to match would test the writer, not the result.
    incremental_logical = logical_state(incremental_state)
    rebuild_logical = logical_state(rebuild_state)
    per_table = {
        service: {
            kind: equivalence.compare(
                incremental_logical[service][kind],
                rebuild_logical[service][kind],
                left_name=INCREMENTAL,
                right_name=REBUILD,
            )
            for kind in incremental_logical[service]
        }
        for service in incremental_logical
    }
    replay_commits = {
        service: {
            kind: {
                "before": incremental_state[service][kind]["delta_version"],
                "after": replay_state[service][kind]["delta_version"],
            }
            for kind in incremental_state[service]
        }
        for service in incremental_state
    }

    return {
        "milestone": "m2",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "run_id": options.run_id,
        "scope": {
            "artifacts": [
                {"service": item.service, "period": item.period} for item in options.artifacts
            ],
            "trip_artifact_completeness": options.completeness,
            "executed_on": "locally landed bounded TLC samples and the official zone lookup",
            "not_executed": [
                "full monthly or annual TLC objects",
                "analytical products, dimensional models and Power BI",
                "measured scale comparisons against DuckDB",
                "cloud infrastructure of any provider",
            ],
        },
        "runtime": observed["runtime"],
        "contracts": {
            service: {
                "contract_version": contracts.SERVICE_CONTRACTS[service].contract_version,
                "fingerprint": contracts.SERVICE_CONTRACTS[service].fingerprint,
                "columns": len(contracts.SERVICE_CONTRACTS[service].columns),
                "fare_components": list(contracts.SERVICE_CONTRACTS[service].fare_components),
                "local_time_zone": contracts.SERVICE_CONTRACTS[service].local_time_zone,
            }
            for service in options.services
        },
        "zone_lookup": observed["zone_dimension"].as_document(),
        "landing": {
            key: {"state": result.state, "version_id": result.version_id}
            for key, result in observed["landed"].items()
        },
        "version_selection": [
            {
                "service": item.service,
                "period": item.period,
                "version_id": item.manifest["version_id"],
                "published_at_utc": item.manifest["published_at_utc"],
                "state": item.state,
                "accepted": item.resolved.accepted,
                "resolution_fingerprint": item.resolved.fingerprint,
                "violations": [violation.as_document() for violation in item.resolved.violations],
                "promotions": promotions(item.resolved),
                "unmapped_source_columns": list(item.resolved.unmapped_source_columns),
            }
            for item in observed["planned"]
        ],
        "version_ledger": {
            "relative_path": "version_ledger.jsonl",
            "first_pass_events": observed["ledger_events"],
            "replay_events": observed["replay_ledger_events"],
            "state": observed["ledger"].state(),
        },
        "incremental": observed["incremental"],
        "rebuild": observed["rebuild"],
        "replay": {
            **observed["replay"],
            "logical_state_unchanged": logical_state(incremental_state)
            == logical_state(replay_state),
            "delta_versions": replay_commits,
            "no_new_delta_commits": all(
                entry["before"] == entry["after"]
                for service in replay_commits.values()
                for entry in service.values()
            ),
        },
        "tables": {INCREMENTAL: incremental_state, REBUILD: rebuild_state},
        "equivalence": {
            "equivalent": all(
                table["equivalent"] for service in per_table.values() for table in service.values()
            ),
            "compared": "marker-visible schema, row counts, distinct technical keys "
            "and content digests",
            "not_compared": "Parquet and Delta file bytes, which differ between correct runs",
            "per_table": per_table,
        },
        "landing_events": {
            "all_runs": _event_counts(store.events()),
            "current_run": _event_counts(
                [event for event in store.events() if event["run_id"] == options.run_id]
            ),
        },
        "timings_seconds": observed["timings"],
        "limits": [
            "Bounded samples of 1,000 rows per source version; no month and no annual window.",
            "The rebuild comparison is logical; Delta and Parquet files are never expected "
            "to be byte-identical.",
            "Concurrency is measured by a separate probe and is not claimed here.",
            "The publication-marker protocol is validated only on the shared local "
            "filesystem used by Docker named volumes.",
            "The orchestration and JSONL audit ledger retain one coordinator.",
            "Analytical products, the dimensional model and Power BI remain M3 work.",
        ],
    }


def promotions(resolved: resolution.SchemaResolution) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {}
    for item in resolved.fields:
        grouped.setdefault(item.promotion or item.status, []).append(item.canonical_name)
    return {key: sorted(value) for key, value in sorted(grouped.items())}


def _event_counts(events: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for event in events:
        counts[event["state"]] = counts.get(event["state"], 0) + 1
    return counts


def slice_passed(report: dict[str, Any]) -> bool:
    return bool(
        report["equivalence"]["equivalent"]
        and report["replay"]["logical_state_unchanged"]
        and report["replay"]["no_new_delta_commits"]
        and not report["version_ledger"]["replay_events"]
    )
