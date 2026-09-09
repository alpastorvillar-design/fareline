"""Integration smoke for the M2 contracted path.

Runs inside the Spark image on synthetic fixtures the script creates itself, so
it needs no TLC access and no credentials. It reproduces the shapes upstream is
known to publish -- a renamed column, widened and narrowed integer types, a new
column, an ambiguous rename and an incompatible type -- and checks that the
contract accepts, rejects, quarantines and supersedes exactly as declared, that
an incremental build matches a full rebuild, and that a replay changes nothing.
"""

from __future__ import annotations

import argparse
import json
import shutil
import uuid
from dataclasses import replace
from pathlib import Path

import duckdb
from pyspark.sql import functions as F

from fareline.m1 import landing, occurrences
from fareline.m1.sources import trip_identity, zone_identity
from fareline.m2 import (
    build,
    contracts,
    equivalence,
    paths,
    pipeline,
    resolution,
    types,
    versions,
    zones,
)

SERVICE = "yellow"
COMPLETENESS = "synthetic_fixture"
ROWS = 400


def zone_csv(zone_count: int) -> str:
    return "LocationID,Borough,Zone,service_zone\n" + "".join(
        f"{index},Borough {index},Zone {index},Boro Zone\n" for index in range(1, zone_count + 1)
    )


ZONE_CSV = zone_csv(50)
# A second published lookup, the way upstream reissuing the file would look.
ZONE_CSV_REVISED = zone_csv(60)


def fixture_sql(target: Path, *, legacy: bool, rows: int, fare_offset: float) -> str:
    """Emulate the two Yellow shapes upstream actually published.

    ``legacy`` is the pre-2023-07 shape: lower-case ``airport_fee``, 64-bit
    identifiers, DOUBLE ``passenger_count`` and no ``cbd_congestion_fee``.
    """
    airport = "airport_fee" if legacy else "Airport_fee"
    identifier = "BIGINT" if legacy else "INTEGER"
    passenger = "DOUBLE" if legacy else "BIGINT"
    passenger_value = (
        f"CAST(1 + (i % 4) AS {passenger})"
        if legacy
        else "CASE WHEN i = 3 THEN CAST(-9223372036854775808 AS BIGINT) "
        "ELSE CAST(1 + (i % 4) AS BIGINT) END"
    )
    extra_column = "" if legacy else ", CAST((i % 3) * 0.75 AS DOUBLE) AS cbd_congestion_fee"
    return f"""
        COPY (
            SELECT
                CAST(1 + (i % 2) AS {identifier}) AS VendorID,
                CASE WHEN i % 97 = 5 THEN NULL
                     WHEN i % 97 = 11 THEN TIMESTAMP '2023-12-24 09:00:00'
                     ELSE TIMESTAMP '2024-01-01 00:00:00' + INTERVAL (i) MINUTE
                END AS tpep_pickup_datetime,
                CASE WHEN i % 97 = 17
                     THEN TIMESTAMP '2024-01-01 00:00:00' + INTERVAL (i - 45) MINUTE
                     ELSE TIMESTAMP '2024-01-01 00:00:00' + INTERVAL (i + 9) MINUTE
                END AS tpep_dropoff_datetime,
                {passenger_value} AS passenger_count,
                CAST((i % 31) * 0.4 AS DOUBLE) AS trip_distance,
                CAST(1 + (i % 6) AS {passenger}) AS RatecodeID,
                CASE WHEN i % 5 = 0 THEN 'Y' ELSE 'N' END AS store_and_fwd_flag,
                CASE WHEN i % 97 = 23 THEN NULL
                     WHEN i % 97 = 29 THEN CAST(9001 AS {identifier})
                     ELSE CAST(1 + (i % 50) AS {identifier})
                END AS PULocationID,
                CAST(1 + ((i * 7) % 50) AS {identifier}) AS DOLocationID,
                CAST(1 + (i % 4) AS BIGINT) AS payment_type,
                CAST((i % 89) + {fare_offset} AS DOUBLE) AS fare_amount,
                CAST((i % 3) * 0.5 AS DOUBLE) AS extra,
                CAST(0.5 AS DOUBLE) AS mta_tax,
                CAST((i % 7) * 0.25 AS DOUBLE) AS tip_amount,
                CAST((i % 11) * 0.1 AS DOUBLE) AS tolls_amount,
                CAST(0.3 AS DOUBLE) AS improvement_surcharge,
                CAST((i % 89) + {fare_offset} + 2.0 AS DOUBLE) AS total_amount,
                CAST(2.5 AS DOUBLE) AS congestion_surcharge,
                CAST((i % 2) * 1.75 AS DOUBLE) AS {airport}{extra_column}
            FROM range(0, {rows}) t(i)
        ) TO '{target.as_posix()}' (FORMAT PARQUET)
    """


def write_fixture(target: Path, **kwargs: object) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(":memory:") as connection:
        connection.execute(fixture_sql(target, **kwargs))  # type: ignore[arg-type]


def write_ambiguous_fixture(spark: object, target: Path, rows: int) -> None:
    """A file that carries both spellings of the same canonical column.

    DuckDB deduplicates column names case-insensitively, so the collision has to
    be written by Spark, which the session already configures to treat source
    names as physical. Landing accepts one file per version, hence the single
    part file.
    """
    write_fixture(target, legacy=True, rows=rows, fare_offset=0.25)
    staging = target.parent / f"{target.stem}_staging"
    frame = spark.read.parquet(build.as_uri(target)).withColumn("Airport_fee", F.col("airport_fee"))
    frame.coalesce(1).write.mode("overwrite").parquet(build.as_uri(staging))
    part = next(item for item in staging.glob("*.parquet"))
    target.unlink()
    part.replace(target)
    shutil.rmtree(staging, ignore_errors=True)


def write_incompatible_fixture(target: Path, rows: int) -> None:
    """A file whose total_amount arrives as text instead of a number."""
    write_fixture(target, legacy=False, rows=rows, fare_offset=0.25)
    staged = target.with_name(f"{target.stem}_incompatible.parquet")
    with duckdb.connect(":memory:") as connection:
        connection.execute(
            f"COPY (SELECT * EXCLUDE (total_amount), "
            f"CAST(total_amount AS VARCHAR) AS total_amount "
            f"FROM read_parquet('{target.as_posix()}')) "
            f"TO '{staged.as_posix()}' (FORMAT PARQUET)"
        )
    staged.replace(target)


def land_trip(store: landing.LandingStore, source: Path, period: str, run_id: str) -> object:
    return landing.acquire(
        store,
        trip_identity(SERVICE, period, COMPLETENESS),
        landing.fetch_local_file(source),
        landing.validate_parquet,
        run_id=run_id,
    )


def land_zone_lookup(
    store: landing.LandingStore, root: Path, run_id: str, text: str = ZONE_CSV
) -> object:
    source = root / "taxi_zone_lookup.csv"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text(text, encoding="utf-8")
    return landing.acquire(
        store,
        zone_identity(),
        landing.fetch_local_file(source),
        landing.validate_zone_lookup,
        run_id=run_id,
    )


def options_for(
    root: Path, store: landing.LandingStore, periods: tuple[str, ...]
) -> pipeline.SliceOptions:
    return pipeline.SliceOptions(
        source_dir=root / "source",
        landing_root=store.root,
        warehouse_root=root / "warehouse",
        rebuild_root=root / "rebuild",
        evidence_path=root / "evidence.json",
        sample_evidence_paths=(),
        artifacts=tuple(pipeline.ArtifactRequest(SERVICE, period) for period in periods),
        completeness=COMPLETENESS,
    )


def states(planned: list[pipeline.VersionPlan]) -> dict[str, str]:
    return {f"{item.period}:{item.manifest['version_id'][:8]}": item.state for item in planned}


def boundary_for(
    options: pipeline.SliceOptions, dimension: zones.ZoneDimension
) -> versions.PublicationBoundary:
    catalog = versions.PublicationCatalog(options.warehouse_root)
    return pipeline.target_boundary(catalog, options, dimension.version_id)


def tables_for(options: pipeline.SliceOptions, service: str = SERVICE) -> paths.TablePaths:
    return paths.table_paths(
        options.warehouse_root, service, contracts.SERVICE_CONTRACTS[service].fingerprint
    )


def run_all(
    spark: object,
    options: pipeline.SliceOptions,
    store: landing.LandingStore,
    dimension: zones.ZoneDimension,
    frame: object,
) -> tuple[list, dict, dict]:
    catalog = versions.PublicationCatalog(options.warehouse_root)
    versions.prepare_layout(catalog)
    target = boundary_for(options, dimension)
    pipeline.check_publication_scope(catalog, options, target)
    planned = pipeline.plan(spark, options, store)
    applied = pipeline.apply_plan(
        spark, options, Path(options.warehouse_root), planned, store, dimension, frame
    )
    pipeline.settle_boundary(catalog, target)
    measured = pipeline.measure_tables(spark, options, Path(options.warehouse_root), target)
    return planned, applied, measured


def rebuild_and_compare(
    spark: object,
    options: pipeline.SliceOptions,
    planned: list,
    store: landing.LandingStore,
    dimension: zones.ZoneDimension,
    frame: object,
    incremental: dict,
) -> dict:
    paths.clear_rebuild_root(options.rebuild_root, protected=pipeline.protected_paths(options))
    rebuild_catalog = versions.PublicationCatalog(options.rebuild_root)
    versions.prepare_layout(rebuild_catalog)
    pipeline.apply_plan(
        spark, options, Path(options.rebuild_root), planned, store, dimension, frame
    )
    target = boundary_for(options, dimension)
    rebuild_catalog.install_boundary(target)
    rebuilt = pipeline.logical_state(
        pipeline.measure_tables(spark, options, Path(options.rebuild_root), target)
    )
    incremental = pipeline.logical_state(incremental)
    return {
        service: {
            kind: equivalence.compare(
                incremental[service][kind],
                rebuilt[service][kind],
                left_name="incremental",
                right_name="rebuild",
            )
            for kind in incremental[service]
        }
        for service in incremental
    }


def require_equivalent(comparison: dict, label: str) -> None:
    for service, tables in comparison.items():
        for kind, result in tables.items():
            if not result["equivalent"]:
                divergent = {
                    name: value for name, value in result["fields"].items() if not value["match"]
                }
                raise AssertionError(f"{label}: {service}.{kind} diverged: {divergent}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("/opt/fareline/output"))
    parser.add_argument("--rows", type=int, default=ROWS)
    parser.add_argument("--min-executor-hosts", type=int, default=1)
    args = parser.parse_args()

    root = Path(args.root) / f"m2-smoke-{uuid.uuid4().hex[:12]}"
    source = root / "source"
    store = landing.LandingStore(root / "landing")
    result: dict[str, object] = {}

    spark = occurrences.build_session("fareline-m2-smoke", root / "warehouse")
    spark.sparkContext.setLogLevel("WARN")
    build.use_physical_column_names(spark)
    try:
        hosts = occurrences.probe_executor_hosts(spark)
        if len(hosts) < args.min_executor_hosts:
            raise AssertionError(
                f"expected at least {args.min_executor_hosts} executor hosts, observed {hosts}"
            )
        result["executor_hosts"] = hosts

        land_zone_lookup(store, source, "smoke-zone")
        write_fixture(source / "legacy.parquet", legacy=True, rows=args.rows, fare_offset=0.25)
        write_fixture(source / "current.parquet", legacy=False, rows=args.rows, fare_offset=0.5)
        write_ambiguous_fixture(spark, source / "ambiguous.parquet", args.rows)
        write_incompatible_fixture(source / "incompatible.parquet", args.rows)

        first = land_trip(store, source / "legacy.parquet", "2024-01", "smoke-1")
        land_trip(store, source / "current.parquet", "2024-02", "smoke-1")
        land_trip(store, source / "ambiguous.parquet", "2024-03", "smoke-1")
        land_trip(store, source / "incompatible.parquet", "2024-04", "smoke-1")

        periods = ("2024-01", "2024-02", "2024-03", "2024-04")
        options = options_for(root, store, periods)
        dimension = zones.load(store)
        frame = build.zone_frame(spark, dimension)

        planned, applied, incremental = run_all(spark, options, store, dimension, frame)
        by_period = {item.period: item for item in planned}
        expected = {
            "2024-01": versions.ACTIVE,
            "2024-02": versions.ACTIVE,
            "2024-03": versions.REJECTED,
            "2024-04": versions.REJECTED,
        }
        observed = {period: item.state for period, item in by_period.items()}
        if observed != expected:
            raise AssertionError(f"unexpected version states: {observed}")
        ambiguous = by_period["2024-03"].resolved.violations[0]
        incompatible = by_period["2024-04"].resolved.violations[0]
        if ambiguous.rule != resolution.AMBIGUOUS_CANONICAL_COLUMN:
            raise AssertionError(f"expected an ambiguity violation, got {ambiguous.as_document()}")
        if incompatible.rule != resolution.INCOMPATIBLE_TYPE_DRIFT:
            raise AssertionError(
                f"expected an incompatible type violation, got {incompatible.as_document()}"
            )
        result["version_states"] = states(planned)
        result["compatible_drift_promotions"] = pipeline.promotions(by_period["2024-02"].resolved)

        contracted = incremental[SERVICE]["contracted"]
        quarantine = incremental[SERVICE]["quarantine"]
        incidents = incremental[SERVICE]["incidents"]
        if contracted["rows"] + quarantine["rows"] != 2 * args.rows:
            raise AssertionError(
                f"{contracted['rows']} contracted plus {quarantine['rows']} quarantined rows "
                f"do not account for the {2 * args.rows} rows of two accepted versions"
            )
        if quarantine["rows"] == 0 or incidents["rows"] == 0:
            raise AssertionError("the planted incidents produced no quarantine and no incident")
        incident_derivations = incidents["visible_derivations"]
        guarded_incidents = (
            spark.read.format("delta")
            .load(build.as_uri(tables_for(options).incidents))
            .where(F.col("derivation_id").isin(incident_derivations))
            .where(F.col("rule") == "guarded_promotion_out_of_range")
            .count()
        )
        if guarded_incidents != 1:
            raise AssertionError(
                f"expected LONG_MIN to produce one guarded-promotion incident, got "
                f"{guarded_incidents}"
            )
        result["first_pass"] = {
            "contracted_rows": contracted["rows"],
            "quarantined_rows": quarantine["rows"],
            "incident_rows": incidents["rows"],
            "guarded_promotion_incidents": guarded_incidents,
            "applied": applied,
        }

        require_equivalent(
            rebuild_and_compare(spark, options, planned, store, dimension, frame, incremental),
            "first pass",
        )

        replay_applied = pipeline.apply_plan(
            spark, options, Path(options.warehouse_root), planned, store, dimension, frame
        )
        replayed = pipeline.measure_tables(
            spark, options, Path(options.warehouse_root), boundary_for(options, dimension)
        )
        if pipeline.logical_state(incremental) != pipeline.logical_state(replayed):
            raise AssertionError("replaying the same plan changed the tables")
        for kind in pipeline.TABLE_KINDS:
            if (
                incremental[SERVICE][kind]["delta_version"]
                != replayed[SERVICE][kind]["delta_version"]
            ):
                raise AssertionError(f"replay added a Delta commit to {kind}")
        result["replay"] = {
            "logical_state_unchanged": True,
            "no_new_delta_commits": True,
            "applied": replay_applied,
        }

        # A corrected file for the same period must take over completely in the
        # published view. Physical derived history and landing keep both.
        contracted_path = tables_for(options).contracted
        old_contracted_rows = (
            spark.read.format("delta")
            .load(build.as_uri(contracted_path))
            .where(f"source_file_version_id = '{first.version_id}'")
            .count()
        )
        write_fixture(source / "legacy.parquet", legacy=True, rows=args.rows, fare_offset=0.95)
        second = land_trip(store, source / "legacy.parquet", "2024-01", "smoke-2")
        if second.version_id == first.version_id:
            raise AssertionError("changed content did not create a new landed version")
        planned, superseded_applied, after = run_all(spark, options, store, dimension, frame)
        live = {
            item.manifest["version_id"]: item.state for item in planned if item.period == "2024-01"
        }
        if live.get(first.version_id) != versions.SUPERSEDED:
            raise AssertionError(f"the first version was not superseded: {live}")
        retained = (
            spark.read.format("delta")
            .load(build.as_uri(contracted_path))
            .where(f"source_file_version_id = '{first.version_id}'")
            .count()
        )
        if retained != old_contracted_rows:
            raise AssertionError(
                f"physical history changed from {old_contracted_rows} to {retained} rows"
            )
        visible = after[SERVICE]["contracted"]["visible_derivations"]
        old_derivation = versions.derivation_id(
            first.version_id, contracts.YELLOW.fingerprint, dimension.version_id
        )
        if old_derivation in visible:
            raise AssertionError("the superseded derivation remains visible")
        if len(store.versions(trip_identity(SERVICE, "2024-01", COMPLETENESS))) != 2:
            raise AssertionError("landing did not keep both versions of the corrected file")
        require_equivalent(
            rebuild_and_compare(spark, options, planned, store, dimension, frame, after),
            "after supersession",
        )
        result["supersession"] = {
            "superseded_version_id": first.version_id,
            "active_version_id": second.version_id,
            "physical_history_rows_retained": retained,
            "physical_history_removed": superseded_applied["physical_history_removed"],
            "contracted_rows": after[SERVICE]["contracted"]["rows"],
        }

        # A scoped rerun must not retract periods omitted from --artifact.
        before_subset = pipeline.logical_state(after)
        subset_options = options_for(root, store, ("2024-01",))
        subset_plan = pipeline.plan(spark, subset_options, store)
        pipeline.apply_plan(
            spark,
            subset_options,
            Path(options.warehouse_root),
            subset_plan,
            store,
            dimension,
            frame,
        )
        after_subset = pipeline.logical_state(
            pipeline.measure_tables(
                spark, options, Path(options.warehouse_root), boundary_for(options, dimension)
            )
        )
        if before_subset != after_subset:
            raise AssertionError("a scoped rerun changed an omitted period")
        result["scoped_rerun_preserves_omitted_periods"] = True

        # Simulate a crash after the first of the three Delta writes. No marker
        # may expose that partial derivation; replay must finish and publish it.
        write_fixture(source / "partial.parquet", legacy=False, rows=args.rows, fare_offset=1.25)
        partial = land_trip(store, source / "partial.parquet", "2024-05", "smoke-partial")
        partial_root = root / "partial-publication"
        partial_options = options_for(partial_root, store, ("2024-05",))
        partial_catalog = versions.PublicationCatalog(partial_options.warehouse_root)
        versions.prepare_layout(partial_catalog)
        partial_plan = pipeline.plan(spark, partial_options, store)
        real_write = build.write_partition
        write_calls = 0

        def fail_second_write(*call_args: object, **call_kwargs: object) -> None:
            nonlocal write_calls
            write_calls += 1
            if write_calls == 2:
                raise RuntimeError("planted failure between Delta tables")
            real_write(*call_args, **call_kwargs)

        build.write_partition = fail_second_write
        try:
            try:
                pipeline.apply_plan(
                    spark,
                    partial_options,
                    Path(partial_options.warehouse_root),
                    partial_plan,
                    store,
                    dimension,
                    frame,
                )
            except RuntimeError as error:
                if "planted failure" not in str(error):
                    raise
            else:
                raise AssertionError("the planted partial-publication failure did not fire")
        finally:
            build.write_partition = real_write

        partial_identity = trip_identity(SERVICE, "2024-05", COMPLETENESS).logical_id
        if partial_catalog.entries(partial_identity):
            raise AssertionError("a partial set of Delta writes received a publication marker")
        versions.check_layout(partial_catalog)
        pipeline.apply_plan(
            spark,
            partial_options,
            Path(partial_options.warehouse_root),
            partial_plan,
            store,
            dimension,
            frame,
        )
        partial_catalog.install_boundary(boundary_for(partial_options, dimension))
        repaired = pipeline.measure_tables(
            spark,
            partial_options,
            Path(partial_options.warehouse_root),
            boundary_for(partial_options, dimension),
        )
        if (
            repaired[SERVICE]["contracted"]["rows"] + repaired[SERVICE]["quarantine"]["rows"]
            != args.rows
        ):
            raise AssertionError("repair did not publish the complete planted version")
        result["partial_publication"] = {
            "source_version_id": partial.version_id,
            "invisible_after_failure": True,
            "repair_published_complete_version": True,
        }

        result["incident_id_encoding_matches_python"] = incident_ids_agree(
            spark, tables_for(options).incidents
        )
        result["contract_revision"] = contract_revision(
            spark, root, store, dimension, frame, args.rows
        )
        result["zone_lookup_migration"] = zone_lookup_migration(
            spark, root, source, store, options, dimension
        )

        result["detects_missing_data_files"] = detects_missing_data_files(
            spark, options, tables_for(options).contracted
        )

        print("FARELINE_M2_SMOKE=" + json.dumps(result, sort_keys=True, default=str))
        return 0
    finally:
        spark.stop()
        shutil.rmtree(root, ignore_errors=True)


def incident_ids_agree(spark: object, incidents_path: Path) -> bool:
    """The Spark and Python encodings of an incident identifier must agree.

    Two implementations of one canonical encoding are only worth having if they
    are checked against each other, so a stored row-scope identifier is
    recomputed on the driver from the columns that produced it.
    """
    row = (
        spark.read.format("delta")
        .load(build.as_uri(incidents_path))
        .where(F.col("scope") == build.ROW_SCOPE)
        .limit(1)
        .collect()
    )
    if not row:
        raise AssertionError("the smoke produced no row-scope incident to check")
    stored = row[0]
    recomputed = versions.incident_id(
        [
            stored["source_file_version_id"],
            stored["derivation_id"],
            str(stored["source_row_ordinal"]),
            stored["rule"],
        ]
    )
    if recomputed != stored["incident_id"]:
        raise AssertionError(
            f"incident id {stored['incident_id']} was not reproduced by the Python encoding"
        )
    return True


def revised_contract(version: str, *, add_column: bool, retype_column: bool) -> object:
    """A contract revision of the kind that changes the derived output schema."""
    columns = contracts.YELLOW.columns
    if retype_column:
        columns = tuple(
            replace(item, target_type=types.FLOAT64, guarded_promotions=True)
            if item.canonical_name == "payment_type"
            else item
            for item in columns
        )
    if add_column:
        columns = (
            *columns,
            contracts.ColumnContract(
                canonical_name="requested_pickup_datetime",
                source_aliases=("request_datetime",),
                target_type=types.TIMESTAMP_NTZ,
                required=False,
                role="temporal",
            ),
        )
    return replace(contracts.YELLOW, contract_version=version, columns=columns)


def field_type(schema: list[str], name: str) -> str:
    """The Spark type of one derived column, from a measured schema signature."""
    return next(item.split(":", 1)[1] for item in schema if item.split(":", 1)[0] == name)


def contract_revision(
    spark: object,
    root: Path,
    store: landing.LandingStore,
    dimension: zones.ZoneDimension,
    frame: object,
    rows: int,
) -> dict:
    """Materialise two contract revisions that change the derived output schema.

    A new column and an incompatible column type are the two shapes Delta
    refuses to append to an existing table. Each revision writes its own tables,
    the previous history stays exactly as it was, and the boundary decides which
    revision readers are on.
    """
    options = options_for(root / "contract-evolution", store, ("2024-01", "2024-02"))
    catalog = versions.PublicationCatalog(options.warehouse_root)
    original = contracts.SERVICE_CONTRACTS[SERVICE]
    observed: dict[str, object] = {}
    try:
        _, _, first = run_all(spark, options, store, dimension, frame)
        baseline = paths.table_paths(options.warehouse_root, SERVICE, original.fingerprint)
        baseline_schema = first[SERVICE]["contracted"]["schema"]
        baseline_state = {
            "fingerprint": original.fingerprint,
            "rows": first[SERVICE]["contracted"]["rows"],
            "columns": len(baseline_schema),
            "delta_version": first[SERVICE]["contracted"]["delta_version"],
            "payment_type": field_type(baseline_schema, "payment_type"),
        }
        if baseline_state["rows"] + first[SERVICE]["quarantine"]["rows"] != 2 * rows:
            raise AssertionError("the contract-evolution baseline did not publish both periods")

        for label, revision in (
            (
                "added_output_column",
                revised_contract("yellow/v2", add_column=True, retype_column=False),
            ),
            (
                "retyped_output_column",
                revised_contract("yellow/v3", add_column=False, retype_column=True),
            ),
        ):
            contracts.SERVICE_CONTRACTS[SERVICE] = revision
            _, _, revised_state = run_all(spark, options, store, dimension, frame)
            contracted = revised_state[SERVICE]["contracted"]
            if catalog.boundary().fingerprint(SERVICE) != revision.fingerprint:
                raise AssertionError(f"{label}: the boundary did not move to the revision")
            if contracted["rows"] != baseline_state["rows"]:
                raise AssertionError(f"{label}: the revision published a different row count")
            # The earlier revision is untouched, not merged into and not dropped.
            retained = pipeline.measure_table(
                spark, options, baseline.contracted, pipeline.KEY_COLUMNS
            )
            if retained["rows"] != baseline_state["rows"]:
                raise AssertionError(f"{label}: the previous contract's history changed")
            before = contracted["delta_version"]
            _, _, replayed = run_all(spark, options, store, dimension, frame)
            if replayed[SERVICE]["contracted"]["delta_version"] != before:
                raise AssertionError(f"{label}: replaying the revision added a Delta commit")
            observed[label] = {
                "fingerprint": revision.fingerprint,
                "rows": contracted["rows"],
                "columns": len(contracted["schema"]),
                "payment_type": field_type(contracted["schema"], "payment_type"),
                "schema_differs_from_baseline": contracted["schema"] != baseline_schema,
                "replay_added_delta_commit": False,
            }
    finally:
        contracts.SERVICE_CONTRACTS[SERVICE] = original

    added = observed["added_output_column"]
    retyped = observed["retyped_output_column"]
    if added["columns"] != baseline_state["columns"] + 1:
        raise AssertionError("the added column did not reach the derived schema")
    # The retype keeps the column count, so the type itself is what proves it
    # reached the output rather than being silently ignored.
    if (baseline_state["payment_type"], retyped["payment_type"]) != ("bigint", "double"):
        raise AssertionError(
            f"the retyped column did not reach the derived schema: "
            f"{baseline_state['payment_type']} -> {retyped['payment_type']}"
        )
    if not all(item["schema_differs_from_baseline"] for item in (added, retyped)):
        raise AssertionError("a revision produced the baseline schema unchanged")
    if len({baseline_state["fingerprint"], added["fingerprint"], retyped["fingerprint"]}) != 3:
        raise AssertionError("two revisions produced the same contract fingerprint")
    return {"baseline": baseline_state, **observed}


def zone_lookup_migration(
    spark: object,
    root: Path,
    source: Path,
    store: landing.LandingStore,
    options: pipeline.SliceOptions,
    dimension: zones.ZoneDimension,
) -> dict:
    """Landing a newer lookup must not move the view, and moving it needs coverage.

    This is the condition the M2 review measured: the run followed the newest
    landed lookup, and every previously published period silently left the view.
    """
    catalog = versions.PublicationCatalog(options.warehouse_root)
    published = catalog.boundary().zone_lookup_version_id
    revised = land_zone_lookup(store, source, "smoke-zone-2", ZONE_CSV_REVISED)
    if revised.version_id == published:
        raise AssertionError("a different lookup body did not create a new landed version")
    if zones.select_version(store)["version_id"] != revised.version_id:
        raise AssertionError("the revised lookup is not the newest landed version")

    if pipeline.pinned_zone_version(catalog, options) != published:
        raise AssertionError("a newly landed lookup changed the default context")
    before = pipeline.measure_tables(
        spark, options, Path(options.warehouse_root), catalog.boundary()
    )
    visible_before = set(before[SERVICE]["contracted"]["visible_derivations"])
    if not visible_before:
        raise AssertionError("nothing was visible before the migration")

    # A run that rebuilds one period cannot move a dataset-wide dimension.
    scoped = options_for(root, store, ("2024-01",))
    scoped_target = pipeline.target_boundary(catalog, scoped, revised.version_id)
    try:
        pipeline.check_publication_scope(catalog, scoped, scoped_target)
    except versions.PublicationCoverageError as error:
        refusal = str(error)
    else:
        raise AssertionError("a scoped lookup migration was accepted")
    if catalog.boundary().zone_lookup_version_id != published:
        raise AssertionError("a refused migration moved the boundary")
    if pipeline.logical_state(before) != pipeline.logical_state(
        pipeline.measure_tables(spark, options, Path(options.warehouse_root), catalog.boundary())
    ):
        raise AssertionError("a refused migration changed the published view")

    migrated = zones.load(store, revised.version_id)
    _, _, after = run_all(spark, options, store, migrated, build.zone_frame(spark, migrated))
    if catalog.boundary().zone_lookup_version_id != revised.version_id:
        raise AssertionError("a complete migration did not move the boundary")
    coverage = pipeline.coverage(catalog, options, catalog.boundary())
    if not coverage["complete"]:
        raise AssertionError(f"the migration left {coverage['invisible_logical_ids']} invisible")
    visible_after = set(after[SERVICE]["contracted"]["visible_derivations"])
    if visible_after & visible_before:
        raise AssertionError("a derivation of the previous lookup is still visible")
    if after[SERVICE]["contracted"]["physical_rows"] <= before[SERVICE]["contracted"]["rows"]:
        raise AssertionError("the migration did not retain the previous physical history")
    return {
        "published_version_id": published,
        "revised_version_id": revised.version_id,
        "default_stayed_on_published_version": True,
        "scoped_migration_refused": refusal,
        "artifacts_covered_after_migration": len(coverage["known_artifacts"]),
        "physical_rows_after": after[SERVICE]["contracted"]["physical_rows"],
        "visible_rows_after": after[SERVICE]["contracted"]["rows"],
    }


def detects_missing_data_files(spark: object, options: pipeline.SliceOptions, path: Path) -> bool:
    """Delete the data files and confirm measurement refuses the result.

    A Delta log that still lists files nobody can read is the failure mode the
    M0 review reproduced for Parquet, so the guard is exercised rather than
    assumed. This runs last, on a table that is about to be deleted.
    """
    removed = 0
    for item in path.rglob("*.parquet"):
        if "_delta_log" not in item.parts:
            item.unlink()
            removed += 1
    if removed == 0:
        raise AssertionError("no data files were found to remove")
    try:
        pipeline.measure_table(spark, options, path, pipeline.KEY_COLUMNS)
    except RuntimeError:
        return True
    raise AssertionError("measurement accepted a Delta log without data files")


if __name__ == "__main__":
    raise SystemExit(main())
