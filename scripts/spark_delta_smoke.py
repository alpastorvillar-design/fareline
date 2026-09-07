"""Integration smoke for the M1 Delta source-occurrence path.

Runs inside the Spark image on synthetic fixtures the test creates itself, so
it needs no TLC access and no credentials. It proves that a landed version
becomes a readable Delta table, that an exact replay changes nothing, that new
content becomes a second version, and that Spark and DuckDB agree on the
service-specific quality rules.
"""

from __future__ import annotations

import argparse
import json
import shutil
import uuid
from pathlib import Path

import duckdb

from fareline.m1 import landing, occurrences, quality, sources

SERVICE = "yellow"
PERIOD = "2024-01"
FIXTURE_ROWS = 5_000


def write_fixture(target: Path, rows: int, fare_offset: float) -> None:
    """Generate one deterministic Parquet file with planted quality incidents."""
    target.parent.mkdir(parents=True, exist_ok=True)
    query = f"""
        COPY (
            SELECT
                CASE WHEN i % 500 = 3
                     THEN TIMESTAMP '2023-12-31 23:50:00'
                     ELSE TIMESTAMP '2024-01-01 00:00:00' + INTERVAL (i) MINUTE
                END AS tpep_pickup_datetime,
                CASE WHEN i % 500 = 7
                     THEN TIMESTAMP '2024-01-01 00:00:00' + INTERVAL (i - 30) MINUTE
                     ELSE TIMESTAMP '2024-01-01 00:00:00' + INTERVAL (i + 11) MINUTE
                END AS tpep_dropoff_datetime,
                CASE WHEN i % 500 = 11 THEN NULL ELSE CAST(1 + (i % 200) AS INTEGER)
                END AS PULocationID,
                CAST(1 + ((i * 7) % 200) AS INTEGER) AS DOLocationID,
                CASE WHEN i % 500 = 13 THEN NULL
                     ELSE CAST((i % 97) + {fare_offset} AS DOUBLE)
                END AS fare_amount,
                CAST((i % 7) * 0.5 AS DOUBLE) AS tip_amount,
                CASE WHEN i % 500 = 13 THEN NULL
                     ELSE CAST((i % 97) + {fare_offset} + (i % 7) * 0.5 + 1.5 AS DOUBLE)
                END AS total_amount
            FROM range(0, {rows}) t(i)
        ) TO '{target.as_posix()}' (FORMAT PARQUET)
    """
    with duckdb.connect(":memory:") as connection:
        connection.execute(query)


def land_fixture(store: landing.LandingStore, source_dir: Path, run_id: str) -> object:
    landed = sources.land_artifacts(
        store,
        source_dir=source_dir,
        period=PERIOD,
        services=(SERVICE,),
        completeness="synthetic_fixture",
        acquire_zone_lookup=False,
        run_id=run_id,
    )
    return landed[SERVICE]


def detects_missing_data_files(
    spark: object, warehouse: Path, version_ids: list[str], rows: int
) -> bool:
    """Destroy the table's data files and confirm verification refuses it.

    A Delta log that still lists files nobody can read is the failure mode the
    M0 review reproduced for Parquet, so the guard is exercised rather than
    assumed. This runs last, on a table that is about to be deleted.
    """
    table = occurrences.table_path(warehouse, SERVICE)
    removed = 0
    for item in table.rglob("*.parquet"):
        if "_delta_log" not in item.parts:
            item.unlink()
            removed += 1
    if removed == 0:
        raise AssertionError("no data files were found to remove")
    try:
        occurrences.verify_table(spark, SERVICE, warehouse, version_ids, rows, "synthetic_fixture")
    except RuntimeError:
        return True
    raise AssertionError("verification accepted a Delta log without data files")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("/opt/fareline/output"))
    parser.add_argument("--rows", type=int, default=FIXTURE_ROWS)
    parser.add_argument("--min-executor-hosts", type=int, default=1)
    args = parser.parse_args()

    root = Path(args.root) / f"m1-delta-smoke-{uuid.uuid4().hex[:12]}"
    source_dir = root / "source"
    store = landing.LandingStore(root / "landing")
    warehouse = root / "warehouse"
    identity = sources.trip_identity(SERVICE, PERIOD, "synthetic_fixture")
    fixture = source_dir / identity.filename

    spark = occurrences.build_session("fareline-m1-delta-smoke", warehouse)
    spark.sparkContext.setLogLevel("WARN")
    result: dict[str, object] = {"spark_version": spark.version, "rows_per_version": args.rows}
    try:
        hosts = occurrences.probe_executor_hosts(spark)
        if len(hosts) < args.min_executor_hosts:
            raise AssertionError(
                f"expected at least {args.min_executor_hosts} executor hosts, observed {hosts}"
            )
        result["executor_hosts"] = hosts

        write_fixture(fixture, args.rows, fare_offset=0.25)
        first = land_fixture(store, source_dir, "smoke-1")
        if first.state != landing.PUBLISHED:
            raise AssertionError(f"first acquisition was {first.state}")
        ingested = occurrences.ingest_version(
            spark, SERVICE, warehouse, first.artifact_path, first.manifest, "smoke-1"
        )
        state = occurrences.verify_table(
            spark, SERVICE, warehouse, [first.version_id], args.rows, "synthetic_fixture"
        )
        result["first_pass"] = {
            "state": ingested.state,
            "delta_version": state["delta_version"],
            "rows": state["rows"],
            "data_files": state["data_files"],
        }

        replayed_landing = land_fixture(store, source_dir, "smoke-2")
        replayed_ingest = occurrences.ingest_version(
            spark,
            SERVICE,
            warehouse,
            replayed_landing.artifact_path,
            replayed_landing.manifest,
            "smoke-2",
        )
        replay_state = occurrences.verify_table(
            spark, SERVICE, warehouse, [first.version_id], args.rows, "synthetic_fixture"
        )
        if replayed_landing.state != landing.REPLAYED or replayed_ingest.state != landing.REPLAYED:
            raise AssertionError(
                f"replay was not a no-op: landing={replayed_landing.state} "
                f"source_occurrences={replayed_ingest.state}"
            )
        if replay_state != state:
            raise AssertionError(f"replay changed the table: {state} -> {replay_state}")
        result["replay"] = {
            "landing": replayed_landing.state,
            "source_occurrences": replayed_ingest.state,
        }

        write_fixture(fixture, args.rows, fare_offset=0.75)
        second = land_fixture(store, source_dir, "smoke-3")
        if second.state != landing.PUBLISHED or second.version_id == first.version_id:
            raise AssertionError("changed content did not create a new version")
        occurrences.ingest_version(
            spark, SERVICE, warehouse, second.artifact_path, second.manifest, "smoke-3"
        )
        both = occurrences.verify_table(
            spark,
            SERVICE,
            warehouse,
            [first.version_id, second.version_id],
            args.rows * 2,
            "synthetic_fixture",
        )
        if both["delta_version"] <= state["delta_version"]:
            raise AssertionError("a new version did not create a new Delta commit")
        result["second_version"] = {
            "rows": both["rows"],
            "versions": both["distinct_versions"],
            "delta_version": both["delta_version"],
        }
        if len(store.versions(identity)) != 2:
            raise AssertionError("landing did not keep both manifests")

        bounded_identity = sources.trip_identity(SERVICE, PERIOD, "bounded_sample")
        bounded = landing.acquire(
            store,
            bounded_identity,
            landing.fetch_local_file(fixture),
            landing.validate_parquet,
            run_id="smoke-mixed-scope",
        )
        try:
            occurrences.ingest_version(
                spark,
                SERVICE,
                warehouse,
                bounded.artifact_path,
                bounded.manifest,
                "smoke-mixed-scope",
            )
        except RuntimeError as error:
            if "refusing to mix" not in str(error):
                raise
            result["rejects_mixed_artifact_completeness"] = True
        else:
            raise AssertionError("source-occurrence table accepted mixed artifact completeness")

        spec = quality.SERVICE_SPECS[SERVICE]
        comparison = quality.compare_metrics(
            occurrences.spark_metrics(spark, spec, warehouse, first.version_id, PERIOD),
            quality.duckdb_metrics(spec, first.artifact_path, PERIOD),
        )
        if not comparison["all_match"]:
            raise AssertionError(f"Spark and DuckDB disagree: {comparison}")
        alignment = quality.compare_alignment(
            occurrences.spark_alignment(spark, spec, warehouse, first.version_id),
            quality.duckdb_alignment(spec, first.artifact_path),
        )
        if not alignment["match"]:
            raise AssertionError(f"physical ordinals are misaligned: {alignment}")
        result["oracle"] = {
            "metrics_match": comparison["all_match"],
            "ordinal_alignment_match": alignment["match"],
            "pickup_out_of_period_rows": comparison["metrics"]["pickup_out_of_period_rows"][
                "delta_spark"
            ],
            "negative_duration_rows": comparison["metrics"]["negative_duration_rows"][
                "delta_spark"
            ],
            "null_pickup_zone_rows": comparison["metrics"]["null_pickup_zone_rows"]["delta_spark"],
        }

        result["detects_missing_data_files"] = detects_missing_data_files(
            spark, warehouse, [first.version_id, second.version_id], args.rows * 2
        )

        print("FARELINE_M1_SMOKE=" + json.dumps(result, sort_keys=True))
        return 0
    finally:
        spark.stop()
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
