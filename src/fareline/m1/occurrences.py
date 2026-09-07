"""Delta tables holding physical source occurrences.

One source-occurrence table per service. Source columns are preserved verbatim and only
lineage metadata is added. The technical key is
``(source_file_version_id, source_row_ordinal)``; the ordinal is the Parquet
reader's own physical row index, so it is produced by the scan itself and never
depends on partition ordering or a shuffle.
"""

from __future__ import annotations

import re
import socket
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from fareline.m1.landing import PUBLISHED, REPLAYED
from fareline.m1.quality import ServiceSpec, metric_names, metrics_sql

OCCURRENCE_RELATIVE_PATHS = {
    "yellow": "source_occurrences/yellow_trip_occurrence",
    "hvfhv": "source_occurrences/hvfhv_trip_occurrence",
}

PARTITION_COLUMN = "source_period"

_HEX = re.compile(r"\A[0-9a-f]{64}\Z")


@dataclass(frozen=True)
class IngestResult:
    state: str
    service: str
    version_id: str
    table_path: str
    rows_written: int
    delta_version: int


def as_uri(path: Path | str) -> str:
    """Build an explicit local URI so the driver and executors agree on the path."""
    return "file://" + str(PurePosixPath(str(path).replace("\\", "/")))


def build_session(app_name: str, warehouse_root: Path | str) -> SparkSession:
    """Create a Delta-enabled session.

    ``spark.jars.packages`` cannot be set after the JVM starts, so the Delta
    coordinate lives in the image's spark-defaults.conf; the catalog wiring is
    declared here to keep the application self-describing.
    """
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # TLC timestamps are wall-clock without an offset and are read as
        # TIMESTAMP_NTZ; a fixed session zone keeps every other timestamp
        # deterministic across hosts.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", as_uri(Path(warehouse_root) / "spark-sql"))
        .getOrCreate()
    )


def table_path(warehouse_root: Path | str, service: str) -> Path:
    return Path(warehouse_root) / OCCURRENCE_RELATIVE_PATHS[service]


def occurrence_frame(
    spark: SparkSession,
    artifact_path: Path | str,
    manifest: dict[str, Any],
    run_id: str,
    ingested_at: datetime,
) -> DataFrame:
    """Read one landed artifact as physical occurrences with lineage."""
    frame = spark.read.parquet(as_uri(artifact_path))
    return (
        frame
        # _metadata.row_index is the reader's physical row position inside the
        # Parquet file. A landed version is exactly one file, so it is the
        # physical source ordinal for that version.
        .withColumn("source_row_ordinal", F.col("_metadata.row_index").cast("long"))
        .withColumn("source_file_version_id", F.lit(manifest["version_id"]))
        .withColumn("source_service", F.lit(manifest["service"]))
        .withColumn("source_period", F.lit(manifest["period"]))
        .withColumn("source_artifact_completeness", F.lit(manifest["completeness"]))
        .withColumn("source_logical_url", F.lit(manifest["logical_url"]))
        .withColumn("source_content_sha256", F.lit(manifest["content_sha256"]))
        .withColumn("ingest_run_id", F.lit(run_id))
        .withColumn("ingested_at_utc", F.lit(ingested_at))
    )


def published_versions(spark: SparkSession, path: Path | str) -> set[str]:
    uri = as_uri(path)
    if not DeltaTable.isDeltaTable(spark, uri):
        return set()
    rows = (
        spark.read.format("delta").load(uri).select("source_file_version_id").distinct().collect()
    )
    return {row[0] for row in rows}


def published_completeness(spark: SparkSession, path: Path | str) -> set[str]:
    """Return the artifact scopes already admitted to one occurrence table."""
    uri = as_uri(path)
    if not DeltaTable.isDeltaTable(spark, uri):
        return set()
    rows = (
        spark.read.format("delta")
        .load(uri)
        .select("source_artifact_completeness")
        .distinct()
        .collect()
    )
    return {row[0] for row in rows}


def delta_version(spark: SparkSession, path: Path | str) -> int:
    uri = as_uri(path)
    if not DeltaTable.isDeltaTable(spark, uri):
        return -1
    history = DeltaTable.forPath(spark, uri).history(1).select("version").collect()
    return int(history[0][0])


def ingest_version(
    spark: SparkSession,
    service: str,
    warehouse_root: Path | str,
    artifact_path: Path | str,
    manifest: dict[str, Any],
    run_id: str,
) -> IngestResult:
    """Append one landed version to its source-occurrence table, or observe a replay.

    M1 declares a single writer. The published-version check followed by an
    append is not safe against a concurrent second driver; that guarantee is M2
    work on a storage implementation that can provide it.
    """
    path = table_path(warehouse_root, service)
    uri = as_uri(path)
    version = manifest["version_id"]
    completeness = manifest["completeness"]
    existing_completeness = published_completeness(spark, path)
    if existing_completeness and existing_completeness != {completeness}:
        raise RuntimeError(
            f"{path} contains artifact completeness {sorted(existing_completeness)}; "
            f"refusing to mix {completeness} into the same source-occurrence table"
        )
    if version in published_versions(spark, path):
        return IngestResult(REPLAYED, service, version, str(path), 0, delta_version(spark, path))

    frame = occurrence_frame(spark, artifact_path, manifest, run_id, datetime.now(timezone.utc))
    _assert_dense_ordinals(frame, manifest)
    frame.write.format("delta").mode("append").partitionBy(PARTITION_COLUMN).save(uri)
    rows = int(manifest["content_profile"]["rows"])
    return IngestResult(PUBLISHED, service, version, str(path), rows, delta_version(spark, path))


def _assert_dense_ordinals(frame: DataFrame, manifest: dict[str, Any]) -> None:
    """Fail loudly if the reader's row index is not a dense 0..n-1 sequence."""
    stats = frame.selectExpr(
        "count(*) AS rows",
        "count(DISTINCT source_row_ordinal) AS distinct_ordinals",
        "min(source_row_ordinal) AS min_ordinal",
        "max(source_row_ordinal) AS max_ordinal",
    ).collect()[0]
    expected = int(manifest["content_profile"]["rows"])
    if (
        stats["rows"] != expected
        or stats["distinct_ordinals"] != expected
        or stats["min_ordinal"] != 0
        or stats["max_ordinal"] != expected - 1
    ):
        raise RuntimeError(
            f"physical ordinals are not dense for {manifest['logical_id']}: "
            f"expected {expected} rows numbered 0..{expected - 1}, observed {stats.asDict()}"
        )


def verify_table(
    spark: SparkSession,
    service: str,
    warehouse_root: Path | str,
    version_ids: list[str],
    expected_rows: int,
    expected_completeness: str,
) -> dict[str, Any]:
    """Re-read the published table and prove the data files really exist."""
    path = table_path(warehouse_root, service)
    uri = as_uri(path)
    log_dir = path / "_delta_log"
    data_files = sorted(item for item in path.rglob("*.parquet") if "_delta_log" not in item.parts)
    if log_dir.is_dir() and not data_files:
        raise RuntimeError(f"{path} has a Delta log but no data files")
    if not log_dir.is_dir():
        raise RuntimeError(f"{path} has no Delta log")

    frame = spark.read.format("delta").load(uri)
    totals = frame.selectExpr(
        "count(*) AS rows",
        "count(DISTINCT source_file_version_id, source_row_ordinal) AS technical_keys",
        "count(DISTINCT source_file_version_id) AS versions",
        "count(DISTINCT source_artifact_completeness) AS completeness_values",
        "min(source_artifact_completeness) AS artifact_completeness",
    ).collect()[0]
    if totals["rows"] != expected_rows:
        raise RuntimeError(f"{path} re-read {totals['rows']} rows, expected {expected_rows}")
    if totals["technical_keys"] != totals["rows"]:
        raise RuntimeError(f"{path} has duplicate technical keys")
    if totals["versions"] != len(version_ids):
        raise RuntimeError(
            f"{path} holds {totals['versions']} versions, expected {len(version_ids)}"
        )
    if (
        totals["completeness_values"] != 1
        or totals["artifact_completeness"] != expected_completeness
    ):
        raise RuntimeError(
            f"{path} has artifact completeness {totals['artifact_completeness']!r}; "
            f"expected only {expected_completeness!r}"
        )
    return {
        "table_path": str(path),
        "delta_log_present": True,
        "data_files": len(data_files),
        "data_file_bytes": sum(item.stat().st_size for item in data_files),
        "rows": int(totals["rows"]),
        "distinct_technical_keys": int(totals["technical_keys"]),
        "distinct_versions": int(totals["versions"]),
        "artifact_completeness": totals["artifact_completeness"],
        "delta_version": delta_version(spark, path),
    }


def spark_metrics(
    spark: SparkSession,
    spec: ServiceSpec,
    warehouse_root: Path | str,
    version_id: str,
    period: str,
) -> dict[str, int | None]:
    """Run the shared quality rules over one published version in Delta."""
    relation = _version_relation(warehouse_root, spec.service, version_id)
    row = spark.sql(metrics_sql(spec, relation, period)).collect()[0]
    return {name: (None if row[name] is None else int(row[name])) for name in metric_names(spec)}


def spark_alignment(
    spark: SparkSession,
    spec: ServiceSpec,
    warehouse_root: Path | str,
    version_id: str,
) -> list[tuple[Any, ...]]:
    """Read technical ordinals and probe columns from the published table."""
    relation = _version_relation(warehouse_root, spec.service, version_id)
    columns = ", ".join(spec.alignment_columns)
    query = f"SELECT source_row_ordinal, {columns} FROM {relation} ORDER BY source_row_ordinal"
    return [tuple(row) for row in spark.sql(query).collect()]


def _version_relation(warehouse_root: Path | str, service: str, version_id: str) -> str:
    if not _HEX.match(version_id):
        raise ValueError(f"version id is not a sha256 digest: {version_id}")
    uri = as_uri(table_path(warehouse_root, service))
    return (
        f"(SELECT * FROM delta.`{uri}` WHERE source_file_version_id = '{version_id}')"
        " AS source_occurrence_version"
    )


def probe_executor_hosts(spark: SparkSession, partitions: int = 16) -> list[str]:
    """Report the hosts that actually executed a small probe stage.

    This is a diagnostic for the evidence file, not a scheduling guarantee: it
    names the hosts that ran the probe, not every host that ran the ingest.
    """
    hosts = (
        spark.sparkContext.parallelize(range(partitions), partitions)
        .map(lambda _: socket.gethostname())
        .distinct()
        .collect()
    )
    return sorted(hosts)
