"""Build the contracted service tables, their quarantine and their incidents.

Three Delta tables per service retain contracted rows, quarantined rows and the
incidents that explain both. They are physical history tables. The immutable
publication catalog selects the complete derivations readers may observe, so a
superseded version remains auditable without remaining visible.

The marker-visible view is a pure function of the landed manifests, the contract
and the taxi zone lookup version, so an incremental run and a full rebuild are
supposed to be indistinguishable. ``fareline.m2.equivalence`` checks that claim.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import unquote, urlparse

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from fareline.m1 import quality
from fareline.m2 import contracts, localtime, resolution, types, zones

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

PARTITION_COLUMN = "source_period"

ROW_SCOPE = "row"
VERSION_SCOPE = "version"

# Deterministic lineage only. A run id or an ingestion timestamp would make two
# correct runs produce different rows and would destroy the rebuild comparison.
LINEAGE_COLUMNS = (
    "derivation_id",
    "source_file_version_id",
    "source_row_ordinal",
    "source_service",
    "source_period",
    "source_artifact_completeness",
    "source_logical_url",
    "source_content_sha256",
    "contract_version",
    "contract_fingerprint",
    "zone_lookup_version_id",
)

ZONE_ATTRIBUTES = ("borough", "zone_name", "service_zone")

_ZONE_SCHEMA = T.StructType(
    [
        T.StructField("location_id", T.LongType(), False),
        T.StructField("borough", T.StringType(), True),
        T.StructField("zone_name", T.StringType(), True),
        T.StructField("service_zone", T.StringType(), True),
    ]
)

INCIDENT_SCHEMA = T.StructType(
    [
        T.StructField("incident_id", T.StringType(), False),
        T.StructField("source_service", T.StringType(), False),
        T.StructField("source_period", T.StringType(), False),
        T.StructField("source_file_version_id", T.StringType(), False),
        T.StructField("derivation_id", T.StringType(), False),
        T.StructField("source_row_ordinal", T.LongType(), True),
        T.StructField("contract_version", T.StringType(), False),
        T.StructField("scope", T.StringType(), False),
        T.StructField("rule", T.StringType(), False),
        T.StructField("severity", T.StringType(), False),
        T.StructField("action", T.StringType(), False),
        T.StructField("detail", T.StringType(), True),
    ]
)


@dataclass(frozen=True)
class TablePaths:
    contracted: Path
    quarantine: Path
    incidents: Path

    def all(self) -> tuple[Path, ...]:
        return (self.contracted, self.quarantine, self.incidents)


@dataclass(frozen=True)
class VersionOutcome:
    service: str
    period: str
    version_id: str
    derivation_id: str
    state: str
    action: str
    contracted_rows: int
    quarantined_rows: int
    incident_rows: int
    source_rows: int

    def as_document(self) -> dict[str, Any]:
        return {
            "service": self.service,
            "period": self.period,
            "version_id": self.version_id,
            "derivation_id": self.derivation_id,
            "state": self.state,
            "action": self.action,
            "contracted_rows": self.contracted_rows,
            "quarantined_rows": self.quarantined_rows,
            "incident_rows": self.incident_rows,
            "source_rows": self.source_rows,
        }


def as_uri(path: Path | str) -> str:
    return "file://" + str(PurePosixPath(str(path).replace("\\", "/")))


def use_physical_column_names(spark: SparkSession) -> None:
    """Make Spark report source column names exactly as the file spells them.

    Case folding belongs to the contract, not to the reader. With Spark's
    default case-insensitive resolution a file carrying both ``airport_fee`` and
    ``Airport_fee`` cannot even be described, so the ambiguity would surface as a
    reader error instead of a contract decision naming both columns.
    """
    spark.conf.set("spark.sql.caseSensitive", "true")


def table_paths(warehouse_root: Path | str, service: str) -> TablePaths:
    root = Path(warehouse_root)
    return TablePaths(
        contracted=root / CONTRACTED_RELATIVE_PATHS[service],
        quarantine=root / QUARANTINE_RELATIVE_PATHS[service],
        incidents=root / INCIDENT_RELATIVE_PATHS[service],
    )


def zone_frame(spark: SparkSession, dimension: zones.ZoneDimension) -> DataFrame:
    """Broadcastable dimension; 265 rows never justify a distributed scan."""
    return spark.createDataFrame(list(dimension.rows), schema=_ZONE_SCHEMA)


def read_source_version(spark: SparkSession, artifact_path: Path | str) -> DataFrame:
    """Read one landed artifact with the reader's own physical row position."""
    return spark.read.parquet(as_uri(artifact_path)).withColumn(
        "source_row_ordinal", F.col("_metadata.row_index").cast("long")
    )


def source_schema(
    spark: SparkSession, artifact_path: Path | str
) -> tuple[resolution.SourceColumn, ...]:
    """Read only the Parquet footer so a candidate can be judged before a scan."""
    return resolution.source_columns_from_spark(spark.read.parquet(as_uri(artifact_path)).schema)


def _canonical_column(field: resolution.FieldResolution) -> Column:
    target = types.spark_sql_name(field.target_type)
    if field.status == resolution.MAPPED:
        return F.col(f"`{field.source_name}`").cast(target).alias(field.canonical_name)
    # Absent, or present but physically untyped: a typed null is written and no
    # value or type is invented for it.
    return F.lit(None).cast(target).alias(field.canonical_name)


def _guard_violation(
    contract: contracts.ServiceContract, resolved: resolution.SchemaResolution
) -> Column:
    """True when a range-checked promotion would silently lose a value."""
    checks = [
        (F.col(f"`{field.source_name}`") < F.lit(-field.guard_bound))
        | (F.col(f"`{field.source_name}`") > F.lit(field.guard_bound))
        for field in resolved.guarded_fields
        if field.status == resolution.MAPPED and field.guard_bound is not None
    ]
    if not checks:
        return F.lit(False)
    combined = checks[0]
    for item in checks[1:]:
        combined = combined | item
    return F.coalesce(combined, F.lit(False))


def _interval_flag(
    column: Column, intervals: tuple[localtime.LocalInterval, ...], kind: str
) -> Column:
    matching = [item for item in intervals if item.kind == kind]
    if not matching:
        return F.lit(False)
    condition = F.lit(False)
    for item in matching:
        condition = condition | (
            (column >= F.lit(item.start).cast("timestamp_ntz"))
            & (column < F.lit(item.end).cast("timestamp_ntz"))
        )
    return F.coalesce(condition, F.lit(False))


def contracted_frame(
    frame: DataFrame,
    contract: contracts.ServiceContract,
    resolved: resolution.SchemaResolution,
    manifest: dict[str, Any],
    zone_dimension: DataFrame,
    intervals: tuple[localtime.LocalInterval, ...],
    derived_id: str,
) -> DataFrame:
    """Project one source version onto the contract and evaluate every rule."""
    year, month = quality.period_parts(manifest["period"])
    guard = _guard_violation(contract, resolved)

    projected = frame.select(
        F.col("source_row_ordinal"),
        F.lit(derived_id).alias("derivation_id"),
        F.lit(manifest["version_id"]).alias("source_file_version_id"),
        F.lit(manifest["service"]).alias("source_service"),
        F.lit(manifest["period"]).alias("source_period"),
        F.lit(manifest["completeness"]).alias("source_artifact_completeness"),
        F.lit(manifest["logical_url"]).alias("source_logical_url"),
        F.lit(manifest["content_sha256"]).alias("source_content_sha256"),
        F.lit(contract.contract_version).alias("contract_version"),
        F.lit(resolved.contract_fingerprint).alias("contract_fingerprint"),
        guard.alias("rule__guarded_promotion_out_of_range"),
        *[_canonical_column(field) for field in resolved.fields],
    )

    pickup = F.col(contract.pickup_timestamp)
    dropoff = F.col(contract.dropoff_timestamp)
    pickup_key = F.col(contract.pickup_zone_key)
    dropoff_key = F.col(contract.dropoff_zone_key)

    joined = _join_zones(projected, zone_dimension, contract)

    return (
        joined.withColumn("pickup_local_date", F.to_date(pickup))
        .withColumn("pickup_local_hour", F.hour(pickup).cast("int"))
        .withColumn("rule__pickup_timestamp_missing", pickup.isNull())
        .withColumn("rule__dropoff_timestamp_missing", dropoff.isNull())
        .withColumn(
            "rule__negative_duration",
            F.coalesce(dropoff < pickup, F.lit(False)),
        )
        .withColumn(
            "rule__pickup_out_of_declared_period",
            pickup.isNotNull() & ((F.year(pickup) != year) | (F.month(pickup) != month)),
        )
        .withColumn("rule__zone_key_missing", pickup_key.isNull() | dropoff_key.isNull())
        .withColumn(
            "rule__zone_key_unknown",
            (pickup_key.isNotNull() & ~F.col("pickup_zone_resolved"))
            | (dropoff_key.isNotNull() & ~F.col("dropoff_zone_resolved")),
        )
        .withColumn(
            "rule__local_time_nonexistent",
            _interval_flag(pickup, intervals, localtime.NONEXISTENT),
        )
        .withColumn(
            "rule__local_time_ambiguous",
            _interval_flag(pickup, intervals, localtime.AMBIGUOUS),
        )
    )


def _join_zones(
    frame: DataFrame, dimension: DataFrame, contract: contracts.ServiceContract
) -> DataFrame:
    """Left-join both zone keys, marking resolution explicitly.

    A left join alone cannot say whether a key was unknown or simply carries
    empty attributes, so the dimension contributes a presence marker instead of
    letting a null attribute stand in for a missing row.
    """
    marked = dimension.withColumn("zone_present", F.lit(True))
    result = frame
    for side, key in (
        ("pickup", contract.pickup_zone_key),
        ("dropoff", contract.dropoff_zone_key),
    ):
        renamed = marked.select(
            F.col("location_id").alias(f"_{side}_location_id"),
            *[F.col(name).alias(f"{side}_{name}") for name in ZONE_ATTRIBUTES],
            F.col("zone_present").alias(f"_{side}_present"),
        )
        result = result.join(
            F.broadcast(renamed),
            F.col(key) == F.col(f"_{side}_location_id"),
            "left",
        ).drop(f"_{side}_location_id")
        result = result.withColumn(
            f"{side}_zone_resolved", F.coalesce(F.col(f"_{side}_present"), F.lit(False))
        ).drop(f"_{side}_present")
    return result


def rule_columns(contract: contracts.ServiceContract) -> tuple[str, ...]:
    return tuple(f"rule__{rule.name}" for rule in contract.row_rules)


def output_columns(contract: contracts.ServiceContract) -> tuple[str, ...]:
    zone_columns = tuple(
        f"{side}_{name}"
        for side in ("pickup", "dropoff")
        for name in (*ZONE_ATTRIBUTES, "zone_resolved")
    )
    return (
        *LINEAGE_COLUMNS,
        *contract.canonical_names,
        "pickup_local_date",
        "pickup_local_hour",
        *zone_columns,
    )


def split(
    evaluated: DataFrame, contract: contracts.ServiceContract, zone_version_id: str
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Separate published rows, quarantined rows and their incidents."""
    quarantine_rules = [rule for rule in contract.row_rules if rule.action == contracts.QUARANTINE]
    blocked = F.lit(False)
    for rule in quarantine_rules:
        blocked = blocked | F.col(f"rule__{rule.name}")
    with_zone = evaluated.withColumn("zone_lookup_version_id", F.lit(zone_version_id))
    with_flag = with_zone.withColumn("_quarantined", blocked)

    selection = [F.col(name) for name in output_columns(contract)]
    published = with_flag.where(~F.col("_quarantined")).select(*selection)

    reasons = F.array_compact(
        F.array(
            *[F.when(F.col(f"rule__{rule.name}"), F.lit(rule.name)) for rule in quarantine_rules]
        )
    )
    quarantined = (
        with_flag.where(F.col("_quarantined"))
        .withColumn("quarantine_reasons", F.array_sort(reasons))
        .select(*selection, F.col("quarantine_reasons"))
    )

    incidents = _row_incidents(with_flag, contract)
    return published, quarantined, incidents


def _row_incidents(evaluated: DataFrame, contract: contracts.ServiceContract) -> DataFrame:
    findings = F.array_compact(
        F.array(
            *[
                F.when(
                    F.col(f"rule__{rule.name}"),
                    F.struct(
                        F.lit(rule.name).alias("rule"),
                        F.lit(rule.severity).alias("severity"),
                        F.lit(rule.action).alias("action"),
                    ),
                )
                for rule in contract.row_rules
            ]
        )
    )
    exploded = evaluated.withColumn("_finding", F.explode(findings))
    return exploded.select(
        F.sha2(
            F.concat_ws(
                "",
                F.col("source_file_version_id"),
                F.col("derivation_id"),
                F.col("source_row_ordinal").cast("string"),
                F.col("_finding.rule"),
            ),
            256,
        ).alias("incident_id"),
        F.col("source_service"),
        F.col("source_period"),
        F.col("source_file_version_id"),
        F.col("derivation_id"),
        F.col("source_row_ordinal"),
        F.col("contract_version"),
        F.lit(ROW_SCOPE).alias("scope"),
        F.col("_finding.rule").alias("rule"),
        F.col("_finding.severity").alias("severity"),
        F.col("_finding.action").alias("action"),
        F.lit(None).cast("string").alias("detail"),
    )


def version_incidents(
    spark: SparkSession,
    contract: contracts.ServiceContract,
    manifest: dict[str, Any],
    resolved: resolution.SchemaResolution,
    derived_id: str,
) -> DataFrame:
    """One incident per blocking schema violation of a rejected version."""
    rows = [
        (
            hashlib.sha256(
                "".join(
                    [derived_id, manifest["version_id"], violation.rule, violation.canonical_name]
                ).encode()
            ).hexdigest(),
            manifest["service"],
            manifest["period"],
            manifest["version_id"],
            derived_id,
            None,
            contract.contract_version,
            VERSION_SCOPE,
            violation.rule,
            "high",
            contracts.QUARANTINE,
            f"{violation.canonical_name}: {violation.detail}",
        )
        for violation in resolved.violations
    ]
    return spark.createDataFrame(rows, schema=INCIDENT_SCHEMA)


def referenced_data_files(spark: SparkSession, path: Path | str) -> list[Path]:
    """Local paths of the data files the transaction log currently points at.

    ``inputFiles`` reports what a reader would open, so comparing it against the
    filesystem checks publication rather than trusting the log.
    """
    uri = as_uri(path)
    if not DeltaTable.isDeltaTable(spark, uri):
        return []
    frame = spark.read.format("delta").load(uri)
    return [Path(unquote(urlparse(item).path)) for item in frame.inputFiles()]


def listed_data_files(spark: SparkSession, path: Path | str) -> int:
    """How many data files the transaction log claims the table currently has."""
    uri = as_uri(path)
    if not DeltaTable.isDeltaTable(spark, uri):
        return 0
    return int(DeltaTable.forPath(spark, uri).detail().collect()[0]["numFiles"])


def delta_version(spark: SparkSession, path: Path | str) -> int:
    uri = as_uri(path)
    if not DeltaTable.isDeltaTable(spark, uri):
        return -1
    return int(DeltaTable.forPath(spark, uri).history(1).select("version").collect()[0][0])


def write_partition(frame: DataFrame, path: Path | str, transaction_id: str) -> None:
    """Append with a Delta idempotent-write marker.

    ``txnAppId``/``txnVersion`` make the append itself idempotent inside the
    transaction log, so a replay or a duplicated driver cannot write the same
    source version twice even if both pass a read-side check first.
    """
    (
        frame.write.format("delta")
        .mode("append")
        .partitionBy(PARTITION_COLUMN)
        .option("txnAppId", transaction_id)
        .option("txnVersion", 1)
        .save(as_uri(path))
    )
