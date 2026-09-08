"""Executable service contracts for Yellow Taxi and HVFHV.

A contract is data, not code: canonical column names, the source spellings they
accept, one explicit target type each, and the row rules that decide whether a
row is published, flagged or quarantined. Yellow and HVFHV keep separate
contracts and separate fare components, so no code path can add unlike money
together.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from fareline.m2 import types

# What a failing row rule does. An incident keeps the row and records the
# finding; quarantine moves the row out of the contracted table into an
# auditable table, and never deletes it from landing or from source occurrences.
INCIDENT = "incident"
QUARANTINE = "quarantine"


@dataclass(frozen=True)
class ColumnContract:
    """One canonical column, the source spellings it accepts, and its type."""

    canonical_name: str
    source_aliases: tuple[str, ...]
    target_type: str
    required: bool
    role: str
    # Opt-in for promotions that are lossless only inside a checked range.
    guarded_promotions: bool = False

    @property
    def alias_keys(self) -> frozenset[str]:
        return frozenset(alias.lower() for alias in self.source_aliases)


@dataclass(frozen=True)
class RowRule:
    """A row-level contract check and what a failure does to the row."""

    name: str
    action: str
    severity: str
    description: str


@dataclass(frozen=True)
class ServiceContract:
    service: str
    contract_version: str
    columns: tuple[ColumnContract, ...]
    row_rules: tuple[RowRule, ...]
    pickup_timestamp: str
    dropoff_timestamp: str
    pickup_zone_key: str
    dropoff_zone_key: str
    fare_components: tuple[str, ...]
    grain: str
    local_time_zone: str = "America/New_York"

    def column(self, canonical_name: str) -> ColumnContract:
        for item in self.columns:
            if item.canonical_name == canonical_name:
                return item
        raise KeyError(f"{self.service} contract has no column {canonical_name}")

    @property
    def canonical_names(self) -> tuple[str, ...]:
        return tuple(item.canonical_name for item in self.columns)

    def rule(self, name: str) -> RowRule:
        for item in self.row_rules:
            if item.name == name:
                return item
        raise KeyError(f"{self.service} contract has no rule {name}")

    def as_document(self) -> dict[str, Any]:
        """Canonical, order-stable description used for the fingerprint."""
        return {
            "service": self.service,
            "contract_version": self.contract_version,
            "grain": self.grain,
            "local_time_zone": self.local_time_zone,
            "pickup_timestamp": self.pickup_timestamp,
            "dropoff_timestamp": self.dropoff_timestamp,
            "pickup_zone_key": self.pickup_zone_key,
            "dropoff_zone_key": self.dropoff_zone_key,
            "fare_components": list(self.fare_components),
            "columns": [
                {
                    "canonical_name": item.canonical_name,
                    "source_aliases": sorted(item.alias_keys),
                    "target_type": item.target_type,
                    "required": item.required,
                    "role": item.role,
                    "guarded_promotions": item.guarded_promotions,
                }
                for item in self.columns
            ],
            "row_rules": [
                {"name": item.name, "action": item.action, "severity": item.severity}
                for item in self.row_rules
            ],
        }

    @property
    def fingerprint(self) -> str:
        payload = json.dumps(self.as_document(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode()).hexdigest()


def _column(
    canonical_name: str,
    aliases: tuple[str, ...],
    target_type: str,
    *,
    required: bool = False,
    role: str = "measure",
    guarded: bool = False,
) -> ColumnContract:
    return ColumnContract(
        canonical_name=canonical_name,
        source_aliases=aliases,
        target_type=target_type,
        required=required,
        role=role,
        guarded_promotions=guarded,
    )


def _row_rules() -> tuple[RowRule, ...]:
    """Both services expose the same failure modes; each owns its own rules."""
    return (
        RowRule(
            "pickup_timestamp_missing",
            QUARANTINE,
            "high",
            "A trip with no pickup instant cannot be placed on the local timeline.",
        ),
        RowRule(
            "negative_duration",
            QUARANTINE,
            "high",
            "Drop-off strictly before pickup is impossible at this grain.",
        ),
        RowRule(
            "guarded_promotion_out_of_range",
            QUARANTINE,
            "high",
            "An integer outside the exactly representable range of its target type.",
        ),
        RowRule(
            "pickup_out_of_declared_period",
            INCIDENT,
            "medium",
            "Pickup falls outside the calendar month named by the source file.",
        ),
        RowRule(
            "dropoff_timestamp_missing",
            INCIDENT,
            "medium",
            "Duration is unresolved; the occurrence is real and is kept.",
        ),
        RowRule(
            "zone_key_missing",
            INCIDENT,
            "medium",
            "A pickup or drop-off zone key is null in the source.",
        ),
        RowRule(
            "zone_key_unknown",
            INCIDENT,
            "medium",
            "A zone key is absent from the joined taxi zone lookup version.",
        ),
        RowRule(
            "local_time_nonexistent",
            INCIDENT,
            "medium",
            "The wall clock falls in the spring-forward hour, which never occurred.",
        ),
        RowRule(
            "local_time_ambiguous",
            INCIDENT,
            "low",
            "The wall clock falls in the fall-back hour and names two instants.",
        ),
    )


YELLOW = ServiceContract(
    service="yellow",
    contract_version="yellow/v1",
    grain="one physical source row inside one landed source-file version",
    pickup_timestamp="pickup_datetime",
    dropoff_timestamp="dropoff_datetime",
    pickup_zone_key="pickup_location_id",
    dropoff_zone_key="dropoff_location_id",
    fare_components=(
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "congestion_surcharge",
        "airport_fee",
        "cbd_congestion_fee",
        "total_amount",
    ),
    row_rules=_row_rules(),
    columns=(
        _column("vendor_id", ("VendorID",), types.INT64, role="code"),
        _column(
            "pickup_datetime",
            ("tpep_pickup_datetime",),
            types.TIMESTAMP_NTZ,
            required=True,
            role="temporal",
        ),
        _column(
            "dropoff_datetime",
            ("tpep_dropoff_datetime",),
            types.TIMESTAMP_NTZ,
            required=True,
            role="temporal",
        ),
        # Observed as DOUBLE through 2023-01 and BIGINT afterwards, so the
        # target is the wider type and the integer form is a guarded promotion.
        _column("passenger_count", ("passenger_count",), types.FLOAT64, guarded=True),
        _column("trip_distance", ("trip_distance",), types.FLOAT64),
        _column("ratecode_id", ("RatecodeID",), types.FLOAT64, role="code", guarded=True),
        _column("store_and_fwd_flag", ("store_and_fwd_flag",), types.STRING, role="flag"),
        _column(
            "pickup_location_id",
            ("PULocationID",),
            types.INT64,
            required=True,
            role="zone_key",
        ),
        _column(
            "dropoff_location_id",
            ("DOLocationID",),
            types.INT64,
            required=True,
            role="zone_key",
        ),
        _column("payment_type", ("payment_type",), types.INT64, role="code"),
        _column("fare_amount", ("fare_amount",), types.FLOAT64, required=True),
        _column("extra", ("extra",), types.FLOAT64),
        _column("mta_tax", ("mta_tax",), types.FLOAT64),
        _column("tip_amount", ("tip_amount",), types.FLOAT64),
        _column("tolls_amount", ("tolls_amount",), types.FLOAT64),
        _column("improvement_surcharge", ("improvement_surcharge",), types.FLOAT64),
        _column("total_amount", ("total_amount",), types.FLOAT64, required=True),
        _column("congestion_surcharge", ("congestion_surcharge",), types.FLOAT64),
        # Renamed in place between 2023-01 and 2023-07. Both spellings are
        # listed so the rename stays documented; matching is case-insensitive,
        # so a file carrying both at once is ambiguous instead of silently
        # resolved to one of them.
        _column("airport_fee", ("airport_fee", "Airport_fee"), types.FLOAT64),
        _column("cbd_congestion_fee", ("cbd_congestion_fee",), types.FLOAT64),
    ),
)


HVFHV = ServiceContract(
    service="hvfhv",
    contract_version="hvfhv/v1",
    grain="one physical source row inside one landed source-file version",
    pickup_timestamp="pickup_datetime",
    dropoff_timestamp="dropoff_datetime",
    pickup_zone_key="pickup_location_id",
    dropoff_zone_key="dropoff_location_id",
    fare_components=(
        "base_passenger_fare",
        "tolls",
        "bcf",
        "sales_tax",
        "congestion_surcharge",
        "airport_fee",
        "cbd_congestion_fee",
        "tips",
        "driver_pay",
    ),
    row_rules=_row_rules(),
    columns=(
        _column("hvfhs_license_num", ("hvfhs_license_num",), types.STRING, role="code"),
        _column("dispatching_base_num", ("dispatching_base_num",), types.STRING, role="code"),
        _column("originating_base_num", ("originating_base_num",), types.STRING, role="code"),
        _column("request_datetime", ("request_datetime",), types.TIMESTAMP_NTZ, role="temporal"),
        _column("on_scene_datetime", ("on_scene_datetime",), types.TIMESTAMP_NTZ, role="temporal"),
        _column(
            "pickup_datetime",
            ("pickup_datetime",),
            types.TIMESTAMP_NTZ,
            required=True,
            role="temporal",
        ),
        _column(
            "dropoff_datetime",
            ("dropoff_datetime",),
            types.TIMESTAMP_NTZ,
            required=True,
            role="temporal",
        ),
        _column(
            "pickup_location_id",
            ("PULocationID",),
            types.INT64,
            required=True,
            role="zone_key",
        ),
        _column(
            "dropoff_location_id",
            ("DOLocationID",),
            types.INT64,
            required=True,
            role="zone_key",
        ),
        _column("trip_miles", ("trip_miles",), types.FLOAT64),
        _column("trip_time", ("trip_time",), types.INT64),
        _column("base_passenger_fare", ("base_passenger_fare",), types.FLOAT64, required=True),
        _column("tolls", ("tolls",), types.FLOAT64),
        _column("bcf", ("bcf",), types.FLOAT64),
        _column("sales_tax", ("sales_tax",), types.FLOAT64),
        _column("congestion_surcharge", ("congestion_surcharge",), types.FLOAT64),
        _column("airport_fee", ("airport_fee",), types.FLOAT64),
        _column("tips", ("tips",), types.FLOAT64),
        _column("driver_pay", ("driver_pay",), types.FLOAT64, required=True),
        _column("shared_request_flag", ("shared_request_flag",), types.STRING, role="flag"),
        _column("shared_match_flag", ("shared_match_flag",), types.STRING, role="flag"),
        _column("access_a_ride_flag", ("access_a_ride_flag",), types.STRING, role="flag"),
        _column("wav_request_flag", ("wav_request_flag",), types.STRING, role="flag"),
        _column("wav_match_flag", ("wav_match_flag",), types.STRING, role="flag"),
        _column("cbd_congestion_fee", ("cbd_congestion_fee",), types.FLOAT64),
    ),
)


SERVICE_CONTRACTS: dict[str, ServiceContract] = {"yellow": YELLOW, "hvfhv": HVFHV}
