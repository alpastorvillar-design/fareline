"""Resolve one physical source schema against a service contract.

This is pure Python on purpose. The same resolver runs against the DuckDB
schemas recorded in M0 evidence and against a live Spark scan, so historical
drift can be replayed in a unit test without Spark and without a download.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from fareline.m2 import types
from fareline.m2.contracts import ColumnContract, ServiceContract

MAPPED = "mapped"
ABSENT_IN_SOURCE = "absent_in_source"
UNTYPED_NULL_SOURCE = "untyped_null_source"

MISSING_REQUIRED_COLUMN = "missing_required_column"
AMBIGUOUS_CANONICAL_COLUMN = "ambiguous_canonical_column"
NARROWING_TYPE_DRIFT = "narrowing_type_drift"
INCOMPATIBLE_TYPE_DRIFT = "incompatible_type_drift"
UNSUPPORTED_SOURCE_TYPE = "unsupported_source_type"
GUARDED_PROMOTION_NOT_ALLOWED = "guarded_promotion_not_allowed"
UNREADABLE_SOURCE_SCHEMA = "unreadable_source_schema"


@dataclass(frozen=True)
class SourceColumn:
    """One column as the reader reports it, before any contract is applied."""

    name: str
    type_token: str
    ordinal: int


@dataclass(frozen=True)
class FieldResolution:
    canonical_name: str
    target_type: str
    status: str
    source_name: str | None
    source_type: str | None
    promotion: str | None
    guard_bound: int | None

    def as_document(self) -> dict[str, Any]:
        return {
            "canonical_name": self.canonical_name,
            "target_type": self.target_type,
            "status": self.status,
            "source_name": self.source_name,
            "source_type": self.source_type,
            "promotion": self.promotion,
            "guard_bound": self.guard_bound,
        }


@dataclass(frozen=True)
class SchemaViolation:
    rule: str
    canonical_name: str
    detail: str

    def as_document(self) -> dict[str, str]:
        return {"rule": self.rule, "canonical_name": self.canonical_name, "detail": self.detail}


@dataclass(frozen=True)
class SchemaResolution:
    service: str
    contract_version: str
    contract_fingerprint: str
    fields: tuple[FieldResolution, ...]
    violations: tuple[SchemaViolation, ...]
    unmapped_source_columns: tuple[str, ...]

    @property
    def accepted(self) -> bool:
        return not self.violations

    def field(self, canonical_name: str) -> FieldResolution:
        for item in self.fields:
            if item.canonical_name == canonical_name:
                return item
        raise KeyError(f"{self.service} resolution has no field {canonical_name}")

    @property
    def guarded_fields(self) -> tuple[FieldResolution, ...]:
        return tuple(item for item in self.fields if item.promotion == types.GUARDED)

    def as_document(self) -> dict[str, Any]:
        return {
            "service": self.service,
            "contract_version": self.contract_version,
            "contract_fingerprint": self.contract_fingerprint,
            "accepted": self.accepted,
            "fields": [item.as_document() for item in self.fields],
            "violations": [item.as_document() for item in self.violations],
            "unmapped_source_columns": list(self.unmapped_source_columns),
        }

    @property
    def fingerprint(self) -> str:
        """Stable digest of the resolution, so evidence can bind to it."""
        payload = json.dumps(self.as_document(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode()).hexdigest()


def source_columns_from_duckdb(schema: list[dict[str, Any]]) -> tuple[SourceColumn, ...]:
    """Read M0 inventory schema records into the shared vocabulary."""
    return tuple(
        SourceColumn(
            name=item["name"],
            type_token=(
                types.UNTYPED_NULL
                if item.get("logical_type") == "NullType()"
                else types.from_duckdb(item["duckdb_type"])
            ),
            ordinal=int(item["source_ordinal"]),
        )
        for item in schema
    )


def source_columns_from_spark(schema: Any) -> tuple[SourceColumn, ...]:
    """Read a live ``DataFrame.schema`` into the shared vocabulary."""
    return tuple(
        SourceColumn(
            name=field.name,
            type_token=types.from_spark(field.dataType.simpleString()),
            ordinal=ordinal,
        )
        for ordinal, field in enumerate(schema.fields)
    )


def _matches(column: ColumnContract, source: tuple[SourceColumn, ...]) -> list[SourceColumn]:
    keys = column.alias_keys
    return [item for item in source if item.name.lower() in keys]


def _resolve_one(
    column: ColumnContract, matches: list[SourceColumn]
) -> tuple[FieldResolution, list[SchemaViolation]]:
    if len(matches) > 1:
        names = sorted(item.name for item in matches)
        return (
            FieldResolution(
                column.canonical_name, column.target_type, ABSENT_IN_SOURCE, None, None, None, None
            ),
            [
                SchemaViolation(
                    AMBIGUOUS_CANONICAL_COLUMN,
                    column.canonical_name,
                    f"source columns {names} all map to one canonical column",
                )
            ],
        )

    if not matches:
        resolution = FieldResolution(
            column.canonical_name, column.target_type, ABSENT_IN_SOURCE, None, None, None, None
        )
        if column.required:
            return resolution, [
                SchemaViolation(
                    MISSING_REQUIRED_COLUMN,
                    column.canonical_name,
                    f"none of {sorted(column.alias_keys)} is present in the source",
                )
            ]
        return resolution, []

    match = matches[0]
    if match.type_token not in types.KNOWN_TYPES:
        return (
            FieldResolution(
                column.canonical_name,
                column.target_type,
                ABSENT_IN_SOURCE,
                match.name,
                None,
                None,
                None,
            ),
            [
                SchemaViolation(
                    UNSUPPORTED_SOURCE_TYPE,
                    column.canonical_name,
                    f"{match.name} has unsupported type {match.type_token}",
                )
            ],
        )

    promotion = types.classify(match.type_token, column.target_type)
    bound = types.guard_bound(match.type_token, column.target_type)

    if promotion == types.UNTYPED_NULL_CAST:
        # Every value is null and the source declares no type. The contract type
        # is applied to typed nulls; no type is inferred from the data.
        return (
            FieldResolution(
                column.canonical_name,
                column.target_type,
                UNTYPED_NULL_SOURCE,
                match.name,
                match.type_token,
                promotion,
                None,
            ),
            [],
        )

    if promotion == types.NARROWING:
        return (
            FieldResolution(
                column.canonical_name,
                column.target_type,
                ABSENT_IN_SOURCE,
                match.name,
                match.type_token,
                promotion,
                None,
            ),
            [
                SchemaViolation(
                    NARROWING_TYPE_DRIFT,
                    column.canonical_name,
                    f"{match.name} is {match.type_token}, wider than the contract "
                    f"{column.target_type}; narrowing is never applied silently",
                )
            ],
        )

    if promotion == types.INCOMPATIBLE:
        return (
            FieldResolution(
                column.canonical_name,
                column.target_type,
                ABSENT_IN_SOURCE,
                match.name,
                match.type_token,
                promotion,
                None,
            ),
            [
                SchemaViolation(
                    INCOMPATIBLE_TYPE_DRIFT,
                    column.canonical_name,
                    f"{match.name} is {match.type_token} and cannot become {column.target_type}",
                )
            ],
        )

    if promotion == types.GUARDED and not column.guarded_promotions:
        return (
            FieldResolution(
                column.canonical_name,
                column.target_type,
                ABSENT_IN_SOURCE,
                match.name,
                match.type_token,
                promotion,
                bound,
            ),
            [
                SchemaViolation(
                    GUARDED_PROMOTION_NOT_ALLOWED,
                    column.canonical_name,
                    f"{match.name} needs a range-checked promotion the contract does not allow",
                )
            ],
        )

    return (
        FieldResolution(
            column.canonical_name,
            column.target_type,
            MAPPED,
            match.name,
            match.type_token,
            promotion,
            bound if promotion == types.GUARDED else None,
        ),
        [],
    )


def resolve(contract: ServiceContract, source: tuple[SourceColumn, ...]) -> SchemaResolution:
    """Resolve a source schema, collecting every violation instead of the first."""
    fields: list[FieldResolution] = []
    violations: list[SchemaViolation] = []
    claimed: set[str] = set()

    for column in contract.columns:
        matches = _matches(column, source)
        claimed.update(item.name for item in matches)
        field, found = _resolve_one(column, matches)
        fields.append(field)
        violations.extend(found)

    # Columns the contract does not mention are not an error: the source
    # occurrence keeps them verbatim, and the contracted table stays a declared
    # surface rather than whatever the newest file happens to carry.
    unmapped = tuple(item.name for item in source if item.name not in claimed)

    return SchemaResolution(
        service=contract.service,
        contract_version=contract.contract_version,
        contract_fingerprint=contract.fingerprint,
        fields=tuple(fields),
        violations=tuple(violations),
        unmapped_source_columns=unmapped,
    )


def unreadable(contract: ServiceContract, reason: str) -> SchemaResolution:
    """A candidate whose schema cannot be read is refused, not skipped."""
    return SchemaResolution(
        service=contract.service,
        contract_version=contract.contract_version,
        contract_fingerprint=contract.fingerprint,
        fields=(),
        violations=(SchemaViolation(UNREADABLE_SOURCE_SCHEMA, "*", reason),),
        unmapped_source_columns=(),
    )
