"""A small physical type vocabulary shared by the contract rules.

Two readers describe the same Parquet file differently, so contract resolution
works on canonical tokens instead of one engine's type names: DuckDB names come
from the M0 inventory, Spark names come from a live scan. Keeping the vocabulary
tiny is deliberate; Fareline only contracts scalar TLC columns.
"""

from __future__ import annotations

INT32 = "int32"
INT64 = "int64"
FLOAT32 = "float32"
FLOAT64 = "float64"
STRING = "string"
BOOLEAN = "boolean"
TIMESTAMP_NTZ = "timestamp_ntz"
DATE = "date"
# A Parquet column written with the logical Null type. Every value is null and
# the source declares no usable type; it is never guessed from the data.
UNTYPED_NULL = "untyped_null"

KNOWN_TYPES = frozenset(
    {INT32, INT64, FLOAT32, FLOAT64, STRING, BOOLEAN, TIMESTAMP_NTZ, DATE, UNTYPED_NULL}
)

IDENTITY = "identity"
WIDENING = "widening"
GUARDED = "guarded"
UNTYPED_NULL_CAST = "untyped_null_cast"
NARROWING = "narrowing"
INCOMPATIBLE = "incompatible"

# Promotions that cannot lose a value for any input of the source type.
_WIDENING: frozenset[tuple[str, str]] = frozenset(
    {
        (INT32, INT64),
        (INT32, FLOAT64),
        (FLOAT32, FLOAT64),
        (DATE, TIMESTAMP_NTZ),
    }
)

# Promotions that are lossless only inside a range, so they must be checked per
# row. int64 -> float64 is exact only up to 2^53.
_GUARDED: dict[tuple[str, str], int] = {(INT64, FLOAT64): 2**53}

# Reverse widening is narrowing, which is always refused rather than truncated.
_NARROWING: frozenset[tuple[str, str]] = (
    frozenset({(target, source) for source, target in _WIDENING})
    | frozenset({(target, source) for source, target in _GUARDED})
    | frozenset({(FLOAT64, INT32), (FLOAT32, INT32), (INT64, INT32)})
)

_DUCKDB_TYPES = {
    "TINYINT": INT32,
    "SMALLINT": INT32,
    "INTEGER": INT32,
    "BIGINT": INT64,
    "FLOAT": FLOAT32,
    "DOUBLE": FLOAT64,
    "VARCHAR": STRING,
    "BOOLEAN": BOOLEAN,
    "TIMESTAMP": TIMESTAMP_NTZ,
    "DATE": DATE,
    "NULL": UNTYPED_NULL,
}

_SPARK_TYPES = {
    "byte": INT32,
    "short": INT32,
    "integer": INT32,
    "int": INT32,
    "long": INT64,
    "bigint": INT64,
    "float": FLOAT32,
    "double": FLOAT64,
    "string": STRING,
    "boolean": BOOLEAN,
    "timestamp_ntz": TIMESTAMP_NTZ,
    "date": DATE,
    "void": UNTYPED_NULL,
    "null": UNTYPED_NULL,
}

_SPARK_SQL_NAMES = {
    INT32: "int",
    INT64: "bigint",
    FLOAT32: "float",
    FLOAT64: "double",
    STRING: "string",
    BOOLEAN: "boolean",
    TIMESTAMP_NTZ: "timestamp_ntz",
    DATE: "date",
}


class UnknownSourceType(ValueError):
    """A source column uses a type the contract vocabulary does not cover."""


def from_duckdb(type_name: str) -> str:
    """Map a DuckDB type name, as recorded in the M0 inventory, to a token."""
    cleaned = type_name.strip().strip('"').upper()
    if cleaned not in _DUCKDB_TYPES:
        raise UnknownSourceType(f"unsupported DuckDB source type: {type_name}")
    return _DUCKDB_TYPES[cleaned]


def from_spark(type_name: str) -> str:
    """Map a Spark ``DataType.simpleString()`` to a token."""
    cleaned = type_name.strip().lower()
    if cleaned.startswith("timestamp_ntz"):
        cleaned = "timestamp_ntz"
    if cleaned not in _SPARK_TYPES:
        raise UnknownSourceType(f"unsupported Spark source type: {type_name}")
    return _SPARK_TYPES[cleaned]


def spark_sql_name(token: str) -> str:
    """Render a contract target type as Spark SQL text for an explicit cast."""
    if token not in _SPARK_SQL_NAMES:
        raise UnknownSourceType(f"{token} is not a valid contract target type")
    return _SPARK_SQL_NAMES[token]


def guard_bound(source: str, target: str) -> int | None:
    """Return the absolute value bound a guarded promotion must respect."""
    return _GUARDED.get((source, target))


def classify(source: str, target: str) -> str:
    """Classify the promotion required to read ``source`` as ``target``."""
    for token in (source, target):
        if token not in KNOWN_TYPES:
            raise UnknownSourceType(f"unknown type token: {token}")
    if source == UNTYPED_NULL:
        return UNTYPED_NULL_CAST
    if source == target:
        return IDENTITY
    if (source, target) in _WIDENING:
        return WIDENING
    if (source, target) in _GUARDED:
        return GUARDED
    if (source, target) in _NARROWING:
        return NARROWING
    return INCOMPATIBLE
