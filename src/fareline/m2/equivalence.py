"""Compare an incrementally maintained table with a full rebuild.

Delta and Parquet files are not byte-identical between two correct runs: file
names, ordering, statistics and compression all vary. The comparison is
therefore logical -- schema, keys, counts and content digests -- and never
touches the physical files.

Two digests are produced. The sorted digest is exact but collects one hash per
row on the driver, so it is bounded. The additive digest is order-independent
and streams, so it stays available above that bound; it is a set checksum, not
a proof of equality.
"""

from __future__ import annotations

import hashlib
from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

DEFAULT_MAX_DIGEST_ROWS = 250_000


def canonical_row(columns: list[str]) -> Column:
    """Render a row without delimiter or null collisions.

    JSON escaping distinguishes embedded control characters, and retaining null
    fields distinguishes a missing value from the literal text ``null``.
    """
    return F.to_json(
        F.struct(*[F.col(f"`{name}`").alias(name) for name in columns]),
        {"ignoreNullFields": "false"},
    )


def schema_signature(frame: DataFrame) -> list[str]:
    return [f"{field.name}:{field.dataType.simpleString()}" for field in frame.schema.fields]


def digest(frame: DataFrame, *, max_rows: int = DEFAULT_MAX_DIGEST_ROWS) -> dict[str, Any]:
    """Digest a table's logical content, independent of physical layout."""
    columns = sorted(field.name for field in frame.schema.fields)
    hashed = frame.select(F.sha2(canonical_row(columns), 256).alias("row_hash"))
    totals = hashed.select(
        F.count("*").alias("rows"),
        F.count(F.col("row_hash")).alias("hashed_rows"),
        F.sum(F.conv(F.substring(F.col("row_hash"), 1, 15), 16, 10).cast("decimal(38,0)")).alias(
            "additive"
        ),
    ).collect()[0]
    rows = int(totals["rows"])
    additive = None if totals["additive"] is None else str(totals["additive"])

    result: dict[str, Any] = {
        "columns": columns,
        "schema": schema_signature(frame),
        "rows": rows,
        "additive_digest": additive,
    }
    if rows > max_rows:
        result["sorted_digest"] = None
        result["sorted_digest_skipped_reason"] = (
            f"{rows} rows exceed the {max_rows} row driver-side digest bound"
        )
        return result

    hashes = sorted(row["row_hash"] for row in hashed.collect())
    folded = hashlib.sha256()
    for item in hashes:
        folded.update(item.encode())
        folded.update(b"\n")
    result["sorted_digest"] = folded.hexdigest()
    return result


def key_digest(
    frame: DataFrame, key_columns: list[str], *, max_rows: int = DEFAULT_MAX_DIGEST_ROWS
) -> dict[str, Any]:
    """Digest only the technical key, so a key drift is reported separately."""
    keyed = frame.select(*[F.col(f"`{name}`") for name in key_columns])
    distinct = keyed.distinct().count()
    return {
        "key_columns": list(key_columns),
        "rows": keyed.count(),
        "distinct_keys": distinct,
        **{
            f"key_{name}": value
            for name, value in digest(keyed, max_rows=max_rows).items()
            if name in {"additive_digest", "sorted_digest"}
        },
    }


def compare(
    left: dict[str, Any], right: dict[str, Any], *, left_name: str, right_name: str
) -> dict[str, Any]:
    """Compare two measured table states field by field, hiding nothing."""
    fields = sorted(set(left) | set(right))
    comparisons: dict[str, Any] = {}
    for name in fields:
        left_value, right_value = left.get(name), right.get(name)
        comparisons[name] = {
            left_name: left_value,
            right_name: right_value,
            "match": left_value == right_value,
        }
    return {
        "equivalent": all(item["match"] for item in comparisons.values()),
        "fields": comparisons,
    }


def measure(
    frame: DataFrame, key_columns: list[str], *, max_rows: int = DEFAULT_MAX_DIGEST_ROWS
) -> dict[str, Any]:
    """Everything the equivalence check compares for one table."""
    content = digest(frame, max_rows=max_rows)
    keys = key_digest(frame, key_columns, max_rows=max_rows)
    return {
        "schema": content["schema"],
        "rows": content["rows"],
        "distinct_keys": keys["distinct_keys"],
        "content_sorted_digest": content["sorted_digest"],
        "content_additive_digest": content["additive_digest"],
        "key_sorted_digest": keys.get("key_sorted_digest"),
        "key_additive_digest": keys.get("key_additive_digest"),
    }
