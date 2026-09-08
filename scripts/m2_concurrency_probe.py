"""Measure what two concurrent Delta writers actually do on this storage.

M1 declared a single writer and did not claim more. This probe runs two
independent Spark drivers against one table on the shared volume and reports
what happened, so the concurrency claim in the documentation is measured rather
than assumed. It is destructive by design and must be pointed at a throwaway
table root, never at a table an earlier milestone produced.

Two scenarios are measured:

``distinct``
    Two drivers append two different source versions. Both committing, or one
    failing with a Delta concurrency exception, are safe outcomes. A lost
    commit or a duplicated row is not.

``same``
    Two drivers append the same source version with the same idempotent-write
    marker. Delta must keep exactly one copy.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
import traceback
from pathlib import Path

from pyspark.sql import types as T

from fareline.m1 import occurrences
from fareline.m2 import build

ROWS = 2_000
PERIOD = "2024-01"

SCHEMA = T.StructType(
    [
        T.StructField("source_file_version_id", T.StringType(), False),
        T.StructField("source_row_ordinal", T.LongType(), False),
        T.StructField("source_period", T.StringType(), False),
        T.StructField("payload", T.StringType(), False),
    ]
)


def version_for(scenario: str, writer: int) -> str:
    """One version id per writer, or one shared id when the race is a replay."""
    seed = "shared" if scenario == "same" else f"writer{writer}"
    return hashlib.sha256(seed.encode()).hexdigest()


def wait_for_peers(barrier: Path, writer: int, peers: int, timeout: float) -> float:
    """Line both drivers up so their commits actually overlap."""
    barrier.mkdir(parents=True, exist_ok=True)
    (barrier / f"{writer}.ready").write_text("ready", encoding="utf-8")
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        if len(list(barrier.glob("*.ready"))) >= peers:
            return round(time.monotonic() - start, 3)
        time.sleep(0.05)
    raise TimeoutError(f"writer {writer} waited {timeout}s for {peers} peers at {barrier}")


def describe_failure(error: BaseException) -> tuple[str, str]:
    """Name the JVM exception, because which failure happened is the result.

    A Delta concurrency exception means the protocol refused the write, which is
    safe. Anything else has to be reported as what it is.
    """
    java = getattr(error, "java_exception", None)
    if java is None:
        return type(error).__name__, str(error).splitlines()[0][:300]
    message = (java.getMessage() or "").splitlines()
    return java.getClass().getName(), (message[0][:300] if message else "")


def write(args: argparse.Namespace) -> dict[str, object]:
    spark = occurrences.build_session(f"fareline-m2-concurrency-{args.writer}", args.table.parent)
    spark.sparkContext.setLogLevel("WARN")
    try:
        version_id = version_for(args.scenario, args.writer)
        rows = [
            (version_id, ordinal, PERIOD, f"{args.scenario}-{version_id[:8]}-{ordinal}")
            for ordinal in range(args.rows)
        ]
        frame = spark.createDataFrame(rows, schema=SCHEMA)
        frame.count()  # Materialise before the barrier so the commit is the race.
        waited = wait_for_peers(args.barrier, args.writer, args.peers, args.timeout)
        started = time.perf_counter()
        try:
            build.write_partition(
                frame,
                args.table,
                f"fareline-m2-concurrency:{args.scenario}:{version_id}",
            )
            outcome, failure, detail = "committed", None, None
        except Exception as error:  # noqa: BLE001 - the failure class is the result
            outcome = "failed"
            failure, detail = describe_failure(error)
        return {
            "writer": args.writer,
            "scenario": args.scenario,
            "version_id": version_id,
            "rows_offered": args.rows,
            "barrier_wait_seconds": waited,
            "commit_seconds": round(time.perf_counter() - started, 3),
            "outcome": outcome,
            "failure_class": failure,
            "detail": detail,
        }
    finally:
        spark.stop()


def verify(args: argparse.Namespace) -> dict[str, object]:
    spark = occurrences.build_session("fareline-m2-concurrency-verify", args.table.parent)
    spark.sparkContext.setLogLevel("WARN")
    try:
        uri = build.as_uri(args.table)
        frame = spark.read.format("delta").load(uri)
        totals = frame.selectExpr(
            "count(*) AS rows",
            "count(DISTINCT source_file_version_id, source_row_ordinal) AS technical_keys",
            "count(DISTINCT source_file_version_id) AS versions",
        ).collect()[0]
        rows = int(totals["rows"])
        keys = int(totals["technical_keys"])
        found = int(totals["versions"])
        duplicated = rows != keys

        # One source version contributes one set of rows. Two drivers racing the
        # same version must still leave one set; two drivers racing different
        # versions leave one set each, for whichever of them committed.
        new_versions = 1 if args.scenario == "same" else args.committed_writers
        expected_versions = args.existing_versions + new_versions
        expected_rows = expected_versions * args.rows
        safe = not duplicated and found == expected_versions and rows == expected_rows
        explanation = (
            f"the table holds {expected_versions} source versions and exactly one row set each"
            if safe
            else f"{rows} rows and {found} versions where {expected_rows} rows and "
            f"{expected_versions} versions were required"
        )
        return {
            "scenario": args.scenario,
            "racing_writers": args.peers,
            "reported_commits": args.committed_writers,
            "rows": rows,
            "expected_rows": expected_rows,
            "distinct_technical_keys": keys,
            "distinct_versions": found,
            "expected_versions": expected_versions,
            "duplicated_keys": duplicated,
            "delta_version": build.delta_version(spark, args.table),
            "listed_data_files": build.listed_data_files(spark, args.table),
            "safe": safe,
            "explanation": explanation,
        }
    finally:
        spark.stop()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--role", choices=["write", "verify"], required=True)
    parser.add_argument("--scenario", choices=["distinct", "same"], required=True)
    parser.add_argument("--table", type=Path, required=True)
    parser.add_argument("--barrier", type=Path, required=True)
    parser.add_argument("--writer", type=int, default=1)
    parser.add_argument("--peers", type=int, default=2)
    parser.add_argument("--rows", type=int, default=ROWS)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument(
        "--committed-writers",
        type=int,
        default=2,
        help="verify only: how many racing writers reported a commit",
    )
    parser.add_argument(
        "--existing-versions",
        type=int,
        default=0,
        help="verify only: how many source versions the table already held",
    )
    args = parser.parse_args()

    try:
        result = write(args) if args.role == "write" else verify(args)
    except Exception:  # noqa: BLE001 - a probe must report its own failure
        print("FARELINE_M2_CONCURRENCY=" + json.dumps({"role": args.role, "error": True}))
        traceback.print_exc()
        return 2

    print("FARELINE_M2_CONCURRENCY=" + json.dumps(result, sort_keys=True))
    if args.role == "verify" and not result["safe"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
