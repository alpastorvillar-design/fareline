"""Command line entry point for the Fareline M1 vertical slice.

``land`` needs only Python and DuckDB. ``run`` needs PySpark and Delta, so it
must be launched with ``spark-submit scripts/fareline_m1.py`` inside the Spark
image; Spark modules are imported lazily to keep ``land`` usable without them.
"""

from __future__ import annotations

import argparse
import json
import uuid
from collections.abc import Sequence
from pathlib import Path

DEFAULT_SOURCE_DIR = Path("/opt/fareline/samples")
DEFAULT_LANDING_ROOT = Path("/opt/fareline/data/landing")
DEFAULT_WAREHOUSE_ROOT = Path("/opt/fareline/warehouse")
DEFAULT_EVIDENCE_PATH = Path("/opt/fareline/output/evidence/m1/vertical_slice.json")
DEFAULT_M0_EVIDENCE = Path("/opt/fareline/evidence/m0/source_inventory.json")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--landing-root", type=Path, default=DEFAULT_LANDING_ROOT)
    parser.add_argument("--source-dir", type=Path, default=DEFAULT_SOURCE_DIR)
    parser.add_argument("--period", default="2024-01")
    parser.add_argument(
        "--service",
        action="append",
        choices=["yellow", "hvfhv"],
        default=None,
        help="repeatable; defaults to yellow and hvfhv",
    )
    parser.add_argument(
        "--completeness",
        choices=["bounded_sample", "synthetic_fixture"],
        default="bounded_sample",
        help="scope of the local derivative; complete upstream objects are not accepted here",
    )
    parser.add_argument("--skip-zone-lookup", action="store_true")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--m0-evidence", type=Path, default=DEFAULT_M0_EVIDENCE)

    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("land", help="acquire artifacts into versioned landing")

    run = subparsers.add_parser("run", help="land, ingest, replay, verify and compare")
    run.add_argument("--warehouse-root", type=Path, default=DEFAULT_WAREHOUSE_ROOT)
    run.add_argument("--evidence-out", type=Path, default=DEFAULT_EVIDENCE_PATH)
    run.add_argument("--max-alignment-rows", type=int, default=250_000)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    services = tuple(args.service or ("yellow", "hvfhv"))
    run_id = args.run_id or uuid.uuid4().hex

    if args.command == "land":
        # fareline.m1.sources deliberately avoids Spark, so landing works
        # outside a spark-submit; the pipeline import below does not.
        from fareline.m1 import landing, sources

        landed = sources.land_artifacts(
            landing.LandingStore(args.landing_root),
            source_dir=args.source_dir,
            period=args.period,
            services=services,
            completeness=args.completeness,
            acquire_zone_lookup=not args.skip_zone_lookup,
            run_id=run_id,
            m0_evidence_path=args.m0_evidence if args.m0_evidence.is_file() else None,
        )
        print(
            json.dumps(
                {
                    key: {"state": result.state, "version_id": result.version_id}
                    for key, result in landed.items()
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    from fareline.m1 import pipeline

    options = pipeline.SliceOptions(
        source_dir=args.source_dir,
        landing_root=args.landing_root,
        warehouse_root=args.warehouse_root,
        evidence_path=args.evidence_out,
        period=args.period,
        services=services,
        completeness=args.completeness,
        acquire_zone_lookup=not args.skip_zone_lookup,
        max_alignment_rows=args.max_alignment_rows,
        m0_evidence_path=args.m0_evidence if args.m0_evidence.is_file() else None,
        run_id=run_id,
    )
    report = pipeline.run_slice(options)
    print("FARELINE_M1_SUMMARY=" + json.dumps(_summary(report), sort_keys=True))
    return 0 if _slice_passed(report) else 1


def _summary(report: dict) -> dict:
    return {
        "run_id": report["run_id"],
        "replay_no_op": report["replay"]["no_op"],
        "source_occurrence_rows": {
            service: table["rows"] for service, table in report["source_occurrences"].items()
        },
        "oracle_all_match": {
            service: result["metrics"]["all_match"] for service, result in report["quality"].items()
        },
        "ordinal_alignment_match": {
            service: result["ordinal_alignment"].get("match")
            for service, result in report["quality"].items()
        },
        "period": report["scope"]["period"],
    }


def _slice_passed(report: dict) -> bool:
    quality_ok = all(
        result["metrics"]["all_match"] and result["ordinal_alignment"].get("match", False)
        for result in report["quality"].values()
    )
    return bool(report["replay"]["no_op"] and quality_ok)


if __name__ == "__main__":
    raise SystemExit(main())
