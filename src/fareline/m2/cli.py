"""Command line entry point for the Fareline M2 contracted slice.

PySpark is available only inside a submitted application, so this is launched
through ``spark-submit scripts/fareline_m2.py``. The ``contracts`` subcommand
needs no Spark and prints the executable contract as data.
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
DEFAULT_REBUILD_ROOT = Path("/opt/fareline/output/m2-rebuild")
DEFAULT_EVIDENCE_PATH = Path("/opt/fareline/output/evidence/m2/contracted_slice.json")
DEFAULT_SAMPLE_EVIDENCE = (
    Path("/opt/fareline/evidence/m0/source_inventory.json"),
    Path("/opt/fareline/evidence/m2/drift_samples.json"),
)

# One drifting period plus the two M0 periods per service. Yellow 2023-01 and
# HVFHV 2019-02 are the historical cases the contract rules exist for.
DEFAULT_ARTIFACTS = (
    "yellow:2023-01",
    "yellow:2024-01",
    "yellow:2025-01",
    "hvfhv:2019-02",
    "hvfhv:2024-01",
    "hvfhv:2025-01",
)


def parse_artifact(value: str) -> tuple[str, str]:
    try:
        service, period = value.split(":", 1)
    except ValueError as error:
        raise argparse.ArgumentTypeError("an artifact must be service:YYYY-MM") from error
    if service not in {"yellow", "hvfhv"}:
        raise argparse.ArgumentTypeError(f"unknown service {service}")
    return service, period


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("contracts", help="print the executable service contracts")

    run = subparsers.add_parser(
        "run", help="land, contract, rebuild, compare and replay the bounded slice"
    )
    run.add_argument("--source-dir", type=Path, default=DEFAULT_SOURCE_DIR)
    run.add_argument("--landing-root", type=Path, default=DEFAULT_LANDING_ROOT)
    run.add_argument("--warehouse-root", type=Path, default=DEFAULT_WAREHOUSE_ROOT)
    run.add_argument("--rebuild-root", type=Path, default=DEFAULT_REBUILD_ROOT)
    run.add_argument("--evidence-out", type=Path, default=DEFAULT_EVIDENCE_PATH)
    run.add_argument(
        "--sample-evidence",
        action="append",
        type=Path,
        default=None,
        help="repeatable; files recording the expected hash of each bounded sample",
    )
    run.add_argument(
        "--artifact",
        action="append",
        type=parse_artifact,
        default=None,
        help=f"repeatable service:YYYY-MM; defaults to {' '.join(DEFAULT_ARTIFACTS)}",
    )
    run.add_argument(
        "--zone-lookup-version",
        default=None,
        help=(
            "pin the taxi zone lookup version; without it a run stays on the version the "
            "publication boundary already exposes. Naming a different version asks for a "
            "migration, which is refused unless this run rebuilds every published artifact"
        ),
    )
    run.add_argument("--max-digest-rows", type=int, default=250_000)
    run.add_argument("--run-id", default=None)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.command == "contracts":
        from fareline.m2 import contracts

        print(
            json.dumps(
                {
                    service: {
                        **contract.as_document(),
                        "fingerprint": contract.fingerprint,
                    }
                    for service, contract in contracts.SERVICE_CONTRACTS.items()
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    from fareline.m2 import pipeline

    requested = args.artifact or [parse_artifact(item) for item in DEFAULT_ARTIFACTS]
    options = pipeline.SliceOptions(
        source_dir=args.source_dir,
        landing_root=args.landing_root,
        warehouse_root=args.warehouse_root,
        rebuild_root=args.rebuild_root,
        evidence_path=args.evidence_out,
        sample_evidence_paths=tuple(args.sample_evidence or DEFAULT_SAMPLE_EVIDENCE),
        artifacts=tuple(pipeline.ArtifactRequest(service, period) for service, period in requested),
        zone_lookup_version_id=args.zone_lookup_version,
        max_digest_rows=args.max_digest_rows,
        run_id=args.run_id or uuid.uuid4().hex,
    )
    report = pipeline.run_slice(options)
    print("FARELINE_M2_SUMMARY=" + json.dumps(_summary(report), sort_keys=True))
    return 0 if pipeline.slice_passed(report) else 1


def _summary(report: dict) -> dict:
    return {
        "run_id": report["run_id"],
        "equivalent": report["equivalence"]["equivalent"],
        "replay_unchanged": report["replay"]["logical_state_unchanged"],
        "replay_new_delta_commits": not report["replay"]["no_new_delta_commits"],
        "replay_ledger_events": len(report["version_ledger"]["replay_events"]),
        "rows": {
            service: {kind: table["rows"] for kind, table in tables.items()}
            for service, tables in report["tables"]["incremental"].items()
        },
        "version_states": {
            f"{item['service']}:{item['period']}:{item['version_id'][:12]}": item["state"]
            for item in report["version_selection"]
        },
        "zone_lookup_version_id": report["zone_lookup"]["zone_lookup_version_id"],
    }


if __name__ == "__main__":
    raise SystemExit(main())
