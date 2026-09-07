# Fareline

Fareline is a reproducible analytical lakehouse for heterogeneous monthly NYC
Taxi and Limousine Commission trip records. It is designed to preserve every
physical source occurrence, reprocess corrected files safely, enforce separate
contracts for Yellow Taxi and High Volume FHV (HVFHV), and make distributed
processing decisions measurable rather than assumed.

**M0 is complete.** The bounded inventory found 280,640,168 rows in the 24
Yellow/HVFHV files for 2024, with 6,469,048,336 compressed source bytes. This
ratifies a future target above 100 million real rows; it does not claim those
rows have been downloaded or processed. No full historical dataset or cloud
infrastructure has been created. See the measured
[`M0 findings`](docs/m0-findings.md).

**M0.1 closes the independent-review gate.** It inventories every 2024 schema
and the zone lookup, documents real incompatible drift, and protects the Docker
baseline with a two-worker write/read test over shared storage. It does not
implement landing or Delta bronze; those remain M1 work requiring separate user
approval.

## Why this exists

The public TLC files are large enough to exercise partitioning, shuffle, skew,
spill, and worker recovery, but they also contain a more important engineering
problem: monthly files and schemas can change without a universal trip ID.
Fareline therefore treats a source-file version and its physical row position as
technical identity. It never uses a heuristic hash to claim that two equal rows
are the same real-world trip.

Yellow and HVFHV are intentionally not forced into one fare contract. Their
service-specific components remain separate, and cross-service products compare
coverage and demand only where the semantics are defensible.

## Planned data flow

```text
official monthly Parquet
        |
source manifest + content hash
        |
versioned landing files
        |
Spark standalone (one or more workers)
        |
source occurrences -> service contracts -> analytical products
        |                                      |
quality ledger                         Spark SQL / Power BI extract
```

M0–M3 are local and cloud-neutral. A managed-cloud proof is a separate M4 gate
that requires current demand evidence, a cost estimate, and explicit approval.

## Reproduce M0

Python 3.10–3.14 is supported by the bounded metadata inspector. CI uses Python
3.12, while the pinned Spark image currently provides Python 3.10. DuckDB stays
in the core package because it powers the inspector and will be the single-node
correctness/performance oracle; installing the package in the Spark image keeps
that diagnostic path available in the same runtime.

```bash
python -m venv .venv
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
pytest
ruff check .
ruff format --check .
fareline-m0 \
  --inventory-year 2024 \
  --schema-period 2019-02 --schema-period 2019-07 \
  --schema-period 2023-01 --schema-period 2023-07 \
  --schema-period 2024-01 --schema-period 2025-01 \
  --sample-period 2024-01 --sample-period 2025-01 \
  --sample-rows 1000 \
  --output evidence/m0/source_inventory.json
```

The last command is the procedure used to produce the published evidence. It
reads HTTP headers and Parquet footers for 24 monthly objects, probes bounded
historical schemas, and downloads the 12 KiB zone lookup. It does not download
the annual trip corpus. Samples are materialized only under the gitignored
`data/` directory; a Parquet row limit may still transfer a complete row group
and should not be interpreted as network bytes. Timestamps and mutable source
HTTP metadata can change between runs.

## Container baseline

The Spark image is pinned to Apache Spark 4.1.0 with Java 17. The M0 smoke test
registered two standalone workers with four cores and 4 GiB total worker memory.
Delta Lake 4.2.0 is the compatible target for M1, but Delta execution is not
claimed at M0.

```bash
docker compose build
docker compose up -d --scale spark-worker=2 --wait --wait-timeout 120
docker compose ps
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/fareline/scripts/spark_storage_smoke.py
docker compose down
```

The smoke must report at least two executor hosts, one or more data files, and
exactly 100,000 rows after re-reading shared output. Docker named volumes share
`data`, `output`, `warehouse`, and `spark-events` across the driver and workers;
`docker compose down -v` removes those volumes.

Defaults reserve two cores and 2 GiB of Spark memory per worker, cap each worker
container at 3 GiB, and cap the master/driver container at 4 GiB. Override them
with `FARELINE_WORKER_CORES`, `FARELINE_WORKER_MEMORY`,
`FARELINE_WORKER_CONTAINER_MEMORY_LIMIT`, and
`FARELINE_MASTER_CONTAINER_MEMORY_LIMIT`. The local host remains one physical
machine even with multiple JVM worker processes.

## Documentation

- [Architecture and milestones](docs/architecture.md)
- [Source-specific data contracts](docs/data-contracts.md)
- [Source terms and redistribution boundary](docs/source-data.md)
- [Acceptance and cancellation gates](docs/milestones.md)

## Scope boundaries

- No streaming, Kafka, Kubernetes, forecasting, or public API.
- DuckDB is a fair single-node reference; Spark is not promised to be faster on
  one host.
- Raw TLC rows and local samples are not committed.
- The MIT license covers this repository's code and documentation, not NYC TLC
  data or third-party materials.
- Cloud, IaC, IAM, and cloud-cost claims remain out of scope until M4 is approved
  and executed.
