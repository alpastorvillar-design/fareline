# M0 findings

Measured on 2026-09-07 and extended by the M0.1 review remediation on the same
date. The machine and source are snapshots, not universal capacity or
availability guarantees.

## Result

The 2024 Yellow Taxi plus HVFHV window contains **280,640,168 rows** across 24
official monthly Parquet files. Their combined compressed object size is
**6,469,048,336 bytes (6.02 GiB)**. This is sufficient to ratify a future M3
target above 100 million real rows without duplication.

Ratification is not execution: M0 read HTTP headers and Parquet footers, then
materialized four local 1,000-row samples. It did not download or process the
annual corpus.

| Service | Files | Rows | Compressed bytes | GiB |
| --- | ---: | ---: | ---: | ---: |
| Yellow Taxi | 12 | 41,169,720 | 693,001,713 | 0.65 |
| HVFHV | 12 | 239,470,448 | 5,776,046,623 | 5.38 |
| **Combined** | **24** | **280,640,168** | **6,469,048,336** | **6.02** |

All 24 objects returned HTTP 200, a content length, an ETag, and
`binary/octet-stream`. Their Parquet footers report 280 row groups and format
version 2. File-level values and schemas are preserved in
[`source_inventory.json`](../evidence/m0/source_inventory.json).

Writer metadata is heterogeneous even inside the stable 2024 schema window: 18
files report `parquet-cpp-arrow 14.0.2`, while six report 16.1.0. The latter are
the August–October files for both services; November and December return to
14.0.2. Fareline therefore treats writer metadata as evidence, not as a contract.

Every monthly schema in the 2024 inventory was fingerprinted. Yellow has one
unique ordered schema across all 12 months; HVFHV also has one. This supports the
bounded M1 window without implying stability outside 2024.

## Schema evidence

January 2024 and January 2025 were inspected for each service.

| Service | 2024 columns | 2025 columns | Observed change |
| --- | ---: | ---: | --- |
| Yellow Taxi | 19 | 20 | Added nullable `cbd_congestion_fee` |
| HVFHV | 24 | 25 | Added nullable `cbd_congestion_fee` |

No removed columns or type changes were observed between these four probes.
Additional bounded historical probes found incompatible changes that the data
contract must handle:

- Yellow 2023-01→2023-07 renamed `airport_fee` to `Airport_fee` and changed five
  logical types, including `RatecodeID` and `passenger_count` from `DOUBLE` to
  `BIGINT`.
- HVFHV 2019-02→2019-07 changed `airport_fee` and `wav_match_flag` from
  physically untyped nulls to `DOUBLE` and `VARCHAR`.

The evidence now preserves all probe schemas in physical source order and lists
the consecutive changes. This demonstrates both compatible and incompatible
drift without claiming that every historical boundary was scanned.

The service contracts remain separate. Yellow exposes `fare_amount`,
`total_amount`, `payment_type`, and taxi-specific components. HVFHV exposes
`base_passenger_fare`, `driver_pay`, shared-ride/accessibility flags, and its own
components. Fareline will not map these to a single universal fare measure.

## Bounded samples

Four local Parquet samples were written under gitignored `data/samples/`:

| Service / period | Rows | Local compressed bytes |
| --- | ---: | ---: |
| Yellow 2024-01 | 1,000 | 24,625 |
| HVFHV 2024-01 | 1,000 | 41,206 |
| Yellow 2025-01 | 1,000 | 24,970 |
| HVFHV 2025-01 | 1,000 | 42,833 |

The evidence JSON records their SHA-256 hashes and explicitly marks them as not
redistributed. No TLC row data is tracked by Git. The local file sizes do not
measure network transfer: a limited Parquet query may fetch a complete row group.

## Taxi zone reference

The required official `taxi_zone_lookup.csv` was downloaded in memory and not
retained. It contains 265 rows and four columns in 12,331 bytes, with SHA-256
`1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed`.
No duplicate or invalid `LocationID` was observed. Its URL, ETag, modified time,
schema, hash, and quality counts are recorded; its rows are not published.

## Capacity gate

| Resource | Measured value |
| --- | ---: |
| Host RAM | 33,397,133,312 bytes (31.10 GiB) |
| Host logical processors | 16 |
| Docker memory | 16,292,409,344 bytes (15.17 GiB) |
| Docker CPUs | 16 |
| Free space on `C:` | 660,314,238,976 bytes (614.97 GiB) |
| Annual compressed input | 6,469,048,336 bytes (6.02 GiB) |

The initial project cap remains 120 GiB with at least 100 GiB free. The cap is
more than 18 times the measured compressed annual input, and current free space
exceeds the reserved floor by 514.97 GiB. This is enough to proceed to a bounded
vertical slice and later annual processing, but Delta-layer expansion, shuffle,
spill, and peak memory have not yet been measured. M1–M3 must stop before either
resource floor is crossed.

The M0.1 validation later observed 657,585,614,848 free bytes (612.42 GiB).
Normal host drift does not change the gate: the free-space floor remains more
than 512 GiB away.

## Runtime gate

Docker Desktop 4.89.0, Engine 29.7.2, and Compose 5.5.0 were verified. The pinned
container built successfully and reported:

- Apache Spark 4.1.0;
- Scala 2.13.17;
- OpenJDK 17.0.17;
- Python 3.10.12 in the Spark image;
- two registered standalone workers, four cores, and 4 GiB worker memory.

The initial runtime check registered workers but did not run a Spark job. Review
then exposed that container-local output could report `_SUCCESS` while losing
executor data. M0.1 corrected the topology with shared named volumes for data,
output, warehouse, and event logs. The replacement smoke executed on two worker
hosts, published eight Parquet data files, and re-read exactly 100,000 rows.
Container limits were observed at 4 GiB for the master/driver and 3 GiB per
worker; Spark worker defaults remain two cores and 2 GiB each.

The host's Java 25 installation is not used by the Spark runtime. Delta Lake
4.2.0 is selected from the upstream compatibility matrix for Spark 4.1.x, but
Delta reads/writes and local `LogStore` behavior remain M1/M2 work.

## Source and redistribution decision

The official TLC page links the monthly objects and dictionaries and warns that
records may be inaccurate or incomplete. The NYC Open Data FAQ says Open Data
has no use restrictions, but the TLC download page and general NYC.gov terms do
not provide an equally explicit redistribution grant for these Parquet objects.

Fareline therefore publishes code, URLs, schemas, counts, and derived metadata,
not source rows. The repository's MIT license applies only to Fareline code and
documentation. See [`source-data.md`](source-data.md).

## Decision

M0.1 closes the blocking runtime defect and expands source/schema evidence, but
does not begin M1. Proceed only after remote CI validates the distributed smoke
and the user explicitly approves M1. The first M1 slice remains bounded, adds
Delta 4.2.0, and must verify idempotency and DuckDB oracles before any annual
download.
