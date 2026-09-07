# M0 findings

Measured on 2026-09-07. The machine and source are snapshots, not universal
capacity or availability guarantees.

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

## Schema evidence

January 2024 and January 2025 were inspected for each service.

| Service | 2024 columns | 2025 columns | Observed change |
| --- | ---: | ---: | --- |
| Yellow Taxi | 19 | 20 | Added nullable `cbd_congestion_fee` |
| HVFHV | 24 | 25 | Added nullable `cbd_congestion_fee` |

No removed columns or type changes were observed between these four probes.
That is evidence for one compatible-addition case, not proof that every month or
historical year shares the same schema.

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
redistributed. No TLC row data is tracked by Git.

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

## Runtime gate

Docker Desktop 4.89.0, Engine 29.7.2, and Compose 5.5.0 were verified. The pinned
container built successfully and reported:

- Apache Spark 4.1.0;
- Scala 2.13.17;
- OpenJDK 17.0.17;
- Python 3.10.12 in the Spark image;
- two registered standalone workers, four cores, and 4 GiB worker memory.

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

Proceed to M1 only after independent review and explicit user approval. The
annual Yellow + HVFHV composition meets the scale objective with a 2.8× row
margin, while preserving distinct contracts. M1 should remain bounded to a
vertical slice, add Delta 4.2.0, and verify idempotency and DuckDB oracles before
any annual download.
