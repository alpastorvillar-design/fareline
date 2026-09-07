# M1 vertical slice

Measured on 2026-09-08 on one Windows host running Docker Desktop with a Spark
standalone master and two worker containers. The numbers below describe bounded
samples, not a month and not the ratified annual target.

## What ran on real data

| Artifact | Rows | Bytes | Scope |
| --- | ---: | ---: | --- |
| Yellow 2024-01 | 1,000 | 24,625 | bounded sample derived from the official object |
| HVFHV 2024-01 | 1,000 | 41,206 | bounded sample derived from the official object |
| `taxi_zone_lookup.csv` | 265 | 12,331 | complete official object |

The two trip artifacts are the M0 samples: publication requires their SHA-256
digests to match the values recorded in
[`source_inventory.json`](../evidence/m0/source_inventory.json). A bounded
sample's digest describes the bytes acquired locally, never the remote monthly
object. The zone lookup is the only file M1 downloads, and it is capped at 4
MiB by the acquisition code.

M1 does **not** process a full month, the 2024 window, or 100 million rows.

## Versioned landing

Landing identity is the logical artifact plus the SHA-256 of the acquired bytes:

```text
version_id = sha256(logical_id + "\n" + content_sha256)
```

`logical_id` includes artifact completeness. A bounded sample, synthetic
fixture and complete upstream object therefore cannot share version history.
The local trip interface does not accept a `complete_object` claim.

Acquisition writes to `_incoming/<uuid>.part` inside the landing volume, hashes
the bytes, checks that the Parquet footer or CSV contract is readable, and only
then renames the file into `files/<logical_id>/<version_id>/`. The rename is
atomic inside the volume, so a reader never sees a partial artifact. A rejected
acquisition deletes its partial file and appends a `rejected` record to the
ledger. An ETag is stored as advisory transport metadata and is never used as a
content checksum.

```text
landing/
  files/trip_records__bounded_sample__yellow__2024-01/<version_id>/yellow_tripdata_2024-01.parquet
  manifest/trip_records__bounded_sample__yellow__2024-01/<version_id>.json
  events.jsonl
  _incoming/
```

Re-acquiring identical, intact bytes is a no-op: the existing manifest is
returned unchanged and the ledger records a `replayed` event. If that manifest
exists but its landed file is missing or damaged, newly acquired and validated
bytes restore the same version and the ledger records `repaired`. Inconsistent
manifest metadata is rejected rather than trusted. New bytes for the same
logical artifact create a new version and leave the previous manifest and file
untouched. Choosing a current version and marking others superseded is M2.

## Delta source occurrences

Two tables, one per service, partitioned by `source_period`:

```text
warehouse/source_occurrences/yellow_trip_occurrence
warehouse/source_occurrences/hvfhv_trip_occurrence
```

Source columns are preserved verbatim, including `Airport_fee` casing and
`timestamp_ntz` pickup and drop-off values. Nine technical columns are added:
`source_row_ordinal`, `source_file_version_id`, `source_service`,
`source_period`, `source_artifact_completeness`, `source_logical_url`,
`source_content_sha256`, `ingest_run_id` and `ingested_at_utc`. The Yellow table
therefore has 28 columns and HVFHV 33. A table refuses to mix completeness
scopes.

The technical key is `(source_file_version_id, source_row_ordinal)`. The ordinal
is `_metadata.row_index`, the Parquet reader's own physical row position inside
the file, so it is produced by the scan before any shuffle and does not depend
on how Spark orders or packs file splits. A landed version is exactly one file,
which is what makes an in-file row index equal to the version's physical
ordinal. Before writing, the job fails unless the ordinals form a dense
`0..n-1` sequence.

Ingestion checks whether the version is already published and appends only if it
is not. After the write it re-reads the table and requires a Delta log, at least
one data file on disk, the expected row count, and one technical key per row.
The integration smoke deletes the data files of a published table and confirms
that this verification refuses the result instead of trusting the log.

## Idempotency observed

| Pass | Landing | Source occurrences | Rows | Delta version |
| --- | --- | --- | ---: | ---: |
| First | published | published | 1,000 per service | 0 |
| Replay in the same run | replayed | replayed | 1,000 per service | 0 |
| Separate later run | replayed | replayed | 1,000 per service | 0 |

The third pass is a different driver process against the warm volumes. After it,
the ledger holds three `published` and nine `replayed` events, the occurrence tables
still hold 1,000 rows each, and neither table has gained a Delta commit.

This is single-writer idempotency on a local filesystem. Two drivers running
concurrently could both pass the published-version check before either commits;
M1 does not claim otherwise, and M2 owns concurrency, supersession and
incremental-versus-rebuild equivalence.

## DuckDB oracle

The same SQL text runs in Spark over the published Delta table and in DuckDB
over the landed source file. Monetary components are summed as integer cents so
the two engines can be compared exactly rather than within a floating-point
tolerance, and a missing component stays null instead of becoming zero: every
sum is published next to its own non-null denominator.

Yellow and HVFHV keep separate components and are never added together:

| Service | Component | Non-null rows | Sum (cents) |
| --- | --- | ---: | ---: |
| Yellow | `fare_amount` | 1,000 | 1,827,320 |
| Yellow | `tip_amount` | 1,000 | 347,287 |
| Yellow | `total_amount` | 1,000 | 2,682,838 |
| HVFHV | `base_passenger_fare` | 1,000 | 2,755,683 |
| HVFHV | `tips` | 1,000 | 107,706 |
| HVFHV | `driver_pay` | 1,000 | 2,112,996 |

These cover 1,000 sampled rows per service, so they describe the sample and not
the month. Yellow `total_amount` and HVFHV `driver_pay` measure different
things and are reported side by side, never combined.

All twelve metrics matched per service. Three Yellow rows have a pickup
timestamp outside 2024-01; they are recorded as an incident and remain in the
source-occurrence tables. Both services matched on all 1,000 ordinals when the technical ordinal
and three probe columns were compared against DuckDB's `file_row_number`, which
is the strongest available evidence that the technical ordinal really is the
physical source position.

Full results, hashes, timings and runtime versions are in
[`vertical_slice.json`](../evidence/m1/vertical_slice.json). It contains derived
metadata only; no TLC rows are published.

## Runtime

Apache Spark 4.1.0, Java 17.0.17, Python 3.10.12, DuckDB 1.5.5, Delta Lake
4.2.0. The Delta coordinate `io.delta:delta-spark_2.13:4.2.0` is declared once
in the image and resolved into an Ivy cache during the build, so runs report
`0 artifacts copied, 32 already retrieved`. The image build fails if the Delta
Python package and the JVM coordinate disagree.

Delta artifacts stay user jars instead of being copied into `/opt/spark/jars`,
because their transitive closure includes older Jackson and Parquet builds that
must not take precedence over the ones Spark ships.

Wall-clock for the recorded run: 0.161 s acquisition, 2.835 s session start,
10.507 s first ingestion including the ordinal density check, 5.972 s table
verification, 1.598 s replay and 2.633 s for the oracle. Repeated runs on the
same host vary by a
few hundred milliseconds, so these describe one run rather than a benchmark;
measured comparisons against other engines belong to M3. Landing holds 156 KiB
and the warehouse 208 KiB after the separate-process replay. Delta data files
are not byte-identical between runs, so their size is reported per run rather
than treated as a constant.

## Known limits

- Bounded samples only; no month, no annual window, no 100 million rows.
- Single writer, single driver, local filesystem named volumes.
- Every acquired version stays published; supersession is M2.
- Ordinal alignment is compared row by row only below 250,000 rows per version.
- The zone lookup is versioned and validated but not yet modelled as a
  dimension; zone joins arrive with the contracted service tables.
- No contracted service tables, analytical products, dimensional model, Power
  BI report, cloud service or IaC.
