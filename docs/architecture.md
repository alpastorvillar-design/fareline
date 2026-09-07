# Architecture

## System boundary

Fareline turns versioned monthly Parquet files into auditable analytical
products. It does not own the source, assign business identity to a trip, or
provide a real-time service.

```mermaid
flowchart LR
    A[NYC TLC monthly Parquet] --> B[Source inventory]
    Z[NYC TLC zone lookup] --> B
    B --> C[Versioned landing]
    C --> D[Spark standalone]
    D --> E[Physical occurrences]
    E --> F[Yellow contract]
    E --> G[HVFHV contract]
    F --> H[Zone-hour demand]
    G --> H
    F --> I[Yellow fare-day]
    G --> J[HVFHV fare-day]
    B --> K[Quality and run ledger]
    D --> K
    H --> L[Spark SQL extracts]
    I --> L
    J --> L
    L --> M[Secondary Power BI report]
```

## Technical identity

The idempotent unit is a source-file version identified by logical URL and
content hash. Each raw row is an occurrence identified by that file version and
its physical ordinal. Equal field values do not imply equal trips, so Fareline
preserves multiplicity and never deduplicates by a heuristic content hash.

## Execution modes

- CI uses small fixtures and Spark local mode where appropriate.
- M0 establishes a Docker Compose Spark standalone master and worker baseline;
  M0.1 proves that it can write and read shared output before M1 uses it.
- M3 compares DuckDB, one Spark worker, and multiple Spark workers on the same
  physical host. It records shuffle, spill, skew, memory, time, and file layout.
- Worker-loss testing is valid only against standalone executor processes, not
  `local[*]`.

The container baseline uses Apache Spark 4.1.0, Scala 2.13, Python 3, and Java
17. Delta Lake 4.2.0 is compatible with Spark 4.1.x according to the upstream
[compatibility matrix](https://docs.delta.io/releases/). Delta dependencies and
write semantics enter at M1.

## Shared-storage boundary

The master/driver and every worker mount the same Docker named volumes at
`/opt/fareline/data`, `/opt/fareline/output`, `/opt/fareline/warehouse`, and
`/opt/fareline/spark-events`. A container-local `file://` path is safe only when
it resolves inside one of those shared mounts. `_SUCCESS` alone is never evidence
of publication: the cluster smoke requires visible data files and an exact
write/read count from two executor hosts.

Named volumes make the local filesystem topology explicit and reproducible in
CI. They are not evidence for object-store semantics. M1 records the volume and
path used by each run; M4 must revalidate publication behavior on any selected
cloud storage implementation.

## Delta boundary

M1 introduces a minimal Delta bronze table on the bounded vertical slice. M2
will validate replacement, atomic publication, schema enforcement/evolution,
and incremental-versus-rebuild equivalence. Local-filesystem concurrency
testing is limited to a single Spark driver. Fareline will not generalize that
result to multi-process writers or object storage; stronger claims require M4
on the chosen storage implementation.

## Cloud gate

M0–M3 contain no cloud provider. M4 first compares current role demand,
compatibility, regional availability, identity/IAM, IaC, and estimated cost.
Only an explicitly approved design may be applied. If M4 is skipped, Fareline
remains a local distributed lakehouse and makes no operational cloud claim.
