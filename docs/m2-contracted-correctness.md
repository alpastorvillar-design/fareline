# M2 contracted correctness

Measured on 2026-09-08 on one Windows host running Docker Desktop with a Spark
standalone master and two worker containers. The numbers describe bounded
samples of 1,000 rows per source version, not a month and not the ratified
annual target.

## What the contracts do

Each service owns an executable contract: canonical column names, the source
spellings each accepts, one explicit target type per column, and the row rules
that decide whether a row is published, flagged or quarantined. Yellow has 20
canonical columns and HVFHV 25, and the two never share a fare component beyond
the three levies upstream defines identically for both.

A contract has a fingerprint, so a published row can be traced to the exact
contract that produced it:

| Contract | Version | Fingerprint |
| --- | --- | --- |
| Yellow | `yellow/v1` | `68dff1dcba7c9e9d…` |
| HVFHV | `hvfhv/v1` | `31d6193c43d514f4…` |

`fareline-m2 contracts` prints both as JSON without needing Spark.

## Schema resolution

Source column names are matched case-insensitively against the aliases a
contract declares; source types are matched against one explicit target type
through a promotion classification:

| Class | Rule | Result |
| --- | --- | --- |
| identity | Same type | Read as is |
| widening | Lossless for every value of the source type | Cast |
| guarded | Lossless only inside a range, e.g. 64-bit integer into a double below 2^53 | Cast, and every row is range-checked |
| untyped null | Source declares the Parquet Null logical type | Contract type applied to typed nulls; no type is inferred from data |
| absent | Optional column the file does not carry | Typed null |
| narrowing | Source is wider than the contract | Version rejected |
| incompatible | No defined promotion | Version rejected |
| ambiguous | Two source columns fold to one canonical column | Version rejected, both names reported |
| missing required | No alias present | Version rejected |

Fareline reads source schemas with `spark.sql.caseSensitive` enabled. Case
folding belongs to the contract, not to the reader: with Spark's default
resolution a file carrying both `airport_fee` and `Airport_fee` cannot even be
described, so the collision surfaces as a reader error instead of a contract
decision that names both columns.

Every schema recorded by M0 between 2019-02 and 2025-01 is replayed against
these rules in the unit tests, with no Spark and no download. All twelve are
accepted, and no real source column falls outside the contracts.

## Real drift, resolved

Six landed versions of real bounded samples:

| Service | Period | State | Promotions observed |
| --- | --- | --- | --- |
| Yellow | 2023-01 | active | 19 identity, 1 absent |
| Yellow | 2024-01 | active | 14 identity, 3 widening, 2 guarded, 1 absent |
| Yellow | 2025-01 | active | 15 identity, 3 widening, 2 guarded |
| HVFHV | 2019-02 | **rejected** | 22 identity, 1 widening, 1 incompatible, 1 absent |
| HVFHV | 2024-01 | active | 22 identity, 2 widening, 1 absent |
| HVFHV | 2025-01 | active | 23 identity, 2 widening |

The Yellow rows exercise the rename upstream actually made: `airport_fee` in
2023-01 and `Airport_fee` from 2023-07 onward resolve to one canonical
`airport_fee` column. `VendorID`, `PULocationID` and `DOLocationID` narrow from
64-bit to 32-bit upstream, which is a widening promotion into the contract's
64-bit target. `passenger_count` and `RatecodeID` move from `DOUBLE` to `BIGINT`,
which becomes the range-checked promotion into the contract's double.
`cbd_congestion_fee` exists only from 2025-01 and is a typed null before that.

The HVFHV 2019-02 rejection is worth reading carefully. Upstream declares
`airport_fee` and `wav_match_flag` with the Parquet Null logical type; the unit
tests confirm the contract accepts that shape and applies the contract type to
typed nulls. The bounded local derivative is not type-identical to upstream:
DuckDB materialises those untyped columns as `INT32`, so the landed file offers
an integer where the contract requires a string. That is refused as incompatible
drift, one incident is recorded against the version, and no row is read. It is a
real rejection of a real file, and it is not evidence about the upstream object.

## Version selection and the ledger

The active version of a logical artifact is the newest landed manifest the
contract accepts, ordered by publication time with the version id breaking ties.
Rejecting a newer file therefore hands the role back to the previous accepted
version rather than stopping the run — which matters for correctness, because a
run that merely stopped would leave an incremental table holding rows a full
rebuild would never produce.

Publication and supersession are appended to `version_ledger.jsonl` and never
rewritten. The measured run wrote five `activated` events and one `rejected`
event; running it again appended nothing.

The ledger advances only after publication succeeds. It is an audit record, not
the visibility mechanism.

## Contracted tables

Three Delta tables per service, partitioned by `source_period`:

```text
warehouse/contracted_trips/<service>_trip
warehouse/quarantine/<service>_trip
warehouse/quality_incidents/<service>_trip
warehouse/publication_catalog/<logical-id-hash>/<derivation-id>.json
```

Every row carries the lineage that makes it reproducible: source file version,
physical row ordinal, service, period, artifact completeness, logical URL,
content hash, derivation id, contract version, contract fingerprint and the taxi
zone lookup version it was joined against. The derivation id hashes the source
version, contract fingerprint and lookup version. Nothing run-specific is stored
in Delta — no run id and no ingestion timestamp — because a table that changes
between two correct runs cannot be compared with a rebuild.

The Delta tables retain physical history. A derivation is visible only after all
of its required table writes complete and an immutable same-directory marker is
atomically installed. The marker-aware view selects the newest completed active
derivation and the completed rejection incidents. Supersession therefore changes
visibility without deleting auditable history.

| Table | Rows | Distinct keys | Columns |
| --- | ---: | ---: | ---: |
| Yellow contracted | 3,000 | 3,000 | 41 |
| Yellow quarantine | 0 | 0 | 42 |
| Yellow incidents | 11 | 11 | 12 |
| HVFHV contracted | 2,000 | 2,000 | 46 |
| HVFHV quarantine | 0 | 0 | 47 |
| HVFHV incidents | 1 | 1 | 12 |

The eleven Yellow incidents are pickups outside the month the source file names:
four in 2023-01, three in 2024-01 and four in 2025-01. They are recorded and
published, not withheld: an out-of-period pickup is real upstream behaviour, not
an impossible row. The single HVFHV incident is the version-scope rejection
above.

No row of the real samples triggered a quarantine rule, and every zone key
resolved against the joined lookup version. The quarantine and unknown-zone
paths are therefore exercised by the integration smoke on fixtures rather than
by these samples, and this document does not claim otherwise.

## Quarantine and incidents

Only impossible rows are quarantined: a missing pickup instant, a drop-off
strictly before pickup, or a value outside the exactly representable range of a
guarded promotion. Everything else is an incident that keeps the row — an
out-of-period pickup, an unresolved duration, a null or unknown zone key, and
the two civil-time cases below.

Quarantine removes a row from the contracted table. It never removes anything
from landing or from the M1 source-occurrence tables, and the quarantined row is
written whole so it stays inspectable.

## Zone joins

Both zone keys are left-joined against one explicitly recorded lookup version
(`d2ac78f0d975…`, 265 rows) broadcast from the driver. The dimension contributes
a presence marker, so an unknown key is distinguishable from a row whose
attributes are empty: `pickup_zone_resolved` and `dropoff_zone_resolved` are
false, the zone attributes stay null, and an incident is recorded. No zone is
ever invented.

## Local time

TLC timestamps are wall-clock values with no offset, read as `timestamp_ntz` and
kept naive. `pickup_local_date` and `pickup_local_hour` are derived from that
wall clock directly, so no UTC instant is manufactured. For the two hours a year
when civil time is not a bijection, the run derives the affected local intervals
for the file's own month from the system time-zone database and flags rows
inside them: `local_time_nonexistent` for the spring-forward hour and
`local_time_ambiguous` for the fall-back hour. Both measured periods are
January, so both counts are zero here; the derivation is unit-tested against
March and November of 2024 and 2025. If the runtime cannot load the named time
zone, the build fails explicitly instead of reporting a misleading zero.

## Incremental versus rebuild

The same plan is applied twice: incrementally into the warehouse, and from
nothing into an isolated rebuild root. The comparison is logical — schema, row
counts, distinct technical keys, and content digests over every column — and it
passed for all six tables.

Two digests are produced. The sorted digest folds one SHA-256 per row in a fixed
order and is exact below a bounded row count; the additive digest is
order-independent and streams, so it stays available above that bound. Physical
facts are deliberately excluded: Delta commit counts, file counts and file bytes
differ legitimately between an incremental history and a rebuild, and requiring
them to match would test the writer rather than the result.

| Table | Rows | Content digest |
| --- | ---: | --- |
| Yellow contracted | 3,000 | `4c9637a2483ae…` |
| Yellow incidents | 11 | `798c81d93e137…` |
| HVFHV contracted | 2,000 | `ba9093b637c12…` |
| HVFHV incidents | 1 | `a1ec067fb5bcb…` |

## Replay

Applying the same plan a third time changed nothing: every version reported
`already_published`, no table gained a Delta commit, the ledger appended no event,
and the logical state was identical. Running the whole command again as a
separate driver process against the warm volumes produced the same result.

Idempotency is enforced in the transaction log, not only by a read-side check.
Each write carries a Delta `txnAppId`/`txnVersion` marker derived from the table
and the derivation, so a repeated or duplicated writer cannot append the
same version twice even if both pass a published-version check first.

The integration smoke additionally fails the second of three Delta writes. The
first physical write remains repairable but receives no publication marker, so
the reader-visible state does not change. A retry completes the other writes and
then publishes the marker. The same smoke runs only one requested period and
proves periods omitted from the request remain visible.

## Concurrency

The Delta append primitive was measured with two drivers in different containers
writing one throwaway table on the shared volume. Full results are in
[`concurrency.json`](../evidence/m2/concurrency.json).

| Scenario | Outcome | Table afterwards |
| --- | --- | --- |
| Two drivers create the table at once | One committed, one raised `ProtocolChangedException` | 1 version, 2,000 rows, no duplicate keys |
| Two drivers append different versions to an existing table | Both committed | 3 versions, 6,000 rows, no duplicate keys |
| Two drivers append the same version with the same marker | One committed, one raised `ConcurrentTransactionException` | 2 versions, 4,000 rows, no duplicate keys |

No row was lost or duplicated in any scenario, and every conflict failed with a
named Delta exception rather than silently. This describes the one-table append
primitive under Delta 4.2.0 over Docker named volumes on a single host. The M2
orchestration and JSONL audit ledger retain one coordinator; this is not an
end-to-end multi-writer claim or evidence for object storage, more than two
drivers, or writers on different machines.

## Verification

After every write the job re-reads the table, asks the transaction log which
data files it points at, and requires each of them to exist on disk before
measuring anything. The integration smoke deletes a published table's data files
and confirms that this check refuses the result instead of trusting the log.

## Runtime and timings

Apache Spark 4.1.0, Delta Lake 4.2.0, Java 17.0.17, Python 3.10.12, DuckDB
1.5.5, two executor hosts. Wall clock for the reviewed cold run: 0.253 s landing,
3.158 s session start, 1.384 s planning, 44.017 s incremental build, 26.876 s
rebuild and 9.460 s replay. These describe one run on one host, not a benchmark;
measured engine comparisons belong to M3.

## Known limits

- 1,000 rows per source version. No month, no annual window, no 100 million rows.
- The quarantine and unknown-zone paths are exercised by fixtures, not by the
  real samples.
- Both measured periods are January, so no civil-time transition is present in
  the data.
- The HVFHV 2019-02 derivative is not type-identical to the upstream object it
  was derived from; see the note above.
- Concurrency is measured only for the one-table Delta append primitive with two
  drivers on one host. The orchestration retains one coordinator.
- Atomic publication markers are validated on the shared local filesystem; an
  object-store protocol remains an M4 decision.
- The exact sorted digest is bounded by a driver-side row limit; above it only
  the additive digest remains.
- No analytical products, dimensional model, Power BI report, cloud service or
  IaC.
