# M2 contracted correctness

Measured on 2026-09-09 on one Windows host running Docker Desktop with a Spark
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
rewritten. Across the two measured runs the ledger holds five `activated` events
and one `rejected` event — three from the first run and three from the second;
replaying either appended nothing.

The ledger advances only after publication succeeds. It is an audit record, not
the visibility mechanism.

## Contracted tables

Three Delta tables per service and contract fingerprint, partitioned by
`source_period`:

```text
warehouse/contracted_trips/<service>_trip/contract=<fingerprint>
warehouse/quarantine/<service>_trip/contract=<fingerprint>
warehouse/quality_incidents/<service>_trip/contract=<fingerprint>
warehouse/publication_catalog/<logical-id-hash>/<derivation-id>.json
warehouse/publication_layout.json
warehouse/publication_boundary.json
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

The layout marker is installed before the first of those writes and carries only
the physical layout schema version. It is not a publication pointer. This makes
a crash after a complete marker but before the first boundary distinguishable
from a legacy M2 warehouse: the interrupted publication can be replayed and the
boundary installed, while legacy data is refused.

A marker's `state` records what the candidate was at the moment its marker was
written, and markers are never rewritten. Two markers of one artifact can
therefore both read `active`. The resolver is the only correct reader: it filters
by the boundary's context and keeps the newest completed active derivation by
`(source_published_at_utc, source_version_id)`. Treating every marker file as
current would be wrong, and no reader should do it.

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

## The published view, and changing it

Completing a derivation and exposing it are separate decisions. Markers do the
first; `publication_boundary.json` does the second, naming the zone lookup
version for the whole dataset and one contract fingerprint per service.

That separation exists because of a measured failure. When the view was resolved
from whatever context the current run happened to compute, a run defaulted to the
newest landed lookup version, and every period published under the previous one
left the view at once — no error, no warning, and no field in the evidence that
showed it. Upstream reissuing `taxi_zone_lookup.csv` was enough to trigger it.

Three mechanisms replace that behaviour.

- A run joins against the lookup version the boundary exposes. Landing a newer
  one changes nothing until an operator asks for it with
  `--zone-lookup-version`. The evidence records both the active version and the
  newest landed one.
- A context change is a migration, and a migration is refused before the first
  write unless the run's scope rebuilds every artifact the previous boundary
  published. The refusal names the artifacts that are missing.
- The boundary is installed only after every one of those markers exists, in one
  rename. Readers see the old context or the new one.

Every run's evidence lists all the artifacts the catalog knows — not only the
ones the run asked for — with whether each is visible under the active context.
The gate fails if any is not.

The integration smoke lands a second lookup version and checks all three: the
default stays on the published version, a single-period migration is refused
with the boundary and the view intact, and a full migration moves the boundary
with all four artifacts covered, leaving the previous derivations physically
present and no longer visible.

## Revising a contract

The derivation identity has always included the contract fingerprint, but a
revision that changes the derived output schema cannot share a table with its
predecessor. A Delta append rejects a frame whose schema does not match the
table's, both when a column is added and when a column's type changes, and
forcing it with `mergeSchema` would blur two output schemas into one table and
make the earlier rows unreadable as what they were.

Each fingerprint therefore owns its own tables under `contract=<fingerprint>`,
and the boundary resolves which one readers are on. The smoke materialises both
shapes against the same warehouse:

| Revision | Fingerprint | Output columns | `payment_type` | Rows | Replay |
| --- | --- | ---: | --- | ---: | --- |
| `yellow/v1` baseline | `68dff1dcba7c…` | 41 | `bigint` | 781 | — |
| `yellow/v2`, one added column | `bdc7058033f1…` | 42 | `bigint` | 781 | no new Delta commit |
| `yellow/v3`, one retyped column | `56970e5d0443…` | 41 | `double` | 781 | no new Delta commit |

The retyped revision keeps the column count, so the derived type is what shows
the revision reached the output rather than being quietly ignored.

After each revision the baseline tables still hold their original rows, the
boundary points at the revision, and replaying the revision is a no-op. The
trade-off is that a revision re-derives every artifact rather than sharing
storage with its predecessor, and that the migration must be complete before it
becomes visible. That is the intended cost: it buys a readable history and an
unambiguous current view.

A warehouse written before per-contract tables existed has no layout marker. A
run refuses it with an explicit message instead of reading it as an empty table.
Markers without a boundary are accepted only when that current-layout identity
is present, which is the recoverable state left by an interrupted first
publication.

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

The measured warehouse is built in two runs, because a single run against an
empty warehouse compares two builds from nothing and proves nothing about
incremental maintenance.

| Run | Artifacts requested | Outcome |
| --- | --- | --- |
| First | Yellow 2023-01, Yellow 2024-01, HVFHV 2024-01 | 3 written; boundary installed |
| Second | all six | 3 `already_published`, 2 written, 1 rejected |

When the second run starts, the warehouse already exposes 2,000 Yellow contracted
rows with 7 incidents at Delta version 1, and 1,000 HVFHV contracted rows at
Delta version 0. The evidence records that starting state in
`incremental.state_before_this_run`, and `incremental.started_from_published_state`
says whether there was one at all.

That maintained warehouse is then compared against a rebuild of all six versions
from nothing into an isolated root. The comparison is logical — schema, row
counts, distinct technical keys, and content digests over every column — and it
passed for all six tables.

The rebuild root is also guarded as an owned scratch location. Fareline may
claim an absent or empty directory by writing `.fareline-rebuild-root.json`, and
may clear only a root carrying that valid marker. A non-empty unmarked directory,
a corrupt marker, or an unsupported marker version is refused before deletion.

Two digests are produced. The sorted digest folds one SHA-256 per row in a fixed
order and is exact below a bounded row count; the additive digest is
order-independent and streams, so it stays available above that bound. Physical
facts are deliberately excluded: Delta commit counts, file counts and file bytes
differ legitimately between an incremental history and a rebuild, and requiring
them to match would test the writer rather than the result.

| Table | Rows | Content digest |
| --- | ---: | --- |
| Yellow contracted | 3,000 | `4c9637a2483ae…` |
| Yellow incidents | 11 | `598db8adc2b72…` |
| HVFHV contracted | 2,000 | `ba9093b637c12…` |
| HVFHV incidents | 1 | `e606a0f579a4a…` |

The two contracted digests are the ones the previous measured run produced. The
incident digests changed because an incident identifier is now the hash of a
JSON encoding of its components rather than of a delimiter-joined string; the
rows it identifies are the same.

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

Each evidence file states its own verdict. `gate.passed` and the five named
checks behind it — incremental matches rebuild, replay changes nothing, replay
adds no Delta commit, replay adds no ledger event, every known artifact is
visible — are written into the file, so a `contracted_slice.json` found on its
own says whether it records a success or a failure.

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

## Physical history retention

Nothing is ever deleted. Every re-derivation — a corrected source file, a moved
lookup version, a revised contract — appends a complete new copy that the
boundary hides and no process removes. At 5,000 rows that is free. At the scale
M3 measures it is a design decision, so the policy is stated before it is needed
rather than after.

- **Auditable warehouse.** History is retained in full, and `VACUUM` is never
  run. Reproducing what a reader saw at a past boundary is the point of keeping
  it, and Delta's own retention defaults must not silently remove files a
  superseded derivation still references.
- **Benchmark warehouse.** A measurement warehouse is disposable. It is built
  from scratch for the run that uses it and deleted afterwards, so a benchmark
  never pays for accumulated history and never contaminates the audit trail.
- **Authorised cleanup.** Pruning the auditable warehouse needs an explicit
  decision that names what is removed and after how long, and it must run
  against a boundary that no longer references the derivations being dropped.
  Nothing in M2 does this, and nothing should do it implicitly.

This is a policy, not a measurement. What is measured here is that the history
survives supersession: after the zone migration in the smoke, the contracted
table holds 1,953 physical rows and exposes 781.

## Runtime and timings

Apache Spark 4.1.0, Delta Lake 4.2.0, Java 17.0.17, Python 3.10.12, DuckDB
1.5.5, two executor hosts. Wall clock for the second measured run: 0.246 s
landing, 3.071 s session start, 1.112 s planning, 20.476 s incremental build,
25.492 s rebuild and 9.136 s replay. The incremental figure is below the rebuild
because three of the six versions were already published by the first run, whose
own incremental pass took 37.217 s. These describe two runs on one host, not a
benchmark; measured engine comparisons belong to M3.

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
- Atomic publication markers and the boundary rename are validated on the shared
  local filesystem; an object-store protocol remains an M4 decision.
- A migration is refused unless one run rebuilds every affected artifact. There
  is no resumable multi-run migration, and there is no partial install of one
  dimension while another waits.
- The exact sorted digest is bounded by a driver-side row limit; above it only
  the additive digest remains. Equivalence at M3 scale needs a different
  mechanism, and measurement cost has to be separated from processing cost
  before any engine comparison is meaningful.
- Physical history is never pruned; see the retention policy above.
- No analytical products, dimensional model, Power BI report, cloud service or
  IaC.
