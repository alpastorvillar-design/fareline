# Data contracts

Fareline uses two source-specific contracts. Common names are used only where
the source semantics are compatible; a missing component remains null rather
than being converted to zero.

## Source-file version

Grain: one acquired content version for one logical monthly URL.

Required technical fields:

| Field | Rule |
| --- | --- |
| `source_service` | `yellow` or `hvfhv` |
| `source_period` | Calendar month encoded by the official filename |
| `logical_url` | Official HTTPS object URL |
| `content_sha256` | Hash of downloaded bytes; never inferred from headers |
| `content_length_bytes` | Observed object length |
| `acquired_at_utc` | Actual acquisition timestamp |
| `source_etag` | Advisory source metadata, not treated as a checksum |
| `publication_state` | acquired, rejected, published, superseded, or replayed |

Invariants:

- A logical URL may have multiple content versions over time.
- The same content hash for the same URL is a replay/no-op.
- A new hash creates a new version; it never overwrites audit history.
- An incomplete or unreadable file cannot become published.

The version identifier is `sha256(logical_id + "\n" + content_sha256)`, so it is
stable across runs and hosts and changes only when the logical artifact or its
bytes change. Completeness is part of `logical_id`: a bounded derivative, a
synthetic fixture and a complete upstream object can never become versions of
one logical artifact. M1 implements `published`, `replayed`, `repaired` and
`rejected` during acquisition. M2 adds selection: the active version of a
logical artifact is the newest landed manifest the contract accepts, ordered by
publication time with the version id breaking ties, and `activated`,
`superseded` and `rejected` are appended to a ledger that is never rewritten.

Rejecting a newer file hands the active role back to the previous accepted
version instead of stopping the run. That is a correctness rule, not a
convenience: a run that stopped would leave an incrementally maintained table
holding rows a full rebuild would never produce.

A local artifact records whether it is a `bounded_sample` derived from the
upstream object or a `synthetic_fixture`; the local-trip interface refuses a
`complete_object` claim. For the measured M1 slice, each bounded sample's hash
must equal the corresponding hash in M0 evidence before publication. The hash
describes the bytes acquired locally and never the remote monthly object.

## Schema normalization and conflicts

Raw metadata preserves each source name, type, and physical ordinal. Contracted
columns use canonical `snake_case` names matched case-insensitively against the
source spellings a contract declares. If two source columns fold to the same
canonical name, the file is rejected as ambiguous and both names are reported,
rather than choosing one silently.

Source schemas are read with case sensitivity enabled. Case folding belongs to
the contract, not to the reader: under case-insensitive resolution a file
carrying both `airport_fee` and `Airport_fee` cannot be described at all, so the
collision would surface as a reader error instead of a contract decision.

Each service contract owns an explicit target type per canonical field, and each
source type is classified before any value is read:

| Class | Meaning | Outcome |
| --- | --- | --- |
| identity | Same type | Read as is |
| widening | Lossless for every value of the source type | Cast |
| guarded | Lossless only inside a range | Cast, with every row range-checked |
| untyped null | Source declares the Parquet Null logical type | Contract type applied to typed nulls |
| absent | Optional column the file does not carry | Typed null |
| narrowing | Source is wider than the contract | Version rejected |
| incompatible | No defined promotion | Version rejected |

A guarded promotion has to be opted into by the contract column, because it is
only lossless inside a checked range: a 64-bit integer is exact in a double below
2^53, and a value beyond it is quarantined rather than rounded. Narrowing is
never applied silently. A physically untyped, all-null column is cast to the
contract type without inferring anything from the data; the source type stays
recorded as unknown.

These rules cover observed upstream behavior, including
`airport_fee`/`Airport_fee`, integer-width changes, nullable numeric identifiers,
and fields that begin as physically untyped nulls. Every schema recorded by M0
between 2019-02 and 2025-01 is replayed against them in the test suite.

## Physical source occurrence

Grain: one physical row ordinal inside one source-file version.

Technical key: `(source_file_version_id, source_row_ordinal)`.

Invariants:

- Source row count equals occurrence row count before quarantine rules.
- Equal values in different ordinals remain separate occurrences.
- Reprocessing the same version cannot add another published copy.
- Lineage to the exact file version and run is mandatory.

The ordinal is the Parquet reader's physical row index within the version's
single file, and must form a dense `0..n-1` sequence. Lineage columns are
`source_file_version_id`, `source_row_ordinal`, `source_service`,
`source_period`, `source_artifact_completeness`, `source_logical_url`,
`source_content_sha256`, `ingest_run_id` and `ingested_at_utc`; every other
column keeps its source name and type. One source-occurrence table cannot mix completeness
scopes.

## Contracted service trip

Grain: one physical source row of the active version of one source file, after
the contract has been applied.

Technical key: `(source_file_version_id, source_row_ordinal)`.

Invariants:

- Published rows plus quarantined rows equal the source rows of the version.
- Physical Delta tables retain derivation history. Marker-aware reads expose one
  active derivation per logical source artifact; superseded rows stay physically
  auditable but are absent from the published view.
- Every column is deterministic, so two correct runs produce the same table. No
  run identifier or ingestion timestamp is stored; those belong to the ledger.
- Each row records a deterministic derivation id, the contract version, the
  contract fingerprint and the taxi zone lookup version it was joined against.
- A derivation becomes visible only after its contracted, quarantine and incident
  writes have completed. A failed run cannot move the publication boundary.

Rows that break an impossible-value rule are moved whole into a quarantine table
with their reasons; rows that break a recoverable rule stay published and are
recorded as incidents. Quarantine never removes anything from landing or from
the source-occurrence tables.

| Rule | Action |
| --- | --- |
| Pickup instant missing | Quarantine |
| Drop-off strictly before pickup | Quarantine |
| Guarded promotion out of range | Quarantine |
| Pickup outside the declared month | Incident |
| Drop-off instant missing | Incident |
| Zone key null | Incident |
| Zone key absent from the joined lookup version | Incident |
| Local time in the spring-forward hour | Incident |
| Local time in the fall-back hour | Incident |

## Yellow Taxi contract

The contract includes Yellow-specific pickup/drop-off timestamps, taxi zones,
trip distance, passenger count, rate and payment codes, and its itemized fare
components. Impossible durations are quarantined; unknown zone keys and
out-of-period timestamps are recorded as incidents without removing rows. Amount
sign checks are not implemented: upstream publishes negative amounts for
adjustments, and deciding which of them are errors needs evidence this milestone
does not have.

`total_amount` is a Yellow field and is not a universal mobility-revenue metric.
Cash tips are not represented by the source, so tip aggregates cannot be
described as complete passenger tipping.

## HVFHV contract

The contract includes HVFHV-specific request/on-scene/pickup/drop-off timestamps,
dispatching and originating bases, company license, taxi zones, miles, seconds,
passenger fare components, driver pay, shared-ride flags, Access-A-Ride flags,
and wheelchair-accessible-vehicle flags.

`base_passenger_fare` and `driver_pay` are not mapped to Yellow `fare_amount` or
`total_amount`. Shared-ride and accessibility fields remain service-specific.

## Taxi zone reference contract

Source: the official TLC `taxi_zone_lookup.csv`. Grain: one `LocationID` row
within one acquired content version. The source-file version rules above apply
to this reference file, including content hashing and supersession.

Required fields are integer `location_id` and string `borough`, `zone`, and
`service_zone`. A published version must have unique, non-null location IDs. Trip
zone keys join to an explicitly recorded reference version; unknown keys are
quality incidents and are not replaced with a fabricated zone.

## Local-time semantics

TLC pickup, request, on-scene, and drop-off timestamps are source wall-clock
times without a UTC offset. Fareline reads them as `timestamp_ntz`, interprets
them in the `America/New_York` civil-time domain, retains the original naive
value, and derives local date and hour from that wall clock without inventing an
offset. The two local intervals a year that civil time cannot resolve are derived
for the file's own month from the system time-zone database, and rows inside them
are flagged: `local_time_nonexistent` for the spring-forward hour that never
occurred, `local_time_ambiguous` for the fall-back hour that names two instants.
No UTC instant is produced unless another source field can resolve the
offset/fold.

## Analytical products

None of the products below exist yet. They are the declared shape of M3 work,
listed here so the contracted tables are designed against a known target.

- `zone_hour_demand`: service, pickup zone, local date/hour, trip occurrences,
  valid-duration denominator, duration statistics, and quality counts.
- `yellow_fare_day`: Yellow component sums and explicit non-null denominators.
- `hvfhv_fare_day`: HVFHV component sums and explicit non-null denominators.
- `source_coverage`: expected, observed, acquired, rejected, published, and
  superseded files/versions by service and period.
- `quality_incident`: rule, severity, affected version/run, count, safe sample,
  and publication decision.

M3 adds an explicit dimensional product: a trip-occurrence fact at the physical
occurrence grain, conformed date, zone, service, and source-version dimensions,
and separate Yellow/HVFHV fare facts. Conformed dimensions do not make unlike
fare components additive.

Cross-service dashboards may compare occurrence counts, coverage, and compatible
time/zone measures. They may display fare products side by side but cannot sum or
rank unlike components as one homogeneous financial measure.
