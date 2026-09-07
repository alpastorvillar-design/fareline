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

## Schema normalization and conflicts

Raw metadata preserves each source name, type, and physical ordinal. Contracted
columns use canonical `snake_case` names matched case-insensitively. If two source
columns map to the same canonical name, the file is rejected as ambiguous rather
than choosing one silently.

Each service contract owns an explicit target type per canonical field. Safe
widening may be applied only when the contract lists it. Narrowing or otherwise
incompatible changes are recorded as schema incidents and quarantined until a
versioned rule resolves them. A physically untyped, all-null column may be cast
only when its contract type is already known; an unknown untyped column remains
quarantined. Original values and schema metadata remain available for audit.

These rules cover observed upstream behavior, including
`airport_fee`/`Airport_fee`, integer-width changes, nullable numeric identifiers,
and fields that begin as physically untyped nulls.

## Physical source occurrence

Grain: one physical row ordinal inside one source-file version.

Technical key: `(source_file_version_id, source_row_ordinal)`.

Invariants:

- Source row count equals occurrence row count before quarantine rules.
- Equal values in different ordinals remain separate occurrences.
- Reprocessing the same version cannot add another published copy.
- Lineage to the exact file version and run is mandatory.

## Yellow Taxi contract

The contract includes Yellow-specific pickup/drop-off timestamps, taxi zones,
trip distance, passenger count, rate and payment codes, and its itemized fare
components. Validity checks flag impossible durations, negative amounts,
unknown zone keys, and out-of-period timestamps without silently removing rows.

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
times without a UTC offset. Fareline interprets them in the `America/New_York`
civil-time domain, retains the original naive value, and derives local date/hour
without inventing an offset. Ambiguous fall-back times and nonexistent spring
times are flagged. No UTC instant is produced unless another source field can
resolve the offset/fold; duration rules exclude unresolved DST cases from their
valid denominator.

## Analytical products

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
