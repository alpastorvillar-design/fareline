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

## Analytical products

- `zone_hour_demand`: service, pickup zone, local date/hour, trip occurrences,
  valid-duration denominator, duration statistics, and quality counts.
- `yellow_fare_day`: Yellow component sums and explicit non-null denominators.
- `hvfhv_fare_day`: HVFHV component sums and explicit non-null denominators.
- `source_coverage`: expected, observed, acquired, rejected, published, and
  superseded files/versions by service and period.
- `quality_incident`: rule, severity, affected version/run, count, safe sample,
  and publication decision.

Cross-service dashboards may compare occurrence counts, coverage, and compatible
time/zone measures. They may display fare products side by side but cannot sum or
rank unlike components as one homogeneous financial measure.
