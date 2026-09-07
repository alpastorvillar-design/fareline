# Source data and terms

Fareline uses the official [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
page and its linked monthly Parquet objects. The primary sources are Yellow Taxi
and High Volume For-Hire Vehicle records. Green Taxi is not in the current scope.

Official references:

- [Yellow Taxi data dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)
- [High Volume FHV data dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_hvfhs.pdf)
- [Trip Record User Guide](https://www.nyc.gov/assets/tlc/downloads/pdf/trip_record_user_guide.pdf)
- [Taxi zone lookup CSV](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv)
- [NYC.gov Terms of Use](https://www.nyc.gov/main/terms-of-use)
- [NYC Open Data FAQ](https://opendata.cityofnewyork.us/faq/)

TLC states that monthly Parquet files are normally published after a delay, may
undergo schema standardization, and are not guaranteed to be accurate or
complete. The current dictionaries add `cbd_congestion_fee` from 2025 for both
selected services.

The zone lookup is a required reference source for zone-based products. Fareline
versions it by official URL and downloaded content hash because its contents may
be republished independently of monthly trip files. The M0 inspector downloads
the small CSV only to record its byte size, hash, columns, row count, and key
quality; it neither stores nor publishes its rows.

M1 landing does keep acquired bytes, including the zone lookup and the bounded
trip samples, but only inside gitignored Docker volumes on the machine running
the pipeline. Downloads are capped at 4 MiB, which is far below any monthly trip
object. Nothing acquired is committed or published.

NYC Open Data says that Open Data has no use restrictions, while the TLC page
and general NYC.gov terms do not provide an equally explicit license grant for
redistributing the linked Parquet objects. Fareline therefore applies a
conservative boundary:

- no raw TLC trip rows, zone rows, or local samples are committed;
- the repository publishes only URLs, derived metadata, schemas, counts, and
  reproducible code;
- users download source files directly from the official host;
- the repository's MIT license covers only Fareline code and documentation;
- source accuracy and completeness limitations remain visible in products.

This is an engineering distribution policy, not legal advice.
