# Source data and terms

Fareline uses the official [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
page and its linked monthly Parquet objects. The primary sources are Yellow Taxi
and High Volume For-Hire Vehicle records. Green Taxi is not in the current scope.

Official references:

- [Yellow Taxi data dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)
- [High Volume FHV data dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_hvfhs.pdf)
- [Trip Record User Guide](https://www.nyc.gov/assets/tlc/downloads/pdf/trip_record_user_guide.pdf)
- [NYC.gov Terms of Use](https://www.nyc.gov/main/terms-of-use)
- [NYC Open Data FAQ](https://opendata.cityofnewyork.us/faq/)

TLC states that monthly Parquet files are normally published after a delay, may
undergo schema standardization, and are not guaranteed to be accurate or
complete. The current dictionaries add `cbd_congestion_fee` from 2025 for both
selected services.

NYC Open Data says that Open Data has no use restrictions, while the TLC page
and general NYC.gov terms do not provide an equally explicit license grant for
redistributing the linked Parquet objects. Fareline therefore applies a
conservative boundary:

- no raw TLC row data or local samples are committed;
- the repository publishes only URLs, derived metadata, schemas, counts, and
  reproducible code;
- users download source files directly from the official host;
- the repository's MIT license covers only Fareline code and documentation;
- source accuracy and completeness limitations remain visible in products.

This is an engineering distribution policy, not legal advice.
