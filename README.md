# CA1 Big Data Analysis

## Data provenance and cleaning

The counts below are from the current `eda.ipynb` cleaning run and the CSV
files in `CA1_Datasets`. They describe the files actually loaded by the
notebook, not illustrative counts.

### Sources, files, periods, and row counts

| Dataset | Input file | Source endpoint used | Observation period | Input = accepted + excluded |
|---|---|---|---|---:|
| Bus origin-destination | `origin_destination_bus_202609.csv` | [LTA DataMall PV/ODBus](https://datamall2.mytransport.sg/ltaodataservice/PV/ODBus), requested for `202609`; the API returned a temporary ZIP download link | September 2026 (`YEAR_MONTH = 2026-09`) | 5,863,920 = 5,863,920 + 0 |
| Bus stops | `bus_stops.csv` | [LTA DataMall BusStops](https://datamall2.mytransport.sg/ltaodataservice/BusStops) | Static reference dataset; snapshot/as-of date to fill from manifest: ______ | 5,209 = 5,209 + 0 |
| Rainfall | `rainfall_20260901_20260930.csv` | [data.gov.sg rainfall API](https://api-open.data.gov.sg/v2/real-time/api/rainfall) | 1 Sep 2026 00:00 through 30 Sep 2026 23:55, Asia/Singapore | 761,445 = 761,445 + 0 invalid rows |
| Carpark availability | `carpark_20260901_20260930.csv` | [data.gov.sg carpark availability API](https://api.data.gov.sg/v1/transport/carpark-availability) | 1 Sep 2026 00:00:37 through 30 Sep 2026 23:00:36, Asia/Singapore; 720 hourly snapshots | 1,745,692 = 1,741,784 + 3,908 |

The acquisition endpoints are visible in `lta_data_download.py` and
`datagov_download.py`. Verify these against the lecturer's frozen dataset
manifest before submission.

### Pinned manifest fingerprints

SHA-256 values must be copied from the lecturer-provided manifest. They are
intentionally left blank because that manifest is not present in this
workspace; no hashes have been invented.

| Input file | Lecturer manifest SHA-256 |
|---|---|
| `origin_destination_bus_202609.csv` | |
| `bus_stops.csv` | |
| `rainfall_20260901_20260930.csv` | |
| `carpark_20260901_20260930.csv` | |

### Cleaning and exclusions

The notebook checks missing/blank values, full-row duplicates, analytical-key
duplicates, timestamp parsing, numeric values, categories, and applicable
coordinate/value ranges. Rows failing cleaning rules are written to
`processed_data/quarantine/`; cleaned inputs are written to `processed_data/`.

| Dataset | Accepted | Excluded and reason |
|---|---:|---|
| Bus origin-destination | 5,863,920 | 0: no missing required fields, invalid month/category/hour/trip values, or duplicate analytical keys |
| Bus stops | 5,209 | 0: no missing required fields, out-of-range coordinates, or duplicate bus-stop codes |
| Rainfall | 761,445 | 0: no missing required fields, negative/unparseable rainfall, misaligned timestamps, or duplicate station/timestamp keys |
| Carpark availability | 1,741,784 | 3,188 with non-positive capacity; 720 where available lots exceeded total capacity; 0 duplicate observation keys |

Carpark exclusions total 3,908 unique rows. The carpark observation key is
`(carpark_number, lot_type, snapshot_time)`. Repeated `update_datetime` values
across different snapshot times are not treated as duplicate observations.
For other datasets, all records belonging to a duplicated analytical key are
quarantined rather than arbitrarily keeping one.

The Bus OD data contains 7,995 rows with at least one endpoint code absent from
the supplied BusStops file (3,542 unmatched origin references, 4,500 unmatched
destination references, with 47 rows unmatched at both ends). These rows are
retained, not silently excluded: the available files do not establish whether
the codes are invalid or simply absent from this BusStops snapshot. Reconcile
against the lecturer's reference data before using this as an exclusion rule.

### Timezones and rainfall slots

Rainfall and carpark `snapshot_time` values are interpreted and exported in
`Asia/Singapore` (UTC+08:00). The carpark API's `update_datetime` values do not
carry an offset; the freshness check compares them as Singapore-local time.
Under that assumption, 2,162 accepted carpark rows have an update time later
than their snapshot time. They are not excluded because the source timezone
semantics and an acceptable tolerance have not been confirmed.

Rainfall observations are assigned to five-minute slots by
`(station_id, timestamp in Asia/Singapore)`. A complete ordinary day has 288
slots, from 00:00 through 23:55. Duplicate station/timestamp rows are
quarantined; no such duplicates were found in this input. Missing observations
are recorded, never filled or simulated.

For 89 observed stations over 30 days, the expected grid contains 768,960
station-slots. There are 761,445 observed and 7,515 missing slots. Of the 2,670
expected station-days:

- 2,483 have all 288 slots.
- 187 have at least one missing slot.
- 39 have fewer than 260 slots and are marked ineligible for ranking under the
  notebook's current rule; 16 of these have no readings.
- The remaining 148 incomplete station-days have 260-287 slots and remain
  eligible under that rule.

The notebook exports the exact missing `(station_id, timestamp_local)` slots
to `processed_data/reports/rainfall_missing_slots.csv` when the export cell is
run. Daily coverage and eligibility are also calculated in the notebook.

### Reference/mapping version

Carpark-to-weather-station mapping file used:  
Mapping file version (from lecturer manifest):  

The mapping/version information is not available in this workspace. Fill these
fields from the lecturer's frozen manifest before submission. No mapping is
assumed in the EDA cleaning described above.

### Other provenance information to fill from the frozen manifest

- Lecturer's manifest filename/location:
- Dataset release/version or frozen-bundle identifier:
- BusStops reference snapshot/as-of date:
- Any additional source URLs or file-level notes supplied by the lecturer:
