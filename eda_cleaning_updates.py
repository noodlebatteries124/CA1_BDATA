import hashlib
import time
import pandas as pd
import numpy as np
from pathlib import Path

t_start = time.time()


# Print timestamped progress messages during pipeline execution
def log_step(msg):
    elapsed = time.time() - t_start
    print(f"[{elapsed:6.1f}s] {msg}", flush=True)


# Validate dataframe rows against rule masks and split into clean and quarantine dataframes
def validate_and_quarantine(df, validation_rules):
    # Vectorized OR across rule masks so we only build reason strings on invalid rows
    invalid_any = pd.Series(False, index=df.index)
    for invalid_mask in validation_rules.values():
        invalid_any |= invalid_mask.fillna(False)

    clean = df.loc[~invalid_any].copy()
    quarantine = df.loc[invalid_any].copy()

    if len(quarantine) > 0:
        reason_parts = []
        for reason_code, invalid_mask in validation_rules.items():
            sub_mask = invalid_mask.loc[invalid_any].fillna(False)
            reason_parts.append(
                pd.Series(
                    np.where(sub_mask, reason_code, ""),
                    index=quarantine.index
                )
            )
        reason_df = pd.concat(reason_parts, axis=1)
        quarantine["quarantine_reason"] = reason_df.apply(
            lambda row: "|".join(value for value in row if value != ""),
            axis=1
        )
    else:
        quarantine["quarantine_reason"] = pd.Series(dtype="string")

    return clean, quarantine


# Compute SHA-256 hex digest for a file in 1 MB chunks
def compute_sha256(file_path):
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


# ==============================================================================
# 1. PATHS & DIRECTORY SETUP
# ==============================================================================
PROJECT_DIR = Path(__file__).resolve().parent
USERFILES_DIR = PROJECT_DIR / "userfiles"
DATA_DIR = USERFILES_DIR / "CA1_Datasets"
if not DATA_DIR.exists() and (PROJECT_DIR / "CA1_Datasets").exists():
    USERFILES_DIR = PROJECT_DIR
    DATA_DIR = PROJECT_DIR / "CA1_Datasets"

OUTPUT_DIR = USERFILES_DIR / "processed_data"
QUARANTINE_DIR = OUTPUT_DIR / "quarantine"
REPORT_DIR = OUTPUT_DIR / "reports"
METADATA_DIR = OUTPUT_DIR / "metadata"
MANIFEST_DIR = OUTPUT_DIR / "manifest"

for directory in [OUTPUT_DIR, QUARANTINE_DIR, REPORT_DIR, METADATA_DIR, MANIFEST_DIR]:
    directory.mkdir(parents=True, exist_ok=True)


# ==============================================================================
# 2. LOAD RAW DATASETS (INCLUDING ALL AVAILABLE BUS OD MONTHS)
# ==============================================================================
log_step("Step 1/6: Loading raw datasets from CA1_Datasets/...")

carpark_raw = pd.read_csv(
    DATA_DIR / "carpark_20260901_20260930.csv",
    dtype={
        "carpark_number": "string",
        "lot_type": "string"
    }
)
log_step(f"  Loaded carpark_20260901_20260930.csv: {len(carpark_raw):,} rows")

bus_stops_raw = pd.read_csv(
    DATA_DIR / "bus_stops.csv",
    dtype={
        "BusStopCode": "string",
        "RoadName": "string",
        "Description": "string"
    }
)
log_step(f"  Loaded bus_stops.csv: {len(bus_stops_raw):,} rows")

rainfall_raw = pd.read_csv(
    DATA_DIR / "rainfall_20260901_20260930.csv",
    dtype={
        "station_id": "string",
        "station_name": "string"
    }
)
log_step(f"  Loaded rainfall_20260901_20260930.csv: {len(rainfall_raw):,} rows")

bus_od_dtype = {
    "YEAR_MONTH": "string",
    "DAY_TYPE": "string",
    "PT_TYPE": "string",
    "ORIGIN_PT_CODE": "string",
    "DESTINATION_PT_CODE": "string"
}

seen_od_names = set()
bus_od_files = []
for p in sorted(DATA_DIR.rglob("origin_destination_bus_*.csv")):
    if p.name not in seen_od_names:
        seen_od_names.add(p.name)
        bus_od_files.append(p)

if not bus_od_files:
    raise FileNotFoundError(f"No origin_destination_bus_*.csv files found in {DATA_DIR}")

od_frames = []
for fpath in bus_od_files:
    df_month = pd.read_csv(fpath, dtype=bus_od_dtype)
    log_step(
        f"  Loaded {fpath.name}: {len(df_month):,} rows "
        f"(months: {df_month['YEAR_MONTH'].unique().tolist()})"
    )
    od_frames.append(df_month)

bus_od_raw = pd.concat(od_frames, ignore_index=True)
del od_frames
log_step(f"  Combined Bus OD total rows: {len(bus_od_raw):,}")


# ==============================================================================
# 3. CLEAN BUS STOPS METADATA & BUS OD
# ==============================================================================
log_step("Step 2/6: Validating and cleaning Bus Stops and Bus OD...")

bus_stops_raw["Latitude"] = pd.to_numeric(bus_stops_raw["Latitude"], errors="coerce")
bus_stops_raw["Longitude"] = pd.to_numeric(bus_stops_raw["Longitude"], errors="coerce")

bus_stops_rules = {
    "MISSING_REQUIRED_VALUE": (
        bus_stops_raw[
            ["BusStopCode", "RoadName", "Description", "Latitude", "Longitude"]
        ].isna().any(axis=1)
        | bus_stops_raw["BusStopCode"].str.strip().eq("")
        | bus_stops_raw["RoadName"].str.strip().eq("")
        | bus_stops_raw["Description"].str.strip().eq("")
    ),
    "INVALID_COORDINATES": (
        ~bus_stops_raw["Latitude"].between(-90, 90)
        | ~bus_stops_raw["Longitude"].between(-180, 180)
    ),
    "DUPLICATE_BUS_STOP_CODE": bus_stops_raw.duplicated(
        subset=["BusStopCode"],
        keep=False
    )
}

bus_stops_clean, bus_stops_quarantine = validate_and_quarantine(
    bus_stops_raw,
    bus_stops_rules
)
log_step(
    f"  Bus Stops -> clean: {len(bus_stops_clean):,}, "
    f"quarantined: {len(bus_stops_quarantine):,}"
)

bus_od_raw["TIME_PER_HOUR"] = pd.to_numeric(bus_od_raw["TIME_PER_HOUR"], errors="coerce")
bus_od_raw["TOTAL_TRIPS"] = pd.to_numeric(bus_od_raw["TOTAL_TRIPS"], errors="coerce")

bus_od_key = [
    "YEAR_MONTH",
    "DAY_TYPE",
    "TIME_PER_HOUR",
    "PT_TYPE",
    "ORIGIN_PT_CODE",
    "DESTINATION_PT_CODE"
]

bus_od_rules = {
    "MISSING_REQUIRED_VALUE": (
        bus_od_raw[
            [
                "YEAR_MONTH", "DAY_TYPE", "TIME_PER_HOUR", "PT_TYPE",
                "ORIGIN_PT_CODE", "DESTINATION_PT_CODE", "TOTAL_TRIPS"
            ]
        ].isna().any(axis=1)
    ),
    "INVALID_YEAR_MONTH": (
        ~bus_od_raw["YEAR_MONTH"].str.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", na=False)
    ),
    "INVALID_DAY_TYPE": (
        ~bus_od_raw["DAY_TYPE"].isin(["WEEKDAY", "WEEKENDS/HOLIDAY"])
    ),
    "INVALID_PT_TYPE": (
        bus_od_raw["PT_TYPE"] != "BUS"
    ),
    "INVALID_HOUR": (
        ~bus_od_raw["TIME_PER_HOUR"].between(0, 23)
        | (bus_od_raw["TIME_PER_HOUR"] % 1 != 0)
    ),
    "INVALID_TRIP_COUNT": (
        bus_od_raw["TOTAL_TRIPS"].isna()
        | (bus_od_raw["TOTAL_TRIPS"] < 0)
        | (bus_od_raw["TOTAL_TRIPS"] % 1 != 0)
    ),
    "DUPLICATE_ANALYTICAL_KEY": bus_od_raw.duplicated(
        subset=bus_od_key,
        keep=False
    )
}

bus_od_clean, bus_od_quarantine = validate_and_quarantine(
    bus_od_raw,
    bus_od_rules
)
bus_od_clean["TIME_PER_HOUR"] = bus_od_clean["TIME_PER_HOUR"].astype("int64")
bus_od_clean["TOTAL_TRIPS"] = bus_od_clean["TOTAL_TRIPS"].astype("int64")

log_step(
    f"  Bus OD -> clean: {len(bus_od_clean):,}, "
    f"quarantined: {len(bus_od_quarantine):,}"
)
print("  Accepted Bus OD rows per month:")
print(bus_od_clean["YEAR_MONTH"].value_counts().sort_index().to_string())


# ==============================================================================
# 4. CLEAN RAINFALL & COMPUTE DAILY COVERAGE / MISSING SLOTS
# ==============================================================================
log_step("Step 3/6: Validating and cleaning Rainfall...")

rainfall_raw["rainfall_mm"] = pd.to_numeric(rainfall_raw["rainfall_mm"], errors="coerce")
rainfall_raw["timestamp"] = (
    pd.to_datetime(rainfall_raw["timestamp"], format="ISO8601", errors="coerce", utc=True, cache=True)
    .dt.tz_convert("Asia/Singapore")
)

rainfall_key = ["station_id", "timestamp"]

rainfall_rules = {
    "MISSING_REQUIRED_VALUE": (
        rainfall_raw[["timestamp", "station_id", "rainfall_mm"]].isna().any(axis=1)
    ),
    "INVALID_RAINFALL": (
        rainfall_raw["rainfall_mm"].isna()
        | (rainfall_raw["rainfall_mm"] < 0)
    ),
    "MISALIGNED_TIMESTAMP": (
        rainfall_raw["timestamp"].notna()
        & (rainfall_raw["timestamp"] != rainfall_raw["timestamp"].dt.floor("5min"))
    ),
    "DUPLICATE_STATION_TIMESTAMP": rainfall_raw.duplicated(
        subset=rainfall_key,
        keep=False
    )
}

rainfall_clean, rainfall_quarantine = validate_and_quarantine(
    rainfall_raw,
    rainfall_rules
)
log_step(
    f"  Rainfall -> clean: {len(rainfall_clean):,}, "
    f"quarantined: {len(rainfall_quarantine):,}"
)

# Compute daily station coverage and missing 5-minute slots from validated rainfall_clean
rainfall_local_dates = rainfall_clean["timestamp"].dt.date

rainfall_daily = (
    rainfall_clean.assign(local_date=rainfall_local_dates)
    .groupby(["station_id", "local_date"])
    .agg(
        observed_rainfall_mm=("rainfall_mm", "sum"),
        observed_slots=("timestamp", "nunique")
    )
    .reset_index()
)

stations = rainfall_clean["station_id"].unique()
expected_timestamps = pd.date_range(
    "2026-09-01 00:00:00",
    "2026-09-30 23:55:00",
    freq="5min",
    tz="Asia/Singapore"
)

expected_slot_keys = pd.MultiIndex.from_product(
    [stations, expected_timestamps],
    names=["station_id", "timestamp_local"]
)
observed_slot_keys = pd.MultiIndex.from_frame(
    rainfall_clean[["station_id", "timestamp"]].drop_duplicates().rename(
        columns={"timestamp": "timestamp_local"}
    )
)
rainfall_missing_slots = expected_slot_keys.difference(
    observed_slot_keys,
    sort=False
).to_frame(index=False)

dates = pd.date_range("2026-09-01", "2026-09-30").date
expected_grid = pd.MultiIndex.from_product(
    [stations, dates],
    names=["station_id", "local_date"]
).to_frame(index=False)

rainfall_daily = expected_grid.merge(
    rainfall_daily,
    on=["station_id", "local_date"],
    how="left"
)
rainfall_daily["observed_rainfall_mm"] = rainfall_daily["observed_rainfall_mm"].fillna(0.0)
rainfall_daily["observed_slots"] = rainfall_daily["observed_slots"].fillna(0).astype(int)
rainfall_daily["coverage_pct"] = rainfall_daily["observed_slots"] / 288 * 100
rainfall_daily["eligible_for_ranking"] = rainfall_daily["observed_slots"] >= 260

log_step(
    f"  Rainfall coverage -> eligible station-days: {rainfall_daily['eligible_for_ranking'].sum():,}, "
    f"partial (<260 slots): {(~rainfall_daily['eligible_for_ranking']).sum():,}, "
    f"missing 5-min slots: {len(rainfall_missing_slots):,}"
)


# ==============================================================================
# 5. CLEAN CARPARK DATA
# ==============================================================================
log_step("Step 4/6: Validating and cleaning Carpark Availability...")

carpark_raw["snapshot_time"] = (
    pd.to_datetime(carpark_raw["snapshot_time"], format="ISO8601", errors="coerce", utc=True, cache=True)
    .dt.tz_convert("Asia/Singapore")
)
carpark_raw["update_datetime"] = pd.to_datetime(
    carpark_raw["update_datetime"],
    format="ISO8601",
    errors="coerce",
    cache=True
)
carpark_raw["lots_available"] = pd.to_numeric(carpark_raw["lots_available"], errors="coerce")
carpark_raw["total_lots"] = pd.to_numeric(carpark_raw["total_lots"], errors="coerce")

carpark_key = ["carpark_number", "lot_type", "snapshot_time"]

carpark_rules = {
    "MISSING_REQUIRED_VALUE": (
        carpark_raw[
            [
                "snapshot_time", "carpark_number", "lot_type",
                "lots_available", "total_lots", "update_datetime"
            ]
        ].isna().any(axis=1)
    ),
    "INVALID_CAPACITY": (
        carpark_raw["total_lots"].isna()
        | (carpark_raw["total_lots"] <= 0)
    ),
    "INVALID_AVAILABLE_LOTS": (
        carpark_raw["lots_available"].isna()
        | (carpark_raw["lots_available"] < 0)
    ),
    "AVAILABLE_EXCEEDS_CAPACITY": (
        carpark_raw["lots_available"] > carpark_raw["total_lots"]
    ),
    "DUPLICATE_OBSERVATION_KEY": carpark_raw.duplicated(
        subset=carpark_key,
        keep=False
    )
}

carpark_clean, carpark_quarantine = validate_and_quarantine(
    carpark_raw,
    carpark_rules
)
carpark_clean["lots_available"] = carpark_clean["lots_available"].astype("int64")
carpark_clean["total_lots"] = carpark_clean["total_lots"].astype("int64")

log_step(
    f"  Carpark -> clean: {len(carpark_clean):,}, "
    f"quarantined: {len(carpark_quarantine):,}"
)


# ==============================================================================
# 6. EXPORT CLEAN CSVS, QUARANTINE, METADATA & REPORTS
# ==============================================================================
log_step("Step 5/6: Exporting cleaned CSVs, metadata, quarantine, and reports...")

bus_od_cols = [
    "YEAR_MONTH", "DAY_TYPE", "TIME_PER_HOUR", "PT_TYPE",
    "ORIGIN_PT_CODE", "DESTINATION_PT_CODE", "TOTAL_TRIPS"
]
bus_stops_cols = [
    "BusStopCode", "RoadName", "Description", "Latitude", "Longitude"
]
rainfall_cols = [
    "timestamp", "station_id", "station_name", "latitude", "longitude", "rainfall_mm"
]
carpark_cols = [
    "snapshot_time", "carpark_number", "lot_type",
    "lots_available", "total_lots", "update_datetime"
]

log_step("  Writing bus_od_clean.csv...")
bus_od_clean[bus_od_cols].to_csv(OUTPUT_DIR / "bus_od_clean.csv", index=False)

log_step("  Writing bus_stops_clean.csv and metadata tables...")
bus_stops_clean[bus_stops_cols].to_csv(OUTPUT_DIR / "bus_stops_clean.csv", index=False)
bus_stops_clean[bus_stops_cols].to_csv(METADATA_DIR / "bus_stops.csv", index=False)

weather_stations = (
    rainfall_clean[["station_id", "station_name", "latitude", "longitude"]]
    .drop_duplicates(subset=["station_id"])
    .sort_values("station_id")
)
weather_stations.to_csv(METADATA_DIR / "weather_stations.csv", index=False)

log_step("  Writing rainfall_clean.csv...")
rainfall_clean[rainfall_cols].to_csv(OUTPUT_DIR / "rainfall_clean.csv", index=False)

log_step("  Writing carpark_clean.csv...")
carpark_clean[carpark_cols].to_csv(OUTPUT_DIR / "carpark_clean.csv", index=False)

log_step("  Writing quarantine CSVs and reports...")
bus_od_quarantine.to_csv(QUARANTINE_DIR / "bus_od_quarantine.csv", index=False)
bus_stops_quarantine.to_csv(QUARANTINE_DIR / "bus_stops_quarantine.csv", index=False)
rainfall_quarantine.to_csv(QUARANTINE_DIR / "rainfall_quarantine.csv", index=False)
carpark_quarantine.to_csv(QUARANTINE_DIR / "carpark_quarantine.csv", index=False)

rainfall_missing_slots.to_csv(REPORT_DIR / "rainfall_missing_slots.csv", index=False)
rainfall_daily.to_csv(REPORT_DIR / "rainfall_daily_coverage.csv", index=False)

valid_stop_codes = set(bus_stops_clean["BusStopCode"].dropna())
unmatched_orig_mask = ~bus_od_clean["ORIGIN_PT_CODE"].isin(valid_stop_codes)
unmatched_dest_mask = ~bus_od_clean["DESTINATION_PT_CODE"].isin(valid_stop_codes)
unmatched_od_rows = bus_od_clean.loc[unmatched_orig_mask | unmatched_dest_mask].copy()
unmatched_od_rows["unmatched_endpoint"] = np.select(
    [
        (unmatched_orig_mask & unmatched_dest_mask).loc[unmatched_od_rows.index],
        unmatched_orig_mask.loc[unmatched_od_rows.index],
        unmatched_dest_mask.loc[unmatched_od_rows.index]
    ],
    ["BOTH", "ORIGIN_ONLY", "DESTINATION_ONLY"],
    default="NONE"
)
unmatched_summary = (
    unmatched_od_rows
    .groupby(["YEAR_MONTH", "ORIGIN_PT_CODE", "DESTINATION_PT_CODE", "unmatched_endpoint"])
    .agg(row_count=("TOTAL_TRIPS", "size"), total_trips=("TOTAL_TRIPS", "sum"))
    .reset_index()
    .sort_values("total_trips", ascending=False)
)
unmatched_summary.to_csv(REPORT_DIR / "bus_od_unmatched_stops.csv", index=False)


# ======================================================================
# 7. DATA QUALITY SUMMARY & SHA-256 MANIFEST
# ======================================================================
log_step("Step 6/6: Generating data quality summaries and SHA-256 manifest...")

quality_summary = pd.DataFrame([
    {
        "dataset": "bus_od (all months)",
        "original_records": len(bus_od_raw),
        "clean_records": len(bus_od_clean),
        "quarantined_records": len(bus_od_quarantine)
    },
    {
        "dataset": "bus_stops",
        "original_records": len(bus_stops_raw),
        "clean_records": len(bus_stops_clean),
        "quarantined_records": len(bus_stops_quarantine)
    },
    {
        "dataset": "rainfall",
        "original_records": len(rainfall_raw),
        "clean_records": len(rainfall_clean),
        "quarantined_records": len(rainfall_quarantine)
    },
    {
        "dataset": "carpark",
        "original_records": len(carpark_raw),
        "clean_records": len(carpark_clean),
        "quarantined_records": len(carpark_quarantine)
    }
])

quality_summary["retention_percentage"] = (
    quality_summary["clean_records"]
    / quality_summary["original_records"]
    * 100
)
quality_summary.to_csv(REPORT_DIR / "data_quality_summary.csv", index=False)
print("\nData Quality Summary:")
print(quality_summary.to_string(index=False))

bus_od_monthly_summary = (
    bus_od_raw.groupby("YEAR_MONTH").size().rename("original_records").to_frame()
    .join(bus_od_clean.groupby("YEAR_MONTH").size().rename("clean_records"), how="left")
    .fillna(0)
    .astype(int)
    .reset_index()
)
bus_od_monthly_summary["quarantined_records"] = (
    bus_od_monthly_summary["original_records"] - bus_od_monthly_summary["clean_records"]
)
bus_od_monthly_summary.to_csv(REPORT_DIR / "bus_od_monthly_summary.csv", index=False)
print("\nBus OD Monthly Breakdown:")
print(bus_od_monthly_summary.to_string(index=False))

manifest_rows = []
for fpath in list(bus_od_files) + [
    DATA_DIR / "bus_stops.csv",
    DATA_DIR / "rainfall_20260901_20260930.csv",
    DATA_DIR / "carpark_20260901_20260930.csv",
    OUTPUT_DIR / "bus_od_clean.csv",
    OUTPUT_DIR / "bus_stops_clean.csv",
    OUTPUT_DIR / "rainfall_clean.csv",
    OUTPUT_DIR / "carpark_clean.csv"
]:
    if fpath.exists():
        manifest_rows.append({
            "file_name": fpath.name,
            "relative_path": str(fpath.relative_to(PROJECT_DIR)) if fpath.is_relative_to(PROJECT_DIR) else str(fpath),
            "size_bytes": fpath.stat().st_size,
            "sha256": compute_sha256(fpath)
        })

manifest_df = pd.DataFrame(manifest_rows)
manifest_df.to_csv(MANIFEST_DIR / "manifest_sha256.csv", index=False)
print("\nSHA-256 Manifest:")
print(manifest_df.to_string(index=False))

log_step("Done")