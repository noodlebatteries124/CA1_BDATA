#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# YARN Execution Script for ST1522 Big Data CA1 (M1 - M5)
# The job files and configuration live beside this script.
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

CONF_FILE="mrjob.conf"
HDFS_BASE="hdfs:///sg_mobility/canonical"
LOCAL_STOPS="/workspace/userfiles/processed_data/bus_stops_clean.csv"
OUT_DIR="/workspace/userfiles/mapreduce_results"

mkdir -p "$OUT_DIR"

echo "=== [0/5] Checking mrjob installation on hadoop-client ==="
python3 -c "import mrjob, yaml"

echo "=== [1/5] Running Question M1: Busiest Directed Bus Corridors on YARN ==="
time python3 m1_bus_corridors.py \
  -r hadoop \
  -c "$CONF_FILE" \
  --bus-stops "$LOCAL_STOPS" \
  "$HDFS_BASE/bus_od_clean.csv" \
  > "$OUT_DIR/m1_results.txt"
echo "Saved: $OUT_DIR/m1_results.txt"

echo "=== [2/5] Running Question M2: Rainfall by Station and Day on YARN ==="
time python3 m2_rainfall_station_day.py \
  -r hadoop \
  -c "$CONF_FILE" \
  "$HDFS_BASE/rainfall_clean.csv" \
  > "$OUT_DIR/m2_results.txt"
echo "Saved: $OUT_DIR/m2_results.txt"

echo "=== [3/5] Running Question M3: Parking Pressure by Carpark and Day on YARN ==="
time python3 m3_carpark_pressure.py \
  -r hadoop \
  -c "$CONF_FILE" \
  "$HDFS_BASE/carpark_clean.csv" \
  > "$OUT_DIR/m3_results.txt"
echo "Saved: $OUT_DIR/m3_results.txt"

echo "=== [4/5] Running Self-Generated M4: Wet vs. Dry Hour Carpark Occupancy Join on YARN ==="
time python3 m4_weather_parking_join.py \
  -r hadoop \
  -c "$CONF_FILE" \
  "$HDFS_BASE/rainfall_clean.csv" \
  "$HDFS_BASE/carpark_clean.csv" \
  > "$OUT_DIR/m4_results.txt"
echo "Saved: $OUT_DIR/m4_results.txt"

echo "=== [5/5] Running Self-Generated M5: Bus Peak Tidal Asymmetry on YARN ==="
time python3 m5_bus_directional_asymmetry.py \
  -r hadoop \
  -c "$CONF_FILE" \
  --bus-stops "$LOCAL_STOPS" \
  "$HDFS_BASE/bus_od_clean.csv" \
  > "$OUT_DIR/m5_results.txt"
echo "Saved: $OUT_DIR/m5_results.txt"

echo "=== All 5 MapReduce jobs completed on YARN! OH MY GODDDDD ==="
ls -lh "$OUT_DIR"
