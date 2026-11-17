#!/usr/bin/env bash
set -euo pipefail

# Automatically switch into the directory containing this script (e.g. /workspace/userfiles/mapreduce)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ==============================================================================
# (m1-m5)
# Sets mapreduce.job.name so localhost:8088 displays readable job names
# bash /workspace/userfiles/mapreduce/run_yarn_jobs.sh
# ==============================================================================

CONF_FILE="$SCRIPT_DIR/mrjob.conf"
HDFS_BASE="hdfs:///sg_mobility/canonical"
LOCAL_STOPS="/workspace/userfiles/processed_data/bus_stops_clean.csv"
OUT_DIR="/workspace/userfiles/mapreduce_results"

mkdir -p "$OUT_DIR"

echo "=== [0/5] Checking python3 & mrjob installation on hadoop-client ==="
python3 -m pip install --quiet mrjob pyyaml || true

echo "=== [1/5] Running Question M1: Busiest Directed Bus Corridors on YARN ==="
time python3 "$SCRIPT_DIR/m1_bus_corridors.py" \
  -r hadoop \
  -c "$CONF_FILE" \
  --jobconf mapreduce.job.name="M1_Bus_Corridors" \
  --bus-stops "$LOCAL_STOPS" \
  "$HDFS_BASE/bus_od_clean.csv" \
  > "$OUT_DIR/m1_results.txt"
echo "Saved: $OUT_DIR/m1_results.txt"

echo "=== [2/5] Running Question M2: Rainfall by Station and Day on YARN ==="
time python3 "$SCRIPT_DIR/m2_rainfall_station_day.py" \
  -r hadoop \
  -c "$CONF_FILE" \
  --jobconf mapreduce.job.name="M2_Rainfall_Station_Day" \
  "$HDFS_BASE/rainfall_clean.csv" \
  > "$OUT_DIR/m2_results.txt"
echo "Saved: $OUT_DIR/m2_results.txt"

echo "=== [3/5] Running Question M3: Parking Pressure by Carpark and Day on YARN ==="
time python3 "$SCRIPT_DIR/m3_carpark_pressure.py" \
  -r hadoop \
  -c "$CONF_FILE" \
  --jobconf mapreduce.job.name="M3_Carpark_Pressure" \
  "$HDFS_BASE/carpark_clean.csv" \
  > "$OUT_DIR/m3_results.txt"
echo "Saved: $OUT_DIR/m3_results.txt"

echo "=== [4/5] Running Self-Generated M4: Wet vs. Dry Hour Carpark Occupancy Join on YARN ==="
time python3 "$SCRIPT_DIR/m4_weather_parking_join.py" \
  -r hadoop \
  -c "$CONF_FILE" \
  --jobconf mapreduce.job.name="M4_Weather_Parking_Join" \
  "$HDFS_BASE/rainfall_clean.csv" \
  "$HDFS_BASE/carpark_clean.csv" \
  > "$OUT_DIR/m4_results.txt"
echo "Saved: $OUT_DIR/m4_results.txt"

echo "=== [5/5] Running Self-Generated M5: Bus Peak Tidal Asymmetry on YARN ==="
time python3 "$SCRIPT_DIR/m5_bus_directional_asymmetry.py" \
  -r hadoop \
  -c "$CONF_FILE" \
  --jobconf mapreduce.job.name="M5_Bus_Directional_Asymmetry" \
  --bus-stops "$LOCAL_STOPS" \
  "$HDFS_BASE/bus_od_clean.csv" \
  > "$OUT_DIR/m5_results.txt"
echo "Saved: $OUT_DIR/m5_results.txt"

echo "=== All 5 MapReduce jobs completed on YARN! ==="
ls -lh "$OUT_DIR"