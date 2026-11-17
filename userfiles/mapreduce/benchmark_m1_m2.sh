#!/usr/bin/env bash
set -euo pipefail

# Automatically switch into the directory containing this script (e.g. /workspace/userfiles/mapreduce)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ==============================================================================
# Task 3 Performance Benchmark Script: 3 Timed Runs Each for M1 and M2 on YARN
# Sets mapreduce.job.name explicitly so YARN UI shows readable job names instead
# of streamjob*.jar, measures wall-clock seconds, and extracts application IDs.
# bash /workspace/userfiles/mapreduce/benchmark_m1_m2.sh
# ==============================================================================

BENCH_DIR="/workspace/userfiles/benchmarks"
mkdir -p "$BENCH_DIR"

python3 - <<'PYEOF'
import re
import statistics
import subprocess
import sys
import time
from pathlib import Path

script_dir = Path.cwd()
bench_dir = Path("/workspace/userfiles/benchmarks")
bench_dir.mkdir(parents=True, exist_ok=True)

stops_path = Path("/workspace/userfiles/processed_data/bus_stops_clean.csv")
if not stops_path.exists() and Path("/workspace/processed_data/bus_stops_clean.csv").exists():
    stops_path = Path("/workspace/processed_data/bus_stops_clean.csv")

jobs = [
    {
        "name": "M1_Bus_Corridors",
        "base_cmd": [
            "python3", str(script_dir / "m1_bus_corridors.py"),
            "-r", "hadoop",
            "-c", str(script_dir / "mrjob.conf"),
            "--bus-stops", str(stops_path),
            "hdfs:///sg_mobility/benchmark/bus_od_clean.csv"
        ]
    },
    {
        "name": "M2_Rainfall_Station_Day",
        "base_cmd": [
            "python3", str(script_dir / "m2_rainfall_station_day.py"),
            "-r", "hadoop",
            "-c", str(script_dir / "mrjob.conf"),
            "hdfs:///sg_mobility/benchmark/rainfall_clean.csv"
        ]
    }
]

summary_rows = []

for job in jobs:
    jname = job["name"]
    runtimes = []
    run_app_ids = []

    for run_no in range(1, 4):
        condition = "Cold (Run 1)" if run_no == 1 else f"Warm (Run {run_no})"
        out_file = bench_dir / f"{jname.lower()}_run_{run_no}.txt"
        log_file = bench_dir / f"{jname.lower()}_run_{run_no}.log"

        # Pass explicit mapreduce.job.name so localhost:8088 displays readable job names
        cmd = job["base_cmd"][:6] + [
            "--jobconf", f"mapreduce.job.name={jname}_Run_{run_no}"
        ] + job["base_cmd"][6:]

        print(f"=== Running {jname} — Run {run_no}/3 ({condition}) ===", flush=True)
        t0 = time.perf_counter()
        with out_file.open("w", encoding="utf-8") as fout, log_file.open("w", encoding="utf-8") as ferr:
            res = subprocess.run(cmd, stdout=fout, stderr=ferr)

        if res.returncode != 0:
            print(f"\n[ERROR] {jname} failed with exit code {res.returncode}. Last 30 lines of {log_file}:", file=sys.stderr)
            lines = log_file.read_text(encoding="utf-8", errors="replace").splitlines()
            print("\n".join(lines[-30:]), file=sys.stderr)
            sys.exit(res.returncode)

        elapsed = round(time.perf_counter() - t0, 2)
        runtimes.append(elapsed)

        log_text = log_file.read_text(encoding="utf-8", errors="replace")
        apps = sorted(set(re.findall(r"application_\d+_\d+", log_text)))
        run_app_ids.append("|".join(apps))
        print(f"    Completed in {elapsed:.2f}s (YARN apps: {', '.join(apps)}) -> {out_file.name}", flush=True)

    med = round(statistics.median(runtimes), 2)
    rng = round(max(runtimes) - min(runtimes), 2)
    summary_rows.append({
        "job": jname,
        "run_1_cold_s": runtimes[0],
        "run_1_apps": run_app_ids[0],
        "run_2_warm_s": runtimes[1],
        "run_2_apps": run_app_ids[1],
        "run_3_warm_s": runtimes[2],
        "run_3_apps": run_app_ids[2],
        "median_s": med,
        "min_s": min(runtimes),
        "max_s": max(runtimes),
        "range_s": rng
    })

csv_path = bench_dir / "benchmark_summary.csv"
with csv_path.open("w", encoding="utf-8") as f:
    headers = [
        "job", "run_1_cold_s", "run_1_apps",
        "run_2_warm_s", "run_2_apps",
        "run_3_warm_s", "run_3_apps",
        "median_s", "min_s", "max_s", "range_s"
    ]
    f.write(",".join(headers) + "\n")
    for r in summary_rows:
        f.write(",".join(str(r[h]) for h in headers) + "\n")

print("\n=== Task 3 Benchmark Summary (Wall-Clock Seconds) ===")
for r in summary_rows:
    print(
        f"{r['job']}: Run1={r['run_1_cold_s']}s ({r['run_1_apps']}), "
        f"Run2={r['run_2_warm_s']}s ({r['run_2_apps']}), "
        f"Run3={r['run_3_warm_s']}s ({r['run_3_apps']}) | "
        f"Median={r['median_s']}s | Range={r['range_s']}s"
    )
print(f"\nSaved summary table to: {csv_path}")
PYEOF

echo "=== Capturing YARN Application & Container Evidence ==="
yarn application -list -appStates ALL > "$BENCH_DIR/yarn_applications_evidence.txt"
cat "$BENCH_DIR/yarn_applications_evidence.txt"