# Debugging & Controlled Fault-Testing Journal (`debugging_journal.md`)

This journal documents the actual technical errors encountered and resolved during the setup, data acquisition, data cleaning, and YARN MapReduce execution phases of ST1522 CA1, as well as the controlled HDFS fault-tolerance test.

## Issue 1: NameNode and DataNode Containers Repeatedly Crashing on Startup

* **Phase:** Task 1 — Docker Hadoop Cluster Setup
* **Symptom:** Running `docker compose up` resulted in the `namenode` and `datanode` containers repeatedly crashing and restarting in a loop (`Restarting (1)` in `docker compose ps`).
* **Root Cause:** The host-to-container bind mount paths in `docker-compose.yml` (`./namenode_data:/opt/hadoop/data/name` and `./datanodeX_data:/opt/hadoop/data/data`) did not match the `file:///` URI paths configured in `config/hdfs-configs/hdfs-site.xml` (`dfs.namenode.name.dir` and `dfs.datanode.data.dir`), and the NameNode storage directory lacked an initial `current/VERSION` layout.
* **Fix & Verification:** Aligned `dfs.namenode.name.dir` (`file:///opt/hadoop/data/name`) and `dfs.datanode.data.dir` (`file:///opt/hadoop/data/data`) in `hdfs-site.xml` with the exact container volume targets in `docker-compose.yml`, and added a startup check (`if [ ! -d "/opt/hadoop/data/name/current" ]; then hdfs namenode -format -nonInteractive; fi`) before `exec hdfs namenode`. Verified with `docker compose ps` and `hdfs dfsadmin -report` showing all 3 DataNodes live and in service.

## Issue 2: Missing Carpark API Data on 11 September 2026 & Transient HTTP Rate Limits

* **Phase:** Section 2 / Appendix A — Dataset Acquisition (`datagov_download.py` -> `datagov_redownload.py`)
* **Symptom:** During the initial 30-day September 2026 download from `https://api.data.gov.sg/v1/transport/carpark-availability`, the script failed mid-run due to HTTP `429`/`5xx` responses and missing snapshot responses on 11 September 2026, forcing a full restart from 1 September if using `datagov_download.py`.
* **Root Cause:** `datagov_download.py` opened the output CSV in overwrite mode (`"w"`) without checkpoint tracking and only handled HTTP `429` with a fixed retry loop, without backing up partial downloads or deduplicating already-fetched timestamps on resume.
* **Fix & Verification:** Created `datagov_redownload.py` with exponential backoff across HTTP `{429, 500, 502, 503, 504}` and connection timeouts (`MAX_RETRIES = 8`), backed up the partial CSV to `carpark_20260901_20260930_pre_resume_backup.csv`, tracked completed hourly request timestamps in `carpark_20260901_20260930.progress`, and deduplicated `(snapshot_time, carpark_number, lot_type)` keys on append. All 720 hourly snapshots (`1,745,692` raw rows) for 1–30 September 2026 were successfully retrieved and verified.

## Issue 3: Data Cleaning Script Hanging on 3-Month Combined Datasets (~20.4 Million Rows) & Quarantine Timezone Bug

* **Phase:** Task 2 / Appendix A — Data Quality, Validation & Canonical Export (`eda.ipynb` -> `eda_cleaning_updates.py`)
* **Symptom:** After downloading August (`202608`) and October (`202610`) LTA Bus OD files to join September (`202609`) (`17,919,803` combined Bus OD rows), running `eda_cleaning_updates.py` hung for over 15 minutes during `validate_and_quarantine()`. In addition, inspecting earlier exports showed two data bugs: `carpark_quarantine.csv` had UTC (`+00:00`) timestamps (`2026-08-31 16:00:37+00:00`) instead of `Asia/Singapore` (`+08:00`), and `carpark_clean.csv` was 133 MB instead of 105 MB because an exploratory `update_datetime_age` column leaked into the CSV export.
* **Root Cause:**
  1. `validate_and_quarantine()` ran `reason_df.apply(lambda row: "|".join(...), axis=1)` across all `17,919,803` Bus OD rows and `1,745,692` Carpark rows—invoking a Python lambda 20+ million times even though `0` Bus OD rows and only `3,908` Carpark rows were invalid.
  2. `carpark["snapshot_time"]` was parsed with `utc=True` before calling `validate_and_quarantine()`, and only `carpark_clean` was converted to `.dt.tz_convert("Asia/Singapore")` at the end of the notebook, leaving `carpark_quarantine` in UTC.
  3. Computing `carpark_clean["update_datetime_age"]` mutated `carpark_clean` in-place prior to `.to_csv()`.
* **Fix & Verification:**
  1. Replaced the full-table `.apply(..., axis=1)` in `validate_and_quarantine()` with a vectorized bitwise OR mask (`invalid_any |= invalid_mask.fillna(False)`) so pipe-delimited `quarantine_reason` strings are built **only** on quarantined rows (`3,908` rows total), and added `format="ISO8601", cache=True` to `pd.to_datetime()`. Runtime dropped from >15 minutes to under 45 seconds.
  2. Converted `rainfall["timestamp"]` and `carpark["snapshot_time"]` to `.dt.tz_convert("Asia/Singapore")` *before* `validate_and_quarantine()`, and explicitly selected canonical column lists on export (`carpark_clean.csv` dropped from 133.3 MB to 105.4 MB).
  3. Separated pure exploratory profiling into `eda_pure.ipynb` (`eda.ipynb`) while keeping all cleaning, metadata extraction, and SHA-256 manifest generation in `eda_cleaning_updates.py`.

## Issue 4: YARN Streaming Subprocess Failures (`No module named 'mrjob'` & Python 3.12 `No module named 'distutils'`)

* **Phase:** Task 2 — Executing Python `mrjob` on YARN (`Dockerfile.mrjob` & `mrjob.conf`)
* **Symptom:** Submitting `m1_bus_corridors.py -r hadoop` initially failed on the client with `No module named 'mrjob'`, and after installing `mrjob`, YARN map tasks failed repeatedly on the NodeManager with `PipeMapRed.waitOutputThreads(): subprocess failed with code 1`, producing a 0-byte `bus_od_mrjob_results.txt`.
* **Root Cause:** Inspecting YARN application logs via `yarn logs -applicationId <app_id>` revealed `ModuleNotFoundError: No module named 'distutils'` inside the YARN task container. The base `apache/hadoop:3.5.0` image runs on Ubuntu 24 with Python 3.12, which removed the standard-library `distutils` module that `mrjob 0.7.4` imports at task startup. Additionally, running `bash /workspace/userfiles/mapreduce/benchmark_m1_m2.sh` from `/workspace/userfiles` initially failed with `exit status 2` because relative script paths were resolved against the caller's working directory, and YARN ResourceManager displayed generic `streamjob*.jar` names instead of descriptive job names.
* **Fix & Verification:**
  1. Created `Dockerfile.mrjob` (used by both `client` and `nodemanager` in `docker-compose.yml`) that builds `/opt/mrjob-venv` and installs `mrjob==0.7.4`, `PyYAML==6.0.2`, and `setuptools==75.8.0` (which provides `distutils` for Python 3.12), and set `python_bin: /opt/mrjob-venv/bin/python3` and `hadoop_streaming_jar: /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.5.0.jar` in `mrjob.conf`.
  2. Added `SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"` and `--jobconf mapreduce.job.name=...` to `run_yarn_jobs.sh` and `benchmark_m1_m2.sh`.
  3. Verified that all 23 YARN applications (`application_1794924083577_0001` through `application_1794924083577_0023`) completed with `FINISHED` / `SUCCEEDED` status across `M1`–`M5` and all 6 benchmark runs.

## Issue 5: Controlled HDFS Fault Tolerance & DataNode Re-Replication Test

* **Phase:** Task 1 — HDFS Fault Tolerance and Data Durability (Compulsory Demonstration)
* **Controlled Fault Procedure:**
  1. Configured 3 DataNodes (`datanode1`, `datanode2`, `datanode3`) with `dfs.replication=2`, `dfs.blocksize=4194304` (4 MB), `dfs.heartbeat.interval=2`, and `dfs.namenode.heartbeat.recheck-interval=10000` (40-second dead-node detection threshold).
  2. Verified baseline block health via `hdfs fsck /sg_mobility/ -files -blocks -locations > /workspace/userfiles/hdfs_fsck.txt`: every file under `/sg_mobility/` reported `replication=2` and `Status: HEALTHY` with blocks distributed across `172.18.0.4` (`datanode1`), `172.18.0.5` (`datanode2`), and `172.18.0.6` (`datanode3`).
  3. Safely stopped `datanode2` (`docker stop datanode2`) without deleting its volume. Immediately verified that `/sg_mobility/canonical/bus_od_clean.csv` remained 100% readable via `hdfs dfs -cat /sg_mobility/canonical/bus_od_clean.csv > /dev/null` (`exit code 0`) because every block stored on `datanode2` had a second healthy replica on either `datanode1` or `datanode3`.
  4. Observed under-replicated blocks during the detection window and confirmed that after ~40 seconds (`2 * 10s recheck + 10 * 2s heartbeat`), the NameNode marked `datanode2` dead and automatically scheduled block re-replication between `datanode1` and `datanode3` until all blocks were restored to `Live_repl=2`.
  5. Restarted `datanode2` (`docker start datanode2`) and verified via `hdfs dfsadmin -report` and `localhost:9870` (`3 In operation`) that `datanode2` rejoined the cluster cleanly and HDFS pruned excess third replicas back to the target replication factor of `2`.
