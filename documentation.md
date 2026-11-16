# Troubleshooting History

This document records the setup and execution issues discussed during CA1 work, how they were diagnosed, and their current status.

## 1. Hadoop containers repeatedly restarted

**Symptom:** The NameNode/DataNode containers repeatedly crashed and restarted.

**Cause recorded:** Paths in `hdfs-site.xml` and `docker-compose.yaml` did not point to the intended container storage locations.

**Resolution:** The host-to-container paths were checked and corrected. The containers then stopped restarting. Use `docker compose config -q` to validate the Compose configuration and `docker compose ps` to check service status.

## 2. Raw datasets and cleaning outputs were split across directories

**Symptom:** CSVs were split between `CA1_Datasets` and `userfiles/CA1_Datasets`; the cleaning script also used paths relative to its working directory, so it could read or write in the wrong location.

**Resolution:**

- Raw datasets are consolidated in `userfiles/CA1_Datasets`.
- Cleaned datasets and reports are written under `userfiles/processed_data`.
- The cleaner resolves its paths relative to its own file. The download scripts and EDA notebook use the same directory layout.
- The September Bus OD copies were byte-identical and consolidated. The two BusStops files differed in four coordinates, so both were preserved; `bus_stops.csv` remains the cleaner's input and the other copy is dated.

## 3. Carpark download had missing data for September 11

**Symptom:** The carpark download did not return data for September 11, and restarting the downloader would otherwise repeat completed requests.

**Resolution:** The redownload script retries transient failures and saves progress so completed requests can be skipped on a later run. Existing records are checked to avoid duplicate rows. Missing source observations are not fabricated.

## 4. MapReduce files were in the wrong directory

**Symptom:** The jobs and `mrjob.conf` were nested under `userfiles/mapreduce/mapreduce`, then were briefly moved outside `userfiles`.

**Resolution:** The job files now live in `userfiles/mapreduce`. Since Compose mounts the project root at `/workspace`, their client-container path is `/workspace/userfiles/mapreduce`. `run_yarn_jobs.sh` changes to its own directory before using the job files and configuration.

## 5. Docker Compose reported that no configuration file was provided

**Symptom:** `docker compose exec ...` returned `no configuration file provided: not found`.

**Cause:** Compose was run from a directory that did not contain `docker-compose.yaml`.

**Resolution:** Run Compose from the project root, `CA1_BDATA`, or specify the Compose file explicitly:

```powershell
Set-Location C:\Users\JunHao\Downloads\BDATA_ca1\CA1_BDATA
docker compose ps
```

## 6. The client had no `pip` or `mrjob`

**Symptom:** The runner printed `/usr/bin/python3: No module named pip`, followed by `ModuleNotFoundError: No module named 'mrjob'`.

**Cause:** The base Hadoop image includes Python but not `pip` or `mrjob`. The runner's earlier runtime install attempt could not work without `pip` and suppressed that failure.

**Resolution:** `Dockerfile.mrjob` creates `/opt/mrjob-venv` and installs `mrjob`, PyYAML, and setuptools there. Compose builds that image for both `client` and `nodemanager`; the runner now checks the imports instead of silently attempting an install. Rebuild/recreate them after changing the Dockerfile:

```powershell
docker compose up -d --build --force-recreate client nodemanager
```

Verify Python and the required imports in both containers:

```powershell
docker compose exec -T client python3 --version
docker compose exec -T client python3 -c "import mrjob, yaml"
docker compose exec -T nodemanager python3 --version
docker compose exec -T nodemanager python3 -c "import mrjob, yaml"
```

## 7. YARN Streaming mapper subprocesses failed

**Symptom:** Tasks failed with `PipeMapRed.waitOutputThreads(): subprocess failed with code 1`. YARN retried many map attempts, making the logs look like many separate failures.

**Debugging:** The Hadoop exception was only a wrapper. The underlying Python traceback was found with YARN logs, for example:

```powershell
docker compose exec -T client yarn logs -applicationId application_<id>
```

The traceback showed `ModuleNotFoundError: No module named 'distutils'` inside the task container. Ubuntu 24's system Python 3.12 does not include `distutils`, which MRJob 0.7.4 expects.

**Resolution:** Setuptools was added to the custom virtual environment, and `mrjob.conf` now sets `python_bin` to `/opt/mrjob-venv/bin/python3`, ensuring YARN task processes use that environment. The unsupported `hadoop_home` entry was also removed from `mrjob.conf`.

**Verification:** M1 completed both MapReduce steps successfully against the combined Bus OD input. Its results are in `userfiles/mapreduce_results/m1_results.txt`. The successful M1 run processed 17,919,803 Bus OD data records.

## 8. Cleaned CSVs in HDFS

The cleaned job inputs were verified in `/sg_mobility/canonical/` with sizes matching the local files:

- `bus_od_clean.csv`
- `bus_stops_clean.csv`
- `rainfall_clean.csv`
- `carpark_clean.csv`

Check the current HDFS contents with:

```bash
hdfs dfs -ls -h /sg_mobility/canonical/
```

## Current validation status

The combined Bus OD cleaner output contains all three downloaded months:

| Month | Clean records |
|---|---:|
| 2026-08 | 6,120,164 |
| 2026-09 | 5,863,920 |
| 2026-10 | 5,935,719 |

M1 has been run successfully end-to-end after the Python environment fix. M2-M5 have not yet been verified by a complete run of `userfiles/mapreduce/run_yarn_jobs.sh`.
