# Hadoop Configuration & Operations Guide

## 2. Hadoop Configuration Verification

### Check Hadoop Storage Directories
```bash
hdfs getconf -confKey dfs.namenode.name.dir
hdfs getconf -confKey dfs.datanode.data.dir
```

### Check Replication Factor and Block Size
```bash
hdfs getconf -confKey dfs.replication
hdfs getconf -confKey dfs.blocksize
```

### Check Default Filesystem
```bash
hdfs getconf -confKey fs.defaultFS
```

### Check Hadoop Version
```bash
hadoop version
```

---

## 3. Check DataNodes and YARN

### Check DataNode Status
```bash
hdfs dfsadmin -report
```

### Check YARN Nodes
```bash
yarn node -list -all
```

### List YARN Applications
```bash
yarn application -list -appStates ALL
```

### Check Docker Containers
Run from Windows PowerShell:
```powershell
docker ps
```

---

## 4. HDFS Directory Structure

### Create Assignment Directories
```bash
hdfs dfs -mkdir -p /sg_mobility/raw
hdfs dfs -mkdir -p /sg_mobility/canonical
hdfs dfs -mkdir -p /sg_mobility/metadata
hdfs dfs -mkdir -p /sg_mobility/quarantine
hdfs dfs -mkdir -p /sg_mobility/manifest
hdfs dfs -mkdir -p /sg_mobility/benchmark
```

### Verify Directory Structure
```bash
hdfs dfs -ls -R /sg_mobility/
```

---

## 5. Upload Datasets to HDFS

### Upload Raw Datasets
```bash
hdfs dfs -put /workspace/userfiles/CA1_Datasets/* /sg_mobility/raw/
```

### Upload Canonical Datasets
```bash
hdfs dfs -put /workspace/userfiles/processed_data/bus_od_clean.csv /sg_mobility/canonical/
hdfs dfs -put /workspace/userfiles/processed_data/bus_stops_clean.csv /sg_mobility/canonical/
hdfs dfs -put /workspace/userfiles/processed_data/carpark_clean.csv /sg_mobility/canonical/
hdfs dfs -put /workspace/userfiles/processed_data/rainfall_clean.csv /sg_mobility/canonical/
```

### Upload Metadata, Quarantine, and Manifest
> **Note:** Only run these commands if the corresponding local directories exist.

```bash
hdfs dfs -put /workspace/userfiles/metadata/* /sg_mobility/metadata/
hdfs dfs -put /workspace/userfiles/quarantine/* /sg_mobility/quarantine/
hdfs dfs -put /workspace/userfiles/manifest/* /sg_mobility/manifest/
```

### Verify Uploaded Datasets
```bash
hdfs dfs -ls -h /sg_mobility/raw/
hdfs dfs -ls -h /sg_mobility/canonical/
hdfs dfs -ls -h /sg_mobility/metadata/
hdfs dfs -ls -h /sg_mobility/quarantine/
hdfs dfs -ls -h /sg_mobility/manifest/
```

---

## 6. Benchmark Dataset Copies

Create benchmark copies of the canonical datasets:
```bash
hdfs dfs -cp /sg_mobility/canonical/bus_od_clean.csv /sg_mobility/benchmark/
hdfs dfs -cp /sg_mobility/canonical/carpark_clean.csv /sg_mobility/benchmark/
hdfs dfs -cp /sg_mobility/canonical/rainfall_clean.csv /sg_mobility/benchmark/
```

### Verify Benchmark Copies
```bash
hdfs dfs -ls -h /sg_mobility/benchmark/
```

---

## 7. Verify Client Can Read HDFS Files

### Read Individual Dataset
```bash
hdfs dfs -cat /sg_mobility/canonical/bus_od_clean.csv | head -5
```


### Read All Canonical Datasets
```bash
for file in bus_od_clean.csv bus_stops_clean.csv carpark_clean.csv rainfall_clean.csv
do
    if hdfs dfs -cat /sg_mobility/canonical/$file > /dev/null 2>&1; then
        echo "READ SUCCESS: $file"
    else
        echo "READ FAILED: $file"
    fi
done
```

### Check File Sizes
```bash
hdfs dfs -du -h /sg_mobility/canonical/
```

### Check File Checksums
```bash
for f in /sg_mobility/canonical/*.csv
do
    hdfs dfs -checksum "$f"
done
```

---

## 8. HDFS Block Health and Replication

### Run Filesystem Check
```bash
hdfs fsck /sg_mobility/ -files -blocks -locations
```

### Save Filesystem Check Output
```bash
hdfs fsck /sg_mobility/ -files -blocks -locations > /workspace/userfiles/hdfs_fsck.txt 2>&1
```

### Inspect Saved Output
```bash
cat /workspace/userfiles/hdfs_fsck.txt
```

### Check DataNode Health Again
```bash
hdfs dfsadmin -report
```

The filesystem check is used to examine:
- HDFS file blocks.
- Block replica locations.
- DataNode distribution.
- Under-replicated blocks.
- Missing or corrupt blocks.

---

## 9. HDFS Fault Tolerance Test

### Check Live and Dead DataNodes
```bash
hdfs dfsadmin -report
```

### Test Reading a Canonical Dataset
```bash
hdfs dfs -cat /sg_mobility/canonical/bus_od_clean.csv > /dev/null
echo $?
```


### Test All Canonical Datasets
```bash
for file in bus_od_clean.csv bus_stops_clean.csv carpark_clean.csv rainfall_clean.csv
do
    if hdfs dfs -cat /sg_mobility/canonical/$file > /dev/null 2>&1; then
        echo "READ SUCCESS: $file"
    else
        echo "READ FAILED: $file"
    fi
done
```

### Check Block Locations After a DataNode Failure
```bash
hdfs fsck /sg_mobility/ -files -blocks -locations
```
*Compare the block locations and replication status before and after the failure.*

---

## 10. Hadoop Streaming MapReduce Demo

### Navigate to the MapReduce Scripts
```bash
cd /workspace/userfiles/utilities/bus_od_demo
```

### Set Hadoop Streaming JAR Path
```bash
export STREAMING_JAR=/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.5.0.jar
```

### Verify JAR
```bash
ls -lh "$STREAMING_JAR"
```

### Set Script Permissions
```bash
chmod +x mapper.py reducer.py
```

### Check Output Directory Existence
```bash
hdfs dfs -test -e /output/sg_mobility_bus_od_demo
echo $?
```
*Expected result:* `1` (Do not submit the job if the output directory already exists. Choose a new output directory instead).

### Submit MapReduce Job to YARN
```bash
hadoop jar "$STREAMING_JAR" \
-D mapreduce.job.name="CA1_SG_Mobility_Bus_OD_DEMO5" \
-D mapreduce.job.reduces=1 \
-files mapper.py,reducer.py \
-mapper "python3 mapper.py" \
-reducer "python3 reducer.py" \
-input /sg_mobility/canonical/bus_od_clean.csv \
-output /output/sg_mobility_bus_od_demo5
```

---

## 11. Verify MapReduce Job Execution

### List YARN Applications
```bash
yarn application -list -appStates ALL
```

### Check Specific Application
> **Note:** Replace the application ID with the ID returned by your latest submission.

```bash
yarn application -status application_XXXXXXXXXXXX_XXXX
```

### Retrieve Application Logs
```bash
yarn logs -applicationId application_XXXXXXXXXXXX_XXXX
```

### Inspect HDFS Output Directory
```bash
hdfs dfs -ls /output/sg_mobility_bus_od_demo
```

### Display First Ten Output Records
```bash
hdfs dfs -cat /output/sg_mobility_bus_od_demo/part-00000 | head -10
```

### Download MapReduce Output to Local Workspace
```bash
hdfs dfs -get /output/sg_mobility_bus_od_demo/part-00000 /workspace/userfiles/bus_od_results.txt
```