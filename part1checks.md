docker compose ps

# storage paths
hdfs getconf -confKey dfs.namenode.name.dir
hdfs getconf -confKey dfs.datanode.data.dir
hdfs getconf -confKey dfs.replication
hdfs getconf -confKey dfs.blocksize

# Check DataNodes
hdfs dfsadmin -report

# Check configured filesystem
hdfs getconf -confKey fs.defaultFS

# Check YARN nodes
yarn node -list

# Check HDFS directories
hdfs dfs -ls /

# check input listing
hdfs dfs -ls /input

# Check a file's blocks and locations
hdfs fsck /your/hdfs/file -files -blocks -locations

# List YARN applications
yarn application -list

# read the datasets
hdfs dfs -ls /input | awk 'NF >= 8 {print $8}' | while read -r f; do
    if hdfs dfs -cat "$f" > /dev/null 2>&1; then
        echo "READ SUCCESS: $f"
    else
        echo "READ FAILED: $f"
    fi
done

for f in /input/*.csv; do
    hdfs dfs -checksum "$f"
done

hdfs fsck /input -files -blocks -locations

# restart datanode2 or namenode
docker restart namenode
docker restart datanode2
hdfs dfsadmin -report
hdfs dfs -ls /input

# block health and replica locations
hdfs fsck /input -files -blocks -locations

# check if yarn is ready
yarn node -list -all

# set the streaming JAR path
export STREAMING_JAR=/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.5.0.jar
ls -lh "$STREAMING_JAR"

# test
chmod +x mapper.py reducer.py

export STREAMING_JAR=/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.5.0.jar

hadoop jar "$STREAMING_JAR" \
-D mapreduce.job.name="CA1_Bus_OD_Aggregation" \
-D mapreduce.job.reduces=1 \
-files mapper.py,reducer.py \
-mapper "python mapper.py" \
-reducer "python reducer.py" \
-input /input/bus_od_clean.csv \
-output /output/bus_od_demo

# inspect output
yarn application -list -appStates ALL
yarn logs -applicationId application_1794193477344_0001

# get output.txt
yarn application -status application_1794196556591_0002
hdfs dfs -ls /output/bus_od_demo_repeat
hdfs dfs -get /output/bus_od_demo_repeat/part-00000 /workspace/userfiles/bus_od_results.txt

2026-11-09 03:00:06,992 [main] WARN  org.apache.hadoop.hdfs.DFSClient  - Failed to connect to /172.18.0.4:9866 for file /input/rainfall_clean.csv for block BP-122732488-172.18.0.7-1794191139724:blk_1073741891_1067. Error is No route to host. Add to deadNodes and continue with other datanodes. 
2026-11-09 03:00:07,022 [main] INFO  org.apache.hadoop.hdfs.DFSClient  - Successfully connected to /172.18.0.5:9866 for BP-122732488-172.18.0.7-1794191139724:blk_1073741891_1067
READ SUCCESS: /input/*.csv

The MapReduce job processes the cleaned bus origin-destination dataset stored in HDFS. The Mapper reads each CSV record, skips the header, validates the fields and extracts the month, day type, origin bus stop ID, destination bus stop ID and total trips. It then generates a key using the month, day type, origin and destination, with the total trips as its value. The hour is intentionally excluded from the key because the objective is to aggregate trips across all hours. During the Shuffle and Sort phase, Hadoop automatically groups all intermediate records with identical keys and transfers them to the reducer. The Reducer receives each grouped key and sums its associated trip counts to calculate the total trips for that origin-destination pair. Finally, the aggregated results are written back to HDFS as part-00000, with _SUCCESS indicating that the job completed successfully.