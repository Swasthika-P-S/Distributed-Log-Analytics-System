# 🧑💻 Person 4: HDFS Storage (The "Vault")

This folder contains the setup for the HDFS cluster used for long-term storage of processed log data.

## Configuration Details
- **1 NameNode**: The manager of the cluster (port 9000 for HDFS, port 9870 for Web UI).
- **2 DataNodes**: The storage nodes.
- **Replication Factor**: Set to `2` (since we have 2 DataNodes).

## How to use

### 1. Start the HDFS Cluster
Run this command from inside the `hdfs-cluster` directory:
```bash
docker-compose up -d
```

### 2. Prepare the Storage
Once the containers are running, run the setup script inside the NameNode:
```bash
docker exec -it namenode /bin/bash /setup_hdfs.sh
```
*Note: I've already mapped this script if you add it to the volume, but for now you can also just copy-paste the commands from the guide into the terminal:*
```bash
docker exec -it namenode hdfs dfs -mkdir -p /logs/processed
docker exec -it namenode hdfs dfs -chmod -R 777 /logs
```

### 3. Provide the URI to Person 3 (Spark)
Tell Person 3 to use the following URI for writing processed logs:
`hdfs://namenode:9000/logs/processed/`

### 4. Verify the Data
To see if Spark has successfully written files:
```bash
docker exec -it namenode hdfs dfs -ls -R /logs/processed
```

## Dashboard
You can monitor the health of your HDFS cluster at:
[http://localhost:9870](http://localhost:9870)
