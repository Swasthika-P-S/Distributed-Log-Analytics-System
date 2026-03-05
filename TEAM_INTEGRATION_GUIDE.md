# Team Integration Guide (Distributed Setup)

This document outlines how the 5-person team connects their systems across different locations using **ZeroTier**.

---

## 🌐 Networking Layer: ZeroTier

Since the team is in different places, we use [ZeroTier](https://www.zerotier.com/) to create a Virtual Local Area Network (VLAN).

### Step 1: Join the Network
Everyone must run this command in PowerShell (after installing ZeroTier):
```powershell
zerotier-cli join b103a835d25420a8
```

### Step 2: Find Your Managed IP
After joining and being "Authorized" by the network admin, find your ZeroTier IP:
```powershell
ipconfig
```
Look for the **ZeroTier One** adapter. It will have an IP (e.g., `10.147.17.x`). **This is the IP you must share with the team.**

---

## 👥 Team Roles & Connections

| Role | Person | Responsible For | Connection Logic |
| :--- | :--- | :--- | :--- |
| **Producer** | Person 1 (You) | `producer/` | Sends logs to Person 2's ZeroTier IP. |
| **Kafka Manager** | Person 2 | `kafka-cluster/` | Hosts the brokers. Everyone connects to their ZeroTier IP. |
| **Spark Processor** | Person 3 | `spark/` | Reads from Person 2, writes to Person 4. |
| **HDFS Storage** | Person 4 | `hdfs/` (Conceptual) | Hosts the data. |
| **Fault/Monitor** | Person 5 | `monitoring/` | Monitors everyone's health. |

---

**Your Goal:** Read real-time logs from Kafka, calculate metrics, and save them to HDFS.

### Step 1: Set up Spark
Use the `spark` directory in the repository. Ensure you have the Kafka-Spark connector JARs.
- **Docker:** Ensure your container can "see" Person 2's IP.

### Step 2: Connect to Kafka
Use Person 2's IP address (e.g., `172.16.244.172`) and the unified topic `service-logs`.
```python
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.16.244.172:9092,172.16.244.172:9093,172.16.244.172:9094") \
    .option("subscribe", "service-logs") \
    .load()
```

### Step 3: Parse the logs
The schema provided by Person 1 is:
```python
schema = StructType([
    StructField("timestamp", StringType()),
    StructField("service", StringType()),
    StructField("level", StringType()),
    StructField("message", StringType()),
    StructField("metadata", MapType(StringType(), StringType()))
])
```

### Step 4: Window Analytics
Implement a **1-minute tumbling window** for error counts per service.
```python
windowedCounts = logsDF \
    .withWatermark("timestamp", "2 minutes") \
    .groupBy(window("timestamp", "1 minute"), "service") \
    .count()
```

### Step 5: Write to HDFS
Coordinate with Person 4 to get their HDFS URI.
```python
query = windowedCounts.writeStream \
    .format("json") \
    .option("path", "hdfs://PERSON_4_IP:9000/logs/processed/") \
    .option("checkpointLocation", "/tmp/spark-checkpoints") \
    .start()
```

---

## 🧑‍💻 Person 4: HDFS Storage (The "Vault")

**Your Goal:** Provide a distributed storage layer that is always available.

### Step 1: Set up the Cluster
- **1 NameNode** (The manager)
- **2 DataNodes** (The storage)
- **Replication Factor:** Set to `2` in `hdfs-site.xml` (since you have 2 datanodes).

### Step 2: Prepare the folder
Run these commands inside your NameNode container:
```bash
hdfs dfs -mkdir -p /logs/processed
hdfs dfs -chmod -R 777 /logs
```

### Step 3: Provide the URI
Send this URI to **Person 3 (Spark)**:
`hdfs://YOUR_IP:9000/logs/processed/`

### Step 4: Verification for the Team
To prove the integration worked, run this command after Spark starts writing:
```bash
hdfs dfs -ls -R /logs/processed
```

---

## 🔄 The Full Integration Flow

| Order | Person | Action |
|-------|--------|--------|
| 1 | **Person 4** | Start HDFS, provide HDFS URI to Person 3 |
| 2 | **Person 2** | Start Kafka cluster, provide IPs to Person 1 & 3 |
| 3 | **Person 1 (You)** | Update `.env` with Person 2's IP, run `python producer/producer.py` |
| 4 | **Person 3** | Start Spark to process stream and write to HDFS |
| 5 | **Person 5** | Test fault tolerance by killing containers |
