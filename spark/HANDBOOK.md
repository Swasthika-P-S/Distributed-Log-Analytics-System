# Person 3: Spark Streaming Processor - Handbook

This document contains the full implementation, explanation, and execution guide for the Spark Streaming component of the Distributed Log Analytics System.

## 1. Core Implementation
The component consists of two main files:

- **[spark_job.py](file:///d:/sem6/distributed%20sys/case%20study/Distributed-Log-Analytics-System/spark/spark_job.py)**: The main PySpark application.
- **[Dockerfile](file:///d:/sem6/distributed%20sys/case%20study/Distributed-Log-Analytics-System/spark/Dockerfile)**: container configuration for Spark.

### Features implemented:
1.  **Log Parsing**: Automated parsing of JSON logs with schema validation.
2.  **Error Monitoring**: 1-minute tumbling window to count errors per service.
3.  **Trend Analysis**: 5-minute sliding window to track log volume trends.
4.  **Anomaly Detection**: Real-time spike detection (Threshold customizable).
5.  **HDFS Storage**: Integration with HDFS (Person 4) for persistent storage.

---

## 2. Technical Explanation

### Windowing Strategy
- **1-Minute Tumbling Window**: Divided into fixed, non-overlapping intervals. Used for error counts where we want a precise snapshot of errors per specific minute.
- **5-Minute Sliding Window**: A 5-minute window that slides every 1 minute. This provides a "rolling" view of service logs, allowing us to see increases or decreases in volume gradually (trends).

### Watermarking
We use a **2-minute watermark** on the `timestamp` column. This allows Spark to handle logs that arrive late (up to 2 minutes behind real-time) and helps Spark manage memory by clearing old state that is no longer needed for windowing.

### Output Integration
- Processed data is sent to HDFS in **JSON format** under the `/logs/processed/` directory.
- Anomalies are also printed to the **Standard Output (Console)** for immediate visibility during monitoring.

---

## 3. How to Run with Docker

Ensure the entire system is configured to use the host IP (`10.12.75.131`).

### Step 1: Start the Cluster
Run from the root directory:
```powershell
docker-compose up -d --build
```

### Step 2: Monitor Real-Time Alerts
To see anomaly detection and Spark status in real-time:
```powershell
docker-compose logs -f spark
```

### Step 3: Verify HDFS Output
Check if Spark is successfully writing to Person 4's HDFS storage:
```powershell
docker exec namenode hdfs dfs -ls -R /logs/processed/
```

---

## 4. Environment Configuration
The application is highly configurable via environment variables in `docker-compose.yml`:

| Variable | Default Value | Description |
| :--- | :--- | :--- |
| `KAFKA_BROKER` | `10.12.75.131:9092...` | Kafka cluster discovery address |
| `HDFS_URI` | `hdfs://namenode:9000/...` | Destination in HDFS |
| `ALERT_THRESHOLD` | `100` | Logs/min limit for anomalies |
