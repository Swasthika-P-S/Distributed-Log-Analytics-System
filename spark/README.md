# Spark Streaming Log Processor (Person 3)

This component performs real-time analytics on logs consumed from Kafka and stores processed results in HDFS.

## Features

1.  **1-Minute Tumbling Window**: Tracks error counts per service every minute.
2.  **5-Minute Sliding Window**: Monitors total log volume trends per service.
3.  **Anomaly Detection**: Automatically detects spikes where log volume exceeds a threshold (default: 100/min).
4.  **HDFS Output**: Saves results to `hdfs://namenode:9000/logs/processed/`.

## How it Works

The Spark job uses **Structured Streaming** to:
- Connect to the Kafka cluster at `10.12.75.131`.
- Parse JSON logs according to the schema provided by Person 1.
- Apply watermarking (2 minutes) to handle out-of-order data.
- Execute parallel analytics queries and output to HDFS.

## Running with Docker

Ensure the entire system is running from the root directory:

```powershell
docker-compose up -d --build
```

To monitor the Spark analytics in real-time (anomalies specifically):

```powershell
docker-compose logs -f spark
```

## Output Locations in HDFS

Results are partitioned by time and stored in:
- `/logs/processed/error_metrics`
- `/logs/processed/service_trends`
- `/logs/processed/anomalies`

Verify with:
```powershell
docker exec namenode hdfs dfs -ls -R /logs/processed/
```
