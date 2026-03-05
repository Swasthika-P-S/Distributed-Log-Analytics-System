from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, current_timestamp, when, lit
from pyspark.sql.types import StructType, StructField, StringType, MapType
import os
import time

# ==============================================================================
# CONFIGURATION (Person 3: Spark Streaming Processor)
# ==============================================================================

# Kafka Configuration
# Internal Docker network addresses are used for stability within containers
# Defaults to system IP 10.12.75.131 for external flexibility
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BROKER", "10.12.75.131:9092,10.12.75.131:9093,10.12.75.131:9094")
KAFKA_TOPIC = "service-logs"
GROUP_ID = "spark-log-processor"

# HDFS Configuration (Output paths for Person 4)
HDFS_BASE_PATH = os.getenv("HDFS_URI", "hdfs://namenode:9000/logs/processed/")
CHECKPOINT_BASE = os.getenv("CHECKPOINT_LOCATION", "hdfs://namenode:9000/spark-checkpoints/")

# Anomaly Detection Threshold
# Alert if log count > threshold in a 1-minute window
ALERT_THRESHOLD = int(os.getenv("ALERT_THRESHOLD", "100"))

# ==============================================================================
# LOG SCHEMA (Contract from Person 1)
# ==============================================================================
log_schema = StructType([
    StructField("timestamp", StringType(), True),         # Log event time
    StructField("service", StringType(), True),           # Service name
    StructField("level", StringType(), True),             # INFO/WARN/ERROR
    StructField("message", StringType(), True),           # Detailed log message
    StructField("metadata", MapType(StringType(), StringType()), True) # Additional fields
])

def main():
    # Initialize Spark Session with Kafka support
    spark = SparkSession.builder \
        .appName("RealTime-Log-Analytics-P3") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_BASE) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print(f"🚀 Initializing Spark Streaming Job...")
    print(f"📡 Kafka Brokers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"💾 HDFS Destination: {HDFS_BASE_PATH}")

    # --------------------------------------------------------------------------
    # 1. READ STREAM FROM KAFKA
    # --------------------------------------------------------------------------
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # --------------------------------------------------------------------------
    # 2. PARSE AND CLEAN DATA
    # --------------------------------------------------------------------------
    # Cast Kafka binary value to string and parse JSON
    parsed_df = raw_stream.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), log_schema).alias("data")) \
        .select("data.*")

    # Convert timestamp string to Spark Timestamp type
    # Apply a 2-minute watermark to handle late-arriving logs
    logs_df = parsed_df.withColumn("timestamp", col("timestamp").cast("timestamp")) \
        .withWatermark("timestamp", "2 minutes")

    # --------------------------------------------------------------------------
    # 3. REAL-TIME ANALYTICS (WINDOWED OPERATIONS)
    # --------------------------------------------------------------------------
    
    # --- TASK A: Error Count (1-Minute Tumbling Window) ---
    # Monitor errors per service every minute
    error_metrics = logs_df.filter(col("level") == "ERROR") \
        .groupBy(window(col("timestamp"), "1 minute"), col("service")) \
        .count() \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "service",
            col("count").alias("error_count")
        )

    # --- TASK B: Service Trends (5-Minute Sliding Window) ---
    # Track log volume trends every 1 minute using a 5-minute sliding window
    service_trends = logs_df \
        .groupBy(window(col("timestamp"), "5 minutes", "1 minute"), col("service")) \
        .count() \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "service",
            col("count").alias("total_logs")
        )

    # --- TASK C: Anomaly Detection (Spike Detection) ---
    # Flag events where log volume exceeds the threshold within a 1-minute window
    anomalies = logs_df \
        .groupBy(window(col("timestamp"), "1 minute"), col("service")) \
        .count() \
        .filter(col("count") > ALERT_THRESHOLD) \
        .select(
            col("window.start").alias("timestamp"),
            "service",
            col("count").alias("log_volume"),
            lit("SPIKE_DETECTED").alias("alert_type"),
            lit(f"Log volume exceeded {ALERT_THRESHOLD}/min").alias("alert_details")
        )

    # --------------------------------------------------------------------------
    # 4. OUTPUT RESULTS (HDFS & CONSOLE)
    # --------------------------------------------------------------------------
    
    # Write Errors to HDFS
    q1 = error_metrics.writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", f"{HDFS_BASE_PATH}error_metrics") \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}errors") \
        .start()

    # Write Trends to HDFS
    q2 = service_trends.writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", f"{HDFS_BASE_PATH}service_trends") \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}trends") \
        .start()

    # Write Anomalies to HDFS
    q3 = anomalies.writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", f"{HDFS_BASE_PATH}anomalies") \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}anomalies") \
        .start()

    # Write Anomalies to Console for immediate alerts
    q4 = anomalies.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    print(f"✅ Analytics Pipelines active. Monitoring logs...")
    
    # Wait for all queries to finish (or run indefinitely)
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
