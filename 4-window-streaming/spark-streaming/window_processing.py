"""
Window-based Stream Processing Algorithm
=========================================
Implements TWO window types on a Kafka log stream using
PySpark Structured Streaming:

  1. TUMBLING WINDOW  – 1 minute, non-overlapping
     → Count logs per (level, service) in each bucket
     → Shows discrete, non-overlapping time slices

  2. SLIDING WINDOW   – 2 min window, 30-second slide
     → Rolling avg latency & error-rate trend
     → Windows overlap, giving a smoother view of trends

  PLUS:
  ● Real-time ERROR alerts  – foreachBatch sink that prints
    any ERROR event the moment it arrives
  ● Live statistics          – running totals every trigger cycle

Algorithm References
--------------------
  - Tumbling: window(col("event_time"), "1 minute")
  - Sliding : window(col("event_time"), "2 minutes", "30 seconds")
  - Watermark: tolerate up to 30-second late arrivals
"""

import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, count, avg, sum as spark_sum,
    from_json, to_timestamp, when, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)

# ── Wait for Kafka broker to be fully up ───────────────────────────────────
print("⏳  Waiting 30 s for Kafka to be ready …")
time.sleep(30)

# ── Config ─────────────────────────────────────────────────────────────────
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC  = os.getenv("KAFKA_TOPIC",  "window-logs")

# ── Spark Session ──────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("WindowBasedStreamProcessing")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")   # suppress verbose Spark logs

print("🔥  Spark Structured Streaming started")
print(f"    Broker : {KAFKA_BROKER}")
print(f"    Topic  : {KAFKA_TOPIC}")
print("=" * 70)

# ── JSON Schema for incoming log events ───────────────────────────────────
LOG_SCHEMA = StructType([
    StructField("timestamp",  StringType(),  True),
    StructField("level",      StringType(),  True),
    StructField("service",    StringType(),  True),
    StructField("message",    StringType(),  True),
    StructField("latency_ms", IntegerType(), True),
    StructField("host",       StringType(),  True),
])

# ── Read raw stream from Kafka ─────────────────────────────────────────────
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# ── Parse JSON payload ─────────────────────────────────────────────────────
parsed = (
    raw_stream
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), LOG_SCHEMA).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    # Add a boolean flag for errors (used in sliding window)
    .withColumn("is_error", when(col("level") == "ERROR", 1).otherwise(0))
)

# Apply watermark – tolerate up to 30 s of late data
watermarked = parsed.withWatermark("event_time", "30 seconds")


# ══════════════════════════════════════════════════════════════════════════
# STREAM 1 – Real-time ERROR Alerts  (foreachBatch, no windowing)
# ══════════════════════════════════════════════════════════════════════════
def alert_on_errors(batch_df, batch_id):
    """Print an alert for every ERROR-level event instantly."""
    errors = batch_df.filter(col("level") == "ERROR").collect()
    for row in errors:
        print(
            f"\n🚨  ALERT  [batch={batch_id}]  {row['timestamp']}"
            f"  |  {row['service']:<20}  |  {row['latency_ms']:>4}ms"
            f"  |  {row['message']}"
        )

alert_query = (
    parsed                      # use non-watermarked stream for immediacy
    .writeStream
    .outputMode("append")
    .foreachBatch(alert_on_errors)
    .option("checkpointLocation", "/tmp/checkpoints/alerts")
    .trigger(processingTime="5 seconds")
    .start()
)


# ══════════════════════════════════════════════════════════════════════════
# STREAM 2 – TUMBLING WINDOW  (1 minute, non-overlapping)
#            Group logs by (1-min bucket, level, service)
# ══════════════════════════════════════════════════════════════════════════
tumbling = (
    watermarked
    .groupBy(
        window(col("event_time"), "1 minute"),   # tumbling = only windowDuration
        col("level"),
        col("service"),
    )
    .agg(
        count("*").alias("log_count"),
        avg("latency_ms").alias("avg_latency_ms"),
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("level"),
        col("service"),
        col("log_count"),
        col("avg_latency_ms"),
    )
)

def print_tumbling(batch_df, batch_id):
    rows = batch_df.orderBy("window_start", "level", "service").collect()
    if not rows:
        return
    print(f"\n{'='*70}")
    print(f"🪟  TUMBLING WINDOW (1 min)  —  batch {batch_id}")
    print(f"{'='*70}")
    print(f"  {'Window Start':<22} {'End':<22} {'Level':<6} {'Service':<20} {'Count':>5} {'Avg ms':>6}")
    print(f"  {'-'*22} {'-'*22} {'-'*6} {'-'*20} {'-'*5} {'-'*6}")
    for r in rows:
        icon = {"INFO": "🟢", "WARN": "🟡", "ERROR": "🔴"}.get(r["level"], "⚪")
        print(
            f"  {str(r['window_start']):<22} {str(r['window_end']):<22}"
            f" {icon}{r['level']:<5} {r['service']:<20}"
            f" {r['log_count']:>5}  {r['avg_latency_ms']:>6.1f}"
        )
    print(f"{'='*70}\n")

tumbling_query = (
    tumbling
    .writeStream
    .outputMode("update")
    .foreachBatch(print_tumbling)
    .option("checkpointLocation", "/tmp/checkpoints/tumbling")
    .trigger(processingTime="30 seconds")
    .start()
)


# ══════════════════════════════════════════════════════════════════════════
# STREAM 3 – SLIDING WINDOW  (2 min window, 30-sec slide)
#            Rolling avg latency + error rate across overlapping windows
# ══════════════════════════════════════════════════════════════════════════
sliding = (
    watermarked
    .groupBy(
        window(col("event_time"), "2 minutes", "30 seconds"),  # sliding
        col("service"),
    )
    .agg(
        count("*").alias("total_events"),
        avg("latency_ms").alias("avg_latency_ms"),
        spark_sum("is_error").alias("error_count"),
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("service"),
        col("total_events"),
        col("avg_latency_ms"),
        col("error_count"),
        # error_rate as percentage
        (col("error_count") * lit(100.0) / col("total_events")).alias("error_pct"),
    )
)

def print_sliding(batch_df, batch_id):
    rows = batch_df.orderBy("window_start", "service").collect()
    if not rows:
        return
    print(f"\n{'='*70}")
    print(f"📊  SLIDING WINDOW (2 min / 30 s slide)  —  batch {batch_id}")
    print(f"{'='*70}")
    print(f"  {'Window Start':<22} {'Service':<20} {'Events':>6} {'AvgMs':>6} {'Errors':>6} {'Err%':>5}")
    print(f"  {'-'*22} {'-'*20} {'-'*6} {'-'*6} {'-'*6} {'-'*5}")
    for r in rows:
        pct_bar = "▓" * int((r["error_pct"] or 0) / 10)       # visual bar
        print(
            f"  {str(r['window_start']):<22} {r['service']:<20}"
            f" {r['total_events']:>6}  {r['avg_latency_ms']:>5.1f}"
            f"  {r['error_count']:>5}  {(r['error_pct'] or 0):>4.1f}%  {pct_bar}"
        )
    print(f"{'='*70}\n")

sliding_query = (
    sliding
    .writeStream
    .outputMode("update")
    .foreachBatch(print_sliding)
    .option("checkpointLocation", "/tmp/checkpoints/sliding")
    .trigger(processingTime="30 seconds")
    .start()
)

# ── Keep alive ─────────────────────────────────────────────────────────────
print("\n✅  All 3 streaming queries running:")
print("    • alert_query    – ERROR alerts (every 5 s)")
print("    • tumbling_query – 1-min tumbling window (every 30 s)")
print("    • sliding_query  – 2-min/30-s sliding window (every 30 s)")
print("\n   Waiting for events from Kafka … (Ctrl+C to stop)\n")

spark.streams.awaitAnyTermination()
