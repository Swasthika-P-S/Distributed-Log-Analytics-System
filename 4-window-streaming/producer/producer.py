"""
Window-based Stream Processing - Log Producer
Sends structured JSON log events to Kafka at 1 event/second
to enable meaningful time-window aggregations in Spark Streaming.
"""

from kafka import KafkaProducer
import random
import time
import json
import os
from datetime import datetime

# ── Wait for Kafka to be ready ─────────────────────────────────────────────
time.sleep(15)

# ── Kafka Connection ────────────────────────────────────────────────────────
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC        = 'window-logs'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ── Log Data Templates ──────────────────────────────────────────────────────
LOG_LEVELS = ["INFO", "WARN", "ERROR"]
SERVICES   = ["UserService", "PaymentService", "AuthService", "DatabaseService", "CacheService"]

MESSAGES = {
    "INFO":  ["User login successful", "Request processed OK",
              "Transaction completed", "Cache hit", "Health check passed"],
    "WARN":  ["Slow response detected", "High memory usage",
              "Retry attempt #1", "Deprecated API call", "Queue nearing capacity"],
    "ERROR": ["Connection refused", "NullPointerException",
              "Timeout after 30s", "Database unavailable", "Auth token expired"],
}

# Weighted distribution: 60% INFO, 25% WARN, 15% ERROR
LEVEL_WEIGHTS = [0.60, 0.25, 0.15]

print(f"🚀 Window-Producer started  →  topic='{TOPIC}'  broker={KAFKA_BROKER}")
print("   Sending 1 event/second  (Ctrl+C to stop)")
print("=" * 60)

count = 0
while True:
    level   = random.choices(LOG_LEVELS, weights=LEVEL_WEIGHTS, k=1)[0]
    service = random.choice(SERVICES)
    message = random.choice(MESSAGES[level])

    # Add synthetic latency (ms) – ERRORs tend to be slower
    base_latency = {"INFO": 50, "WARN": 200, "ERROR": 800}[level]
    latency_ms   = int(abs(random.gauss(base_latency, base_latency * 0.3)))

    event = {
        "timestamp":  datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "level":      level,
        "service":    service,
        "message":    message,
        "latency_ms": latency_ms,
        "host":       f"node-{random.randint(1, 3)}",
    }

    producer.send(TOPIC, event)
    producer.flush()
    count += 1

    level_icon = {"INFO": "🟢", "WARN": "🟡", "ERROR": "🔴"}[level]
    print(f"{level_icon} [{count:04d}] {event['timestamp']}  "
          f"{level:<5}  {service:<20}  {latency_ms:>4}ms  |  {message}")

    time.sleep(1)   # 1 event / second  →  good for 1-min & 2-min windows
