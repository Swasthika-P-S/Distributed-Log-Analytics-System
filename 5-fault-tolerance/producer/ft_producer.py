"""
Fault-Tolerant Kafka Log Producer
====================================
Fault Tolerance Mechanisms:
  • acks='all'                         – waits for ALL ISR replicas to confirm
  • enable_idempotence=True            – exactly-once delivery per partition
  • retries=10 + retry_backoff_ms=500  – retries with back-off on transient errors
  • max_in_flight_requests=1           – prevents message reordering during retry
  • request_timeout / delivery waits   – explicit timeout handling
  • Wraps every send() in try/except   – never crashes on single message failure
  • Connects to all 3 brokers          – survives single broker loss
"""

import json
import os
import random
import sys
import time
from datetime import datetime

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

# ── Config ──────────────────────────────────────────────────────────────────
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092").split(",")
TOPIC         = os.getenv("KAFKA_TOPIC",   "logs")
SEND_INTERVAL = float(os.getenv("SEND_INTERVAL", "1.0"))   # seconds between logs

# ── Log Templates ────────────────────────────────────────────────────────────
LOG_LEVELS = ["INFO", "WARN", "ERROR"]
SERVICES   = ["UserService", "PaymentService", "AuthService", "DatabaseService", "CacheService"]
MESSAGES   = {
    "INFO":  ["User login successful", "Request processed OK",
              "Transaction completed", "Cache hit", "Health check passed"],
    "WARN":  ["Slow response detected", "High memory usage",
              "Retry attempt #1", "Queue nearing capacity", "Deprecated API call"],
    "ERROR": ["Connection refused", "NullPointerException",
              "Timeout after 30s", "Database unavailable", "Auth token expired"],
}
LEVEL_WEIGHTS = [0.60, 0.25, 0.15]


def make_producer(brokers: list[str]) -> KafkaProducer:
    """
    Create a fault-tolerant KafkaProducer with retry / idempotence settings.
    Retries connection up to 10 times with 5-second back-off.
    """
    for attempt in range(1, 11):
        try:
            producer = KafkaProducer(
                bootstrap_servers=brokers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                # ── Fault Tolerance Settings ───────────────────────────────
                acks="all",                              # wait for all ISR replicas
                retries=10,                              # retry on transient failures
                retry_backoff_ms=500,                   # 500 ms between retries
                enable_idempotence=True,                 # exactly-once per partition
                max_in_flight_requests_per_connection=1, # no reorder during retry
                request_timeout_ms=30_000,              # 30 s request timeout
                # ── Batching / Throughput ──────────────────────────────────
                linger_ms=10,                           # batch for 10 ms
                batch_size=16_384,                      # 16 KB batch
            )
            print(f"✅  Connected to Kafka brokers: {brokers}")
            return producer
        except NoBrokersAvailable:
            print(f"⚠️  Attempt {attempt}/10 — no brokers available, retrying in 5 s …",
                  file=sys.stderr)
            time.sleep(5)
    print("❌  Could not connect after 10 attempts. Exiting.", file=sys.stderr)
    sys.exit(1)


def on_send_success(metadata):
    pass   # called on successful delivery (async callback)


def on_send_error(exc):
    print(f"❌  Delivery failed: {exc}", file=sys.stderr)


# ── Main ─────────────────────────────────────────────────────────────────────
print(f"⏳  Waiting 20 s for Kafka cluster to be ready …")
time.sleep(20)

producer = make_producer(KAFKA_BROKERS)

print(f"🚀  Fault-Tolerant Producer started")
print(f"    Brokers : {KAFKA_BROKERS}")
print(f"    Topic   : {TOPIC}")
print(f"    acks=all | idempotent=True | retries=10")
print("=" * 70)

count = 0
fail_count = 0

while True:
    level   = random.choices(LOG_LEVELS, weights=LEVEL_WEIGHTS, k=1)[0]
    service = random.choice(SERVICES)
    message = random.choice(MESSAGES[level])
    base_latency = {"INFO": 50, "WARN": 200, "ERROR": 800}[level]
    latency_ms   = int(abs(random.gauss(base_latency, base_latency * 0.3)))

    event = {
        "timestamp":  datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "level":      level,
        "service":    service,
        "message":    message,
        "latency_ms": latency_ms,
        "host":       f"node-{random.randint(1, 3)}",
        "seq":        count + 1,     # sequence number helps detect loss
    }

    try:
        # send() is async; add callbacks to detect failures
        producer.send(TOPIC, event) \
                .add_callback(on_send_success) \
                .add_errback(on_send_error)
        producer.flush(timeout=10)  # ensure delivery before moving on
        count += 1
        icon = {"INFO": "🟢", "WARN": "🟡", "ERROR": "🔴"}[level]
        print(f"{icon} [{count:05d}] {event['timestamp']}  "
              f"{level:<5}  {service:<20}  {latency_ms:>4}ms")

    except KafkaError as exc:
        # Log the failure but keep running — producer will reconnect
        fail_count += 1
        print(f"⚠️  Send failed (total_failures={fail_count}): {exc}", file=sys.stderr)
        time.sleep(2)   # brief pause before retrying

    time.sleep(SEND_INTERVAL)
