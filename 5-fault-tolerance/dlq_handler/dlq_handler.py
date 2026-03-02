"""
Dead-Letter Queue (DLQ) Handler
====================================
Subscribes to the `logs-dlq` Kafka topic and:
  • Writes every DLQ event to /dlq/dlq_events.log
  • Prints a live dashboard: total DLQ events, error-reason breakdown
  • Helps operators identify WHY messages are failing

A message lands in the DLQ when:
  - JSON parsing fails (malformed payload)
  - Required fields are missing (level, service)
  - Consumer encounters any other processing error
"""

import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# ── Config ───────────────────────────────────────────────────────────────────
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092").split(",")
DLQ_TOPIC     = os.getenv("DLQ_TOPIC",    "logs-dlq")
DLQ_LOG_DIR   = os.getenv("DLQ_LOG_DIR",  "/dlq")
PRINT_EVERY   = int(os.getenv("PRINT_EVERY", "10"))   # print stats every N events

os.makedirs(DLQ_LOG_DIR, exist_ok=True)
DLQ_LOG_FILE = os.path.join(DLQ_LOG_DIR, "dlq_events.log")


def make_consumer():
    for attempt in range(1, 11):
        try:
            consumer = KafkaConsumer(
                DLQ_TOPIC,
                bootstrap_servers=KAFKA_BROKERS,
                group_id="dlq-monitor-group",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: m.decode("utf-8"),
            )
            print(f"✅  DLQ handler connected → topic='{DLQ_TOPIC}'")
            return consumer
        except NoBrokersAvailable:
            print(f"⚠️  Attempt {attempt}/10 — no brokers, retrying in 5 s …", file=sys.stderr)
            time.sleep(5)
    sys.exit(1)


def print_dashboard(total: int, reason_counts: dict, consumer_counts: dict):
    print(f"\n{'='*60}")
    print(f"☠️   DLQ DASHBOARD  —  {datetime.utcnow().strftime('%H:%M:%S UTC')}")
    print(f"{'='*60}")
    print(f"  Total DLQ events : {total}")
    print(f"\n  Failure reasons:")
    for reason, cnt in sorted(reason_counts.items(), key=lambda x: -x[1]):
        bar = "█" * min(cnt, 30)
        print(f"    {reason:<40}  {cnt:>4}  {bar}")
    print(f"\n  Events by consumer:")
    for cid, cnt in sorted(consumer_counts.items(), key=lambda x: -x[1]):
        print(f"    {cid:<20}  {cnt:>4}")
    print(f"{'='*60}\n")


# ── Startup ──────────────────────────────────────────────────────────────────
print("⏳  Waiting 30 s for Kafka cluster …")
time.sleep(30)

print(f"☠️   Dead-Letter Queue Handler started")
print(f"    Brokers  : {KAFKA_BROKERS}")
print(f"    DLQ Topic: {DLQ_TOPIC}")
print(f"    Log file : {DLQ_LOG_FILE}")
print("=" * 60)

consumer       = make_consumer()
total          = 0
reason_counts  = defaultdict(int)
consumer_counts = defaultdict(int)

for message in consumer:
    raw = message.value

    # ── Parse DLQ envelope ────────────────────────────────────────────────
    try:
        event = json.loads(raw)
    except json.JSONDecodeError:
        event = {
            "timestamp":   datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "reason":      "dlq_handler_parse_error",
            "consumer_id": "unknown",
            "raw_payload": raw,
        }

    timestamp   = event.get("timestamp", "?")
    reason      = event.get("reason",      "unknown")
    consumer_id = event.get("consumer_id", "unknown")
    raw_payload = event.get("raw_payload", "")

    # ── Write to log file ─────────────────────────────────────────────────
    log_line = (
        f"[{timestamp}] consumer={consumer_id} "
        f"reason={reason!r} payload={raw_payload[:120]!r}\n"
    )
    with open(DLQ_LOG_FILE, "a") as f:
        f.write(log_line)

    total += 1
    reason_counts[reason]   += 1
    consumer_counts[consumer_id] += 1

    print(f"☠️  DLQ [{total:04d}]  {timestamp}  consumer={consumer_id}  reason={reason!r}")

    # ── Print dashboard every N events ────────────────────────────────────
    if total % PRINT_EVERY == 0:
        print_dashboard(total, dict(reason_counts), dict(consumer_counts))
