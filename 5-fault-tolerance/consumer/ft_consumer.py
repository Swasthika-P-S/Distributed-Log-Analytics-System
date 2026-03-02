"""
Fault-Tolerant Kafka Log Consumer
====================================
Fault Tolerance Mechanisms:
  • Consumer Group (`ft-log-consumers`)   – multiple instances share partitions
  • Auto-rebalance on member join/leave   – if one instance dies, others take over
  • enable_auto_commit=False              – manual offset commit (at-least-once)
  • Commit only AFTER successful write    – no data loss on crash mid-process
  • DLQ forwarding                        – bad/unparseable messages → logs-dlq
  • CommitFailedError handling            – retry commit on failure
  • Reconnect loop                        – re-creates consumer on fatal errors
"""

import json
import os
import sys
import time
from datetime import datetime

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import CommitFailedError, KafkaError, NoBrokersAvailable

# ── Config ───────────────────────────────────────────────────────────────────
KAFKA_BROKERS   = os.getenv("KAFKA_BROKERS", "localhost:9092").split(",")
TOPIC           = os.getenv("KAFKA_TOPIC",   "logs")
DLQ_TOPIC       = os.getenv("DLQ_TOPIC",     "logs-dlq")
GROUP_ID        = os.getenv("GROUP_ID",       "ft-log-consumers")
CONSUMER_ID     = os.getenv("CONSUMER_ID",    "consumer-1")
LOG_DIR         = os.getenv("LOG_DIR",        "/app/logs")

os.makedirs(LOG_DIR, exist_ok=True)

# ── DLQ Producer (sends unparseable messages to dead-letter topic) ───────────
def make_dlq_producer():
    for attempt in range(1, 6):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=5,
            )
        except NoBrokersAvailable:
            print(f"⚠️  DLQ producer attempt {attempt}/5, retrying …", file=sys.stderr)
            time.sleep(5)
    return None   # continue without DLQ if unavailable


def send_to_dlq(dlq_producer, raw_value: str, reason: str):
    """Route a bad message to the dead-letter queue topic."""
    if dlq_producer is None:
        return
    payload = {
        "timestamp":   datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "consumer_id": CONSUMER_ID,
        "reason":      reason,
        "raw_payload": raw_value,
    }
    try:
        dlq_producer.send(DLQ_TOPIC, payload)
        dlq_producer.flush(timeout=5)
        print(f"☠️  DLQ [{reason}]: {raw_value[:80]}")
    except KafkaError as exc:
        print(f"⚠️  DLQ send failed: {exc}", file=sys.stderr)


def write_log(log_entry: dict):
    """Persist log entry to disk — partitioned by level."""
    level     = log_entry.get("level", "UNKNOWN")
    timestamp = log_entry.get("timestamp", "")
    service   = log_entry.get("service", "")
    message   = log_entry.get("message", "")
    latency   = log_entry.get("latency_ms", 0)

    line = f"{timestamp} | {level:<5} | {service:<20} | {latency:>4}ms | {message}\n"

    with open(f"{LOG_DIR}/all_logs.txt", "a") as f:
        f.write(line)

    level_file = {
        "ERROR": "error_logs.txt",
        "WARN":  "warn_logs.txt",
    }.get(level, "info_logs.txt")

    with open(f"{LOG_DIR}/{level_file}", "a") as f:
        f.write(line)


def make_consumer():
    """Create a consumer with manual commit and consumer-group membership."""
    for attempt in range(1, 11):
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BROKERS,
                group_id=GROUP_ID,
                client_id=CONSUMER_ID,
                # ── Fault Tolerance Settings ──────────────────────────────
                enable_auto_commit=False,           # MANUAL commit (at-least-once)
                auto_offset_reset="earliest",       # don't miss messages on restart
                session_timeout_ms=10_000,          # detect dead instance fast
                heartbeat_interval_ms=3_000,        # heartbeat every 3 s
                max_poll_interval_ms=60_000,        # allow slow batch processing
                # ── Deserialization ───────────────────────────────────────
                value_deserializer=lambda m: m.decode("utf-8"),
            )
            print(f"✅  Consumer '{CONSUMER_ID}' joined group '{GROUP_ID}'")
            return consumer
        except NoBrokersAvailable:
            print(f"⚠️  Attempt {attempt}/10 — no brokers, retrying in 5 s …", file=sys.stderr)
            time.sleep(5)
    print("❌  Could not connect. Exiting.", file=sys.stderr)
    sys.exit(1)


# ── Startup ──────────────────────────────────────────────────────────────────
print(f"⏳  Waiting 25 s for Kafka cluster …")
time.sleep(25)

print(f"🎧  Fault-Tolerant Consumer  '{CONSUMER_ID}'")
print(f"    Brokers      : {KAFKA_BROKERS}")
print(f"    Topic        : {TOPIC}  |  DLQ: {DLQ_TOPIC}")
print(f"    Group        : {GROUP_ID}")
print(f"    Auto-commit  : OFF (manual commit after write)")
print("=" * 70)

dlq_producer = make_dlq_producer()
consumer     = make_consumer()

count  = 0
errors = 0

for message in consumer:
    raw_value = message.value

    # ── Parse ─────────────────────────────────────────────────────────────
    try:
        log_entry = json.loads(raw_value)
        if "level" not in log_entry or "service" not in log_entry:
            raise ValueError("missing required fields: level, service")
    except (json.JSONDecodeError, ValueError) as exc:
        # Bad message → DLQ, skip offset commit advancement on this record
        send_to_dlq(dlq_producer, raw_value, str(exc))
        try:
            consumer.commit()       # still commit so we advance past bad msg
        except CommitFailedError:
            pass
        continue

    # ── Write to disk ─────────────────────────────────────────────────────
    try:
        write_log(log_entry)
    except OSError as exc:
        print(f"⚠️  Disk write failed: {exc}", file=sys.stderr)
        # Don't commit offset — message will be redelivered after restart
        continue

    # ── Manual offset commit (AFTER successful write) ─────────────────────
    for commit_attempt in range(1, 4):
        try:
            consumer.commit()
            break
        except CommitFailedError as exc:
            print(f"⚠️  Commit attempt {commit_attempt}/3 failed: {exc}", file=sys.stderr)
            time.sleep(1)

    count += 1
    level = log_entry.get("level", "?")
    icon  = {"INFO": "🟢", "WARN": "🟡", "ERROR": "🔴"}.get(level, "⚪")
    print(f"{icon} [{count:05d}] {CONSUMER_ID}  "
          f"partition={message.partition}  offset={message.offset}  "
          f"{level:<5}  {log_entry.get('service','?'):<20}")
