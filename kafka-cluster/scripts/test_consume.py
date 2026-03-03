#!/usr/bin/env python3
"""
test_consume.py – Verify Person 3 (Spark) can connect to Kafka.

Reads from topic 'service-logs' using consumer group 'spark-log-processor'
and validates that messages follow the Phase 0 JSON schema.

Phase 0 schema expected:
  { timestamp, service, level, message, metadata: {ip, latency_ms, status_code} }

Usage:
    pip install kafka-python==2.0.2
    python scripts/test_consume.py           # read up to 50 messages
    python scripts/test_consume.py --max 100
    python scripts/test_consume.py --tail    # keep reading (Ctrl-C to stop)
"""

import argparse
import json
import sys
import time
from datetime import datetime

try:
    from kafka import KafkaConsumer, TopicPartition
    from kafka.errors import NoBrokersAvailable
except ImportError:
    print("ERROR: kafka-python not installed.  Run: pip install kafka-python==2.0.2")
    sys.exit(1)

# ── Phase 0 Constants ─────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094"
TOPIC             = "service-logs"           # Phase 0 contract
CONSUMER_GROUP    = "spark-log-processor"    # Phase 0 contract
REQUIRED_FIELDS   = {"timestamp", "service", "level", "message", "metadata"}
REQUIRED_META     = {"ip", "latency_ms", "status_code"}

# ── Colors ────────────────────────────────────────────────────────────────────
LEVEL_COLORS = {"INFO": "\033[92m", "WARN": "\033[93m",
                "ERROR": "\033[91m", "DEBUG": "\033[90m"}
R = "\033[0m"
def lc(level):
    return f"{LEVEL_COLORS.get(level, '')}{level:<5}{R}"

# ── Schema validator ─────────────────────────────────────────────────────────
def validate_schema(msg: dict) -> list[str]:
    """Return list of violations, empty = valid."""
    errors = []
    missing = REQUIRED_FIELDS - set(msg.keys())
    if missing:
        errors.append(f"missing top-level fields: {missing}")
    meta = msg.get("metadata", {})
    if not isinstance(meta, dict):
        errors.append("'metadata' is not an object")
    else:
        m_missing = REQUIRED_META - set(meta.keys())
        if m_missing:
            errors.append(f"missing metadata fields: {m_missing}")
    return errors

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Kafka consumer integration test (Person 3)")
    parser.add_argument("--max",     type=int, default=50)
    parser.add_argument("--timeout", type=int, default=20, help="Idle timeout in seconds")
    parser.add_argument("--tail",    action="store_true", help="Keep reading indefinitely")
    args = parser.parse_args()

    print("\n" + "="*65)
    print("  🔍 Kafka Consumer Test  (Phase 0 Schema Validation)")
    print("="*65)
    print(f"  Topic         : {TOPIC}")
    print(f"  Consumer Group: {CONSUMER_GROUP}")
    print(f"  Max messages  : {'∞' if args.tail else args.max}")
    print("="*65 + "\n")

    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=CONSUMER_GROUP,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda b: json.loads(b.decode("utf-8")) if b else None,
            key_deserializer=lambda b: b.decode("utf-8") if b else None,
            consumer_timeout_ms=args.timeout * 1000,
        )
    except NoBrokersAvailable:
        print("ERROR: No brokers available. Run:  docker-compose up -d")
        sys.exit(1)

    time.sleep(2)
    print(f"  Assigned partitions: {[p.partition for p in consumer.assignment()]}\n")
    print(f"  {'#':>5}  {'Part':>4}  {'Service':<20} Level  {'IP':<15} Lat(ms)  Status  Schema")
    print(f"  {'─'*5}  {'─'*4}  {'─'*20} {'─'*5}  {'─'*15} {'─'*7}  {'─'*6}  {'─'*6}")

    count     = 0
    schema_ok = 0
    schema_fail = 0
    part_counts: dict[int, int] = {}

    try:
        for msg in consumer:
            count += 1
            part_counts[msg.partition] = part_counts.get(msg.partition, 0) + 1
            val = msg.value or {}
            errors = validate_schema(val)

            service = val.get("service", "?")
            level   = val.get("level", "?")
            meta    = val.get("metadata", {})
            ip      = meta.get("ip", "?") if isinstance(meta, dict) else "?"
            lat     = meta.get("latency_ms", "?") if isinstance(meta, dict) else "?"
            status  = meta.get("status_code", "?") if isinstance(meta, dict) else "?"
            schema_mark = "\033[92m✓\033[0m" if not errors else "\033[91m✗\033[0m"

            if not errors:
                schema_ok += 1
            else:
                schema_fail += 1

            print(
                f"  {count:>5}  {msg.partition:>4}  {service:<20} {lc(level)}  "
                f"{str(ip):<15} {str(lat):>7}  {str(status):>6}  {schema_mark}"
            )
            if errors:
                for e in errors:
                    print(f"          \033[91m⚠ {e}\033[0m")

            if not args.tail and count >= args.max:
                break

    except KeyboardInterrupt:
        print("\n  (stopped by user)")
    except StopIteration:
        print(f"\n  (idle timeout after {args.timeout}s — no more messages)")

    consumer.close()

    # Summary
    print(f"\n{'='*65}")
    print(f"  📋 SUMMARY")
    print(f"{'='*65}")
    print(f"  Messages read    : {count}")
    print(f"  Schema valid     : \033[92m{schema_ok}\033[0m")
    print(f"  Schema invalid   : \033[91m{schema_fail}\033[0m")
    print(f"\n  Per-partition distribution:")
    for p, cnt in sorted(part_counts.items()):
        bar = "█" * min(cnt, 40)
        print(f"    Partition {p}: {cnt:>4}  {bar}")

    if count > 0 and schema_fail == 0:
        print(f"\n  \033[92m✅ PASS — cluster ready for Person 3 (Spark)\033[0m")
        print(f"     bootstrap.servers = {BOOTSTRAP_SERVERS}")
        print(f"     subscribe         = {TOPIC}")
        print(f"     group.id          = {CONSUMER_GROUP}")
    elif count == 0:
        print(f"\n  ⚠  No messages found. Run: python scripts/test_produce.py")
    else:
        print(f"\n  \033[91m❌ Schema violations found — Person 1 must fix their producer.\033[0m")
    print(f"{'='*65}\n")

if __name__ == "__main__":
    main()
