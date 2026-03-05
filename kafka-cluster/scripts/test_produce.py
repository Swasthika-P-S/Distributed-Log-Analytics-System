#!/usr/bin/env python3
"""
test_produce.py – Send Phase 0-compliant log messages to Kafka.

JSON Schema (Phase 0 contract):
{
    "timestamp": "ISO-8601 string",
    "service":   "string (web_server, database, etc.)",
    "level":     "string (INFO, ERROR, WARN, DEBUG)",
    "message":   "string",
    "metadata": {
        "ip":          "string",
        "latency_ms":  int,
        "status_code": int
    }
}

Topic    : service-logs
Key      : service name  (hash partitioning — all logs from same service → same partition)
Producer : Person 2 integration test (Person 1 uses their own producer)

Usage:
    pip install kafka-python==2.0.2
    python scripts/test_produce.py
    python scripts/test_produce.py --strategy round_robin
    python scripts/test_produce.py --count 30
"""

import argparse
import json
import random
import sys
import time
from datetime import datetime, timezone

try:
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable
except ImportError:
    print("ERROR: kafka-python not installed.  Run: pip install kafka-python==2.0.2")
    sys.exit(1)

# ── Phase 0 Constants ─────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = "10.12.75.131:9092,10.12.75.131:9093,10.12.75.131:9094"
TOPIC             = "service-logs"          # Phase 0 contract

# Sample services (matches what Person 1 will produce)
SERVICES = ["web_server", "database", "auth_service",
            "payment_service", "api_gateway", "notification_svc"]
LOG_LEVELS = ["INFO", "ERROR", "WARN", "DEBUG"]

SAMPLE_IPS = [f"192.168.1.{i}" for i in range(10, 50)]

# ── Partitioner (Phase 0: key = service name) ─────────────────────────────────
def hash_partitioner(key, all_partitions, available_partitions):
    """Deterministic hash: same service always lands on same partition."""
    idx = hash(key) % len(all_partitions)
    return all_partitions[idx]

# ── Build a Phase 0-compliant log event ───────────────────────────────────────
def make_log_event(service: str) -> dict:
    level = random.choice(LOG_LEVELS)
    messages = {
        "INFO":  f"Request processed successfully in {random.randint(5, 200)}ms",
        "WARN":  f"High latency detected: {random.randint(300, 800)}ms response time",
        "ERROR": f"Connection timeout after {random.randint(3, 5)} retries",
        "DEBUG": f"Cache {'hit' if random.random() > 0.4 else 'miss'} for key=user_{random.randint(1, 9999)}",
    }
    status_map = {"INFO": 200, "WARN": 429, "ERROR": 503, "DEBUG": 200}
    return {
        "timestamp":  datetime.now(timezone.utc).isoformat(),
        "service":    service,
        "level":      level,
        "message":    messages[level],
        "metadata": {
            "ip":          random.choice(SAMPLE_IPS),
            "latency_ms":  random.randint(1, 1000),
            "status_code": status_map[level],
        }
    }

# ── Callbacks ─────────────────────────────────────────────────────────────────
def on_success(meta):
    print(f"    → partition={meta.partition}  offset={meta.offset}")

def on_error(exc):
    print(f"    ✗ SEND ERROR: {exc}", file=sys.stderr)

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Phase 0 Kafka producer test")
    parser.add_argument("--strategy", choices=["hash", "round_robin"],
                        default="hash", help="Partitioning strategy (default: hash)")
    parser.add_argument("--count", type=int, default=20,
                        help="Number of messages to send (default: 20)")
    args = parser.parse_args()

    print("\n" + "="*60)
    print("  🚀 Kafka Producer Test  (Phase 0 Schema)")
    print("="*60)
    print(f"  Topic     : {TOPIC}")
    print(f"  Strategy  : {args.strategy}  (Phase 0: hash by service name)")
    print(f"  Messages  : {args.count}")
    print("="*60 + "\n")

    try:
        if args.strategy == "hash":
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                partitioner=hash_partitioner,
                acks="all",
                retries=3,
            )
        else:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
    except NoBrokersAvailable:
        print("ERROR: No brokers available. Run:  docker-compose up -d")
        sys.exit(1)

    for i in range(args.count):
        service = random.choice(SERVICES)
        event   = make_log_event(service)
        key     = service if args.strategy == "hash" else None

        print(f"  [{i+1:>3}/{args.count}] service={service:<20} "
              f"level={event['level']:<5}  ip={event['metadata']['ip']}")
        producer.send(TOPIC, key=key, value=event) \
                .add_callback(on_success)           \
                .add_errback(on_error)
        time.sleep(0.05)

    producer.flush()
    producer.close()

    print(f"\n{'='*60}")
    print(f"  ✅  {args.count} messages sent to '{TOPIC}'")
    print(f"  Next step: python scripts/test_consume.py")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
