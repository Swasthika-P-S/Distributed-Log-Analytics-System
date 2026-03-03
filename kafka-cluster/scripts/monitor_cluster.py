#!/usr/bin/env python3
"""
monitor_cluster.py – Real-time Kafka cluster health dashboard.

Displays:
  • Broker status (up/down)
  • Messages-in rate per partition (approx per second)
  • Consumer group lag for 'spark-log-processor'
  • Under-replicated partition count

Usage:
    pip install kafka-python==2.0.2
    python scripts/monitor_cluster.py
    python scripts/monitor_cluster.py --interval 5
"""

import argparse
import sys
import time
from collections import defaultdict
from datetime import datetime

try:
    from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
    from kafka.errors import NoBrokersAvailable, KafkaError
except ImportError:
    print("ERROR: kafka-python is not installed.")
    print("Run:  pip install kafka-python==2.0.2")
    sys.exit(1)

# ── CONFIG ────────────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS  = "localhost:9092,localhost:9093,localhost:9094"
TOPIC              = "service-logs"          # Phase 0 contract
CONSUMER_GROUP     = "spark-log-processor"
BROKER_IDS         = [1, 2, 3]
BROKER_INTERNAL    = {
    1: "localhost:9092",
    2: "localhost:9093",
    3: "localhost:9094",
}
DEFAULT_INTERVAL   = 5

# ── COLORS ────────────────────────────────────────────────────────────────────
R = "\033[0m"
BOLD  = "\033[1m"
CYAN  = "\033[96m"
GREEN = "\033[92m"
YELLOW= "\033[93m"
RED   = "\033[91m"
GREY  = "\033[90m"

def c(color, text):
    return f"{color}{text}{R}"

# ── BROKER STATUS ─────────────────────────────────────────────────────────────
def check_brokers() -> dict[int, bool]:
    """Ping each broker individually using a short-lived AdminClient."""
    status = {}
    for broker_id, addr in BROKER_INTERNAL.items():
        try:
            tmp = KafkaAdminClient(
                bootstrap_servers=addr,
                client_id=f"health-check-b{broker_id}",
                request_timeout_ms=4000,
            )
            tmp.list_topics()
            tmp.close()
            status[broker_id] = True
        except Exception:
            status[broker_id] = False
    return status

# ── PARTITION OFFSETS ─────────────────────────────────────────────────────────
def get_end_offsets(consumer: KafkaConsumer, partitions: int) -> dict[int, int]:
    """Return the latest (end) offset for each partition."""
    tps = [TopicPartition(TOPIC, p) for p in range(partitions)]
    end_offsets = consumer.end_offsets(tps)
    return {tp.partition: offset for tp, offset in end_offsets.items()}

def get_committed_offsets(consumer: KafkaConsumer,
                          partitions: int) -> dict[int, int]:
    """Return committed offsets for the CONSUMER_GROUP on each partition."""
    committed = {}
    for p in range(partitions):
        tp  = TopicPartition(TOPIC, p)
        off = consumer.committed(tp)
        committed[p] = off if off is not None else 0
    return committed

# ── TOPIC METADATA ────────────────────────────────────────────────────────────
def get_partition_count(admin: KafkaAdminClient) -> int:
    meta = admin.describe_topics([TOPIC])
    if not meta or meta[0].get("error_code", 0) != 0:
        return 0
    return len(meta[0]["partitions"])

def get_under_replicated(admin: KafkaAdminClient) -> list[int]:
    meta = admin.describe_topics([TOPIC])
    if not meta or meta[0].get("error_code", 0) != 0:
        return []
    under = []
    for part in meta[0]["partitions"]:
        if set(part["isr"]) != set(part["replicas"]):
            under.append(part["partition"])
    return under

# ── DISPLAY ───────────────────────────────────────────────────────────────────
def clear_lines(n: int) -> None:
    """Move cursor up n lines and clear them (in-place refresh)."""
    for _ in range(n):
        sys.stdout.write("\033[F\033[K")

def print_dashboard(
    poll:           int,
    broker_status:  dict[int, bool],
    end_offsets:    dict[int, int],
    prev_offsets:   dict[int, int],
    committed:      dict[int, int],
    under_rep:      list[int],
    interval:       int,
) -> int:
    """Print the dashboard and return the number of lines printed."""
    ts   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sep  = "─" * 68
    lines = 0

    def pr(text=""):
        nonlocal lines
        print(text)
        lines += 1

    pr(f"{c(CYAN, sep)}")
    pr(f"  {c(BOLD, '📊 Kafka Cluster Health Dashboard')}   {c(GREY, ts)}  poll #{poll}")
    pr(f"{c(CYAN, sep)}")

    # ── Broker Status ──────────────────────────────────────────────────────
    pr(f"  {c(BOLD, '🖥  Broker Status')}")
    for bid, up in broker_status.items():
        state = c(GREEN, "● UP  ") if up else c(RED, "○ DOWN")
        pr(f"    broker-{bid}  (:{9091+bid})   {state}")
    pr()

    # ── Partition Metrics ──────────────────────────────────────────────────
    pr(f"  {c(BOLD, '📨 Partition Metrics')}  [{TOPIC}]")
    pr(f"  {'Part':>4}  {'End Offset':>12}  {'Msgs/s':>8}  {'Committed':>10}  {'Lag':>8}")
    pr(f"  {'----':>4}  {'----------':>12}  {'------':>8}  {'---------':>10}  {'---':>8}")

    total_lag = 0
    for p in sorted(end_offsets.keys()):
        end  = end_offsets[p]
        prev = prev_offsets.get(p, end)
        rate = max(0, (end - prev) / interval)
        com  = committed.get(p, 0)
        lag  = max(0, end - com)
        total_lag += lag

        lag_str = c(RED, f"{lag:>8}") if lag > 1000 else c(GREEN, f"{lag:>8}")
        rate_str = f"{rate:>8.1f}"
        ur_mark  = c(YELLOW, " ⚠") if p in under_rep else ""
        pr(
            f"  {p:>4}  {end:>12}  {rate_str}  {com:>10}  {lag_str}{ur_mark}"
        )
    pr()

    # ── Consumer Group ─────────────────────────────────────────────────────
    lag_color = RED if total_lag > 5000 else (YELLOW if total_lag > 100 else GREEN)
    pr(f"  {c(BOLD, '🔁 Consumer Group:')} {CONSUMER_GROUP}")
    pr(f"    Total lag: {c(lag_color, str(total_lag))} messages")
    pr()

    # ── Under-replicated ───────────────────────────────────────────────────
    pr(f"  {c(BOLD, '🔄 Replication Health')}")
    if under_rep:
        pr(f"    {c(RED, f'❌ Under-replicated partitions: {under_rep}')}")
    else:
        pr(f"    {c(GREEN, '✅ All partitions fully replicated')}")
    pr(f"{c(CYAN, sep)}")
    pr(f"  Press Ctrl-C to stop  |  refreshing every {interval}s")
    pr()

    return lines

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Kafka Cluster Health Dashboard")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL,
                        help=f"Refresh interval in seconds (default: {DEFAULT_INTERVAL})")
    args = parser.parse_args()

    print(c(BOLD, "\n📡 Connecting to Kafka cluster…"))
    print(f"   Bootstrap: {BOOTSTRAP_SERVERS}\n")

    try:
        admin = KafkaAdminClient(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            client_id="cluster-monitor-admin",
            request_timeout_ms=10000,
        )
    except NoBrokersAvailable:
        print(c(RED, "ERROR: No brokers available. Run:  docker-compose up -d"))
        sys.exit(1)

    # Consumer for offset queries (uses the monitored group id so we see lag)
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        enable_auto_commit=False,
        auto_offset_reset="latest",
        consumer_timeout_ms=5000,
    )
    # Assign all partitions manually to be able to query offsets
    partition_count = get_partition_count(admin)
    if partition_count == 0:
        print(c(YELLOW, f"Topic '{TOPIC}' not found. Run create_topics.sh first."))
        consumer.close()
        admin.close()
        sys.exit(1)

    tps = [TopicPartition(TOPIC, p) for p in range(partition_count)]
    consumer.assign(tps)

    prev_offsets = {}
    prev_lines   = 0
    poll_num     = 0

    try:
        while True:
            poll_num += 1

            broker_status = check_brokers()
            end_offsets   = get_end_offsets(consumer, partition_count)
            committed     = get_committed_offsets(consumer, partition_count)
            under_rep     = get_under_replicated(admin)

            if prev_lines > 0:
                clear_lines(prev_lines)

            prev_lines = print_dashboard(
                poll=poll_num,
                broker_status=broker_status,
                end_offsets=end_offsets,
                prev_offsets=prev_offsets,
                committed=committed,
                under_rep=under_rep,
                interval=args.interval,
            )
            prev_offsets = dict(end_offsets)
            time.sleep(args.interval)

    except KeyboardInterrupt:
        print(c(GREY, "\nDashboard stopped."))
    finally:
        consumer.close()
        admin.close()


if __name__ == "__main__":
    main()
