#!/usr/bin/env python3
"""
show_leaders.py – Live Kafka partition leader election display.

Shows the current leader broker, replica list, and ISR (in-sync replicas)
for every partition of the 'service-logs' topic. Polls every 5 seconds.

Usage:
    python scripts/show_leaders.py
    python scripts/show_leaders.py --once      # print once and exit
    python scripts/show_leaders.py --interval 10  # custom poll interval (sec)
"""

import argparse
import sys
import time
from datetime import datetime

# ── dependency check ──────────────────────────────────────────────────────────
try:
    from kafka import KafkaAdminClient
    from kafka.errors import NoBrokersAvailable, KafkaError
except ImportError:
    print("ERROR: kafka-python not installed.")
    print("Install with:  pip install kafka-python==2.0.2")
    sys.exit(1)

# ── CONSTANTS ─────────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094"
TOPIC             = "service-logs"          # Phase 0 contract
DEFAULT_INTERVAL  = 5   # seconds between polls

# ── HELPERS ───────────────────────────────────────────────────────────────────
COLORS = {
    "RESET":  "\033[0m",
    "BOLD":   "\033[1m",
    "CYAN":   "\033[96m",
    "GREEN":  "\033[92m",
    "YELLOW": "\033[93m",
    "RED":    "\033[91m",
    "GREY":   "\033[90m",
}

def c(color: str, text: str) -> str:
    """Wrap text in ANSI color (no-op on Windows without ANSI support)."""
    return f"{COLORS.get(color, '')}{text}{COLORS['RESET']}"

def broker_label(broker_id: int) -> str:
    """Map broker ID to the container name used in docker-compose."""
    return f"broker-{broker_id}"

def isr_status(replicas: list, isr: list) -> str:
    """Return colored ISR string: green when fully in-sync, red when degraded."""
    isr_str = str(isr)
    if set(isr) == set(replicas):
        return c("GREEN", isr_str)
    return c("RED", f"{isr_str}  ⚠ UNDER-REPLICATED")

# ── CORE LOGIC ────────────────────────────────────────────────────────────────
def fetch_leader_info(admin_client: KafkaAdminClient) -> list[dict]:
    """
    Fetch partition metadata for TOPIC and return a list of dicts:
    {partition, leader, replicas, isr}
    """
    metadata = admin_client.describe_topics([TOPIC])
    if not metadata:
        return []

    topic_meta = metadata[0]
    if topic_meta.get("error_code", 0) != 0:
        raise KafkaError(f"Topic metadata error: code {topic_meta['error_code']}")

    results = []
    for part in sorted(topic_meta["partitions"], key=lambda p: p["partition"]):
        results.append({
            "partition": part["partition"],
            "leader":    part["leader"],
            "replicas":  [r for r in part["replicas"]],
            "isr":       [r for r in part["isr"]],
        })
    return results


def display(partitions: list[dict], poll_num: int) -> None:
    """Render a formatted leader table to stdout."""
    ts  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sep = "─" * 70

    print(f"\n{c('CYAN', sep)}")
    print(
        f"  {c('BOLD', 'Topic:')} {c('CYAN', TOPIC)}"
        f"   {c('GREY', f'│ Poll #{poll_num} │ {ts}')}"
    )
    print(f"{c('CYAN', sep)}")

    for p in partitions:
        leader_name  = broker_label(p["leader"])
        replicas_str = [broker_label(r) for r in p["replicas"]]
        isr_str      = [broker_label(r) for r in p["isr"]]

        leader_colored = c("GREEN", leader_name)
        replicas_colored = c("YELLOW", str(replicas_str))
        isr_colored = isr_status(p["replicas"], p["isr"])

        print(
            f"  Partition {c('BOLD', str(p['partition']))} "
            f"→ Leader: {leader_colored:<30} "
            f"| Replicas: {replicas_colored}"
        )
        print(
            f"{'':>16}ISR: {isr_colored}"
        )
    print(f"{c('CYAN', sep)}")

    # Aggregate health summary
    degraded = [p for p in partitions if set(p["isr"]) != set(p["replicas"])]
    if degraded:
        print(c("RED", f"  ❌  {len(degraded)} partition(s) are under-replicated!"))
    else:
        print(c("GREEN", "  ✅  All partitions fully replicated (ISR = Replicas)"))
    print()


# ── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Live Kafka leader election viewer")
    parser.add_argument("--once",     action="store_true", help="Print once and exit")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL,
                        help=f"Poll interval in seconds (default: {DEFAULT_INTERVAL})")
    args = parser.parse_args()

    print(c("BOLD", "\n📡 Kafka Leader Election Monitor"))
    print(f"   Bootstrap : {BOOTSTRAP_SERVERS}")
    print(f"   Topic     : {TOPIC}")
    print(f"   Interval  : {args.interval}s  (Ctrl-C to stop)\n")

    admin = None
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            client_id="leader-monitor",
            request_timeout_ms=10000,
        )
    except NoBrokersAvailable:
        print(c("RED", "ERROR: No brokers available. Is the cluster running?"))
        print("  Start with:  docker-compose up -d")
        sys.exit(1)

    poll_num = 0
    try:
        while True:
            poll_num += 1
            try:
                partitions = fetch_leader_info(admin)
                if partitions:
                    display(partitions, poll_num)
                else:
                    print(c("YELLOW", f"[{datetime.now():%H:%M:%S}] "
                                      f"Topic '{TOPIC}' not found. "
                                      f"Run create_topics.sh first."))
            except KafkaError as exc:
                print(c("RED", f"[{datetime.now():%H:%M:%S}] KafkaError: {exc}"))

            if args.once:
                break
            time.sleep(args.interval)

    except KeyboardInterrupt:
        print(c("GREY", "\nMonitor stopped by user."))
    finally:
        if admin:
            admin.close()


if __name__ == "__main__":
    main()
