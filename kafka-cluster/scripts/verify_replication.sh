#!/usr/bin/env bash
# =============================================================================
# verify_replication.sh  –  Confirm all partition replicas are in-sync (ISR)
#
# Usage:
#   ./verify_replication.sh
#
# Exit codes:
#   0  – Cluster is healthy (no under-replicated partitions)
#   1  – Under-replicated partitions detected or connection failure
# =============================================================================

set -e

TOPIC="service-logs"      # Phase 0 contract
BOOTSTRAP="localhost:9092,localhost:9093,localhost:9094"

echo "=========================================="
echo "  Kafka Replication Verification"
echo "  Topic: $TOPIC"
echo "=========================================="
echo ""

# ---------------------------------------------------------
# Helper: run a command inside broker-1 container
# ---------------------------------------------------------
_kafka() {
  docker exec broker-1 "$@"
}

# ---------------------------------------------------------
# 1. Check that all 3 brokers are reachable
# ---------------------------------------------------------
echo "[1/4] Checking broker connectivity..."
BROKERS_OK=0
for BROKER_CONTAINER in broker-1 broker-2 broker-3; do
  # Internal port mapping
  case "$BROKER_CONTAINER" in
    broker-1) INTERNAL="broker-1:29092" ;;
    broker-2) INTERNAL="broker-2:29093" ;;
    broker-3) INTERNAL="broker-3:29094" ;;
  esac
  if docker exec broker-1 kafka-broker-api-versions \
      --bootstrap-server "$INTERNAL" > /dev/null 2>&1; then
    echo "  ✓ $BROKER_CONTAINER is UP"
    BROKERS_OK=$((BROKERS_OK + 1))
  else
    echo "  ✗ $BROKER_CONTAINER is DOWN or unreachable"
  fi
done

echo ""
echo "  Brokers reachable: $BROKERS_OK / 3"
echo ""

# ---------------------------------------------------------
# 2. Describe the topic
# ---------------------------------------------------------
echo "[2/4] Topic description for '$TOPIC':"
_kafka kafka-topics \
  --bootstrap-server broker-1:29092 \
  --describe \
  --topic "$TOPIC"
echo ""

# ---------------------------------------------------------
# 3. Check for under-replicated partitions
# ---------------------------------------------------------
echo "[3/4] Checking under-replicated partitions..."
UNDER_REP=$(_kafka kafka-topics \
  --bootstrap-server broker-1:29092 \
  --describe \
  --under-replicated-partitions 2>/dev/null || true)

if [ -z "$UNDER_REP" ]; then
  echo "  ✓ No under-replicated partitions. Replication is healthy!"
else
  echo "  ✗ UNDER-REPLICATED PARTITIONS FOUND:"
  echo "$UNDER_REP"
  EXIT_CODE=1
fi
echo ""

# ---------------------------------------------------------
# 4. Check for unavailable partitions (no elected leader)
# ---------------------------------------------------------
echo "[4/4] Checking for unavailable partitions..."
UNAVAILABLE=$(_kafka kafka-topics \
  --bootstrap-server broker-1:29092 \
  --describe \
  --unavailable-partitions 2>/dev/null || true)

if [ -z "$UNAVAILABLE" ]; then
  echo "  ✓ All partitions have elected leaders — cluster is available!"
else
  echo "  ✗ PARTITIONS WITH NO LEADER:"
  echo "$UNAVAILABLE"
  EXIT_CODE=1
fi
echo ""

# ---------------------------------------------------------
# Final result
# ---------------------------------------------------------
echo "=========================================="
if [ "${EXIT_CODE:-0}" -eq 0 ]; then
  echo "  ✅  REPLICATION HEALTH: PASS"
  echo "  All $BROKERS_OK brokers are up."
  echo "  All partitions fully replicated (ISR = replicas)."
else
  echo "  ❌  REPLICATION HEALTH: FAIL"
  echo "  See details above."
fi
echo "=========================================="
exit "${EXIT_CODE:-0}"
