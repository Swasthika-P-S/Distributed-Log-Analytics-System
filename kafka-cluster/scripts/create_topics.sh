#!/usr/bin/env bash
# =============================================================================
# create_topics.sh  –  Create Kafka topics (Phase 0 Contract values)
#
#  Topic            : service-logs
#  Partitions       : 3
#  Replication      : 2
#  Retention        : 24 hours
#  Consumer Group   : spark-log-processor
#
# Usage:
#   bash scripts/create_topics.sh
# =============================================================================

set -e

BOOTSTRAP="10.12.75.131:9092,10.12.75.131:9093,10.12.75.131:9094"
TOPIC="service-logs"          # Phase 0 contract
PARTITIONS=3                  # Phase 0 contract — 3 Spark tasks in parallel
REPLICATION_FACTOR=2          # Phase 0 contract — survive 1 broker failure
RETENTION_MS=86400000         # 24 hours

echo "=========================================="
echo "  Kafka Topic Creation  (Phase 0 Values)"
echo "  Topic       : $TOPIC"
echo "  Partitions  : $PARTITIONS"
echo "  Replication : $REPLICATION_FACTOR"
echo "  Retention   : 24 hours"
echo "=========================================="
echo ""

# ---------------------------------------------------------
# Wait for broker-1 to be ready
# ---------------------------------------------------------
echo "[1/4] Waiting for brokers..."
MAX_RETRY=30
COUNT=0
until docker exec broker-1 \
    kafka-broker-api-versions \
    --bootstrap-server broker-1:29092 > /dev/null 2>&1; do
  COUNT=$((COUNT + 1))
  if [ $COUNT -ge $MAX_RETRY ]; then
    echo "ERROR: Brokers not ready after $MAX_RETRY attempts. Is docker-compose up?"
    exit 1
  fi
  echo "  Waiting... ($COUNT/$MAX_RETRY)"
  sleep 3
done
echo "  ✓ Brokers are reachable."
echo ""

# ---------------------------------------------------------
# Idempotent check — skip if already exists
# ---------------------------------------------------------
echo "[2/4] Checking if topic '$TOPIC' already exists..."
EXISTING=$(docker exec broker-1 \
  kafka-topics \
  --bootstrap-server broker-1:29092 \
  --list 2>/dev/null | grep -w "$TOPIC" || true)

if [ -n "$EXISTING" ]; then
  echo "  ⚠  Topic '$TOPIC' already exists. Showing current config:"
  docker exec broker-1 \
    kafka-topics \
    --bootstrap-server broker-1:29092 \
    --describe --topic "$TOPIC"
  exit 0
fi
echo "  Topic not found — creating now..."
echo ""

# ---------------------------------------------------------
# Create topic
# ---------------------------------------------------------
echo "[3/4] Creating topic '$TOPIC'..."
docker exec broker-1 \
  kafka-topics \
  --bootstrap-server broker-1:29092 \
  --create \
  --topic "$TOPIC" \
  --partitions "$PARTITIONS" \
  --replication-factor "$REPLICATION_FACTOR" \
  --config retention.ms="$RETENTION_MS" \
  --config cleanup.policy=delete

echo "  ✓ Topic '$TOPIC' created."
echo ""

# ---------------------------------------------------------
# Describe to confirm
# ---------------------------------------------------------
echo "[4/4] Verifying topic..."
docker exec broker-1 \
  kafka-topics \
  --bootstrap-server broker-1:29092 \
  --describe --topic "$TOPIC"

echo ""
echo "=========================================="
echo "  ✅ DONE"
echo ""
echo "  Integration points:"
echo "    Person 1 (Producer):"
echo "      bootstrap.servers = $BOOTSTRAP"
echo "      topic             = $TOPIC"
echo "      key               = service name  (hash partitioning)"
echo ""
echo "    Person 3 (Spark):"
echo "      bootstrap.servers = $BOOTSTRAP"
echo "      subscribe         = $TOPIC"
echo "      group.id          = spark-log-processor"
echo "=========================================="
