#!/bin/bash
#
# Test Kafka Failover (Person 5)
# Verify system handles Kafka broker failure
#

echo "=========================================="
echo "TEST: Kafka Broker Failover"
echo "=========================================="
echo ""

BROKER="broker-2"

echo "Step 1: Check initial broker status"
docker ps --filter "name=broker" --format "table {{.Names}}\t{{.Status}}"
echo ""

echo "Step 2: Describe topic before failure"
docker exec broker-1 kafka-topics \
  --bootstrap-server broker-1:29092 \
  --describe \
  --topic service-logs
echo ""

echo "Step 3: Kill $BROKER"
docker kill $BROKER
sleep 5
echo ""

echo "Step 4: Check broker status after killing $BROKER"
docker ps --filter "name=broker" --format "table {{.Names}}\t{{.Status}}"
echo ""

echo "Step 5: Verify leader election (partitions should have new leaders)"
docker exec broker-1 kafka-topics \
  --bootstrap-server broker-1:29092 \
  --describe \
  --topic service-logs
echo ""

echo "Step 6: Test if topic is still accessible"
docker exec broker-1 kafka-topics \
  --bootstrap-server broker-1:29092 \
  --list | grep service-logs
if [ $? -eq 0 ]; then
  echo "✅ Topic is still accessible"
else
  echo "❌ Topic is NOT accessible"
fi
echo ""

echo "Step 7: Restart $BROKER"
docker start $BROKER
sleep 10
echo ""

echo "Step 8: Verify $BROKER rejoined cluster"
docker ps --filter "name=broker" --format "table {{.Names}}\t{{.Status}}"
echo ""

echo "Step 9: Final topic status"
docker exec broker-1 kafka-topics \
  --bootstrap-server broker-1:29092 \
  --describe \
  --topic service-logs
echo ""

echo "=========================================="
echo "TEST COMPLETE"
echo "Expected: System continued working during broker failure"
echo "=========================================="
