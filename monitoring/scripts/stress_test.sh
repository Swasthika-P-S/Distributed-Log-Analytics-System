#!/bin/bash
#
# Stress Test (Person 5)
# High-load testing of the system
#

echo "=========================================="
echo "STRESS TEST: High Load Simulation"
echo "=========================================="
echo ""

DURATION=300  # 5 minutes
MESSAGE_RATE=100  # messages per second

echo "Test Configuration:"
echo "  Duration: $DURATION seconds"
echo "  Target Rate: $MESSAGE_RATE messages/second"
echo ""

echo "Step 1: Check baseline system status"
echo "----------------------------------------"
docker stats --no-stream
echo ""

echo "Step 2: Monitor consumer lag before test"
docker exec broker-1 kafka-consumer-groups \
  --bootstrap-server broker-1:29092 \
  --describe \
  --group spark-log-processor 2>/dev/null || echo "Consumer group not active yet"
echo ""

echo "Step 3: Starting stress test..."
echo "Increasing producer rate temporarily"
echo "(This would normally be done by scaling up producers)"
echo ""

# Note: This is a placeholder - actual implementation would
# modify producer rate or use a separate stress testing tool
echo "⚠️  Manual step: Increase producer message rate to $MESSAGE_RATE msg/sec"
echo "Press Enter when ready to continue..."
read

echo "Step 4: Monitoring system for $DURATION seconds..."
START_TIME=$(date +%s)
COUNTER=0

while [ $COUNTER -lt $DURATION ]; do
    ELAPSED=$(($(date +%s) - START_TIME))
    REMAINING=$((DURATION - ELAPSED))
    
    echo "Time elapsed: ${ELAPSED}s / ${DURATION}s (${REMAINING}s remaining)"
    
    # Check consumer lag
    docker exec broker-1 kafka-consumer-groups \
      --bootstrap-server broker-1:29092 \
      --describe \
      --group spark-log-processor 2>/dev/null | grep service-logs || true
    
    sleep 30
    COUNTER=$((COUNTER + 30))
done

echo ""
echo "Step 5: Post-test system status"
echo "----------------------------------------"
docker stats --no-stream
echo ""

echo "Step 6: Final consumer lag check"
docker exec broker-1 kafka-consumer-groups \
  --bootstrap-server broker-1:29092 \
  --describe \
  --group spark-log-processor 2>/dev/null || echo "Consumer group not active"
echo ""

echo "Step 7: Check HDFS storage"
docker exec namenode hdfs dfs -du -h /logs/processed 2>/dev/null || echo "No processed logs yet"
echo ""

echo "=========================================="
echo "STRESS TEST COMPLETE"
echo "=========================================="
echo ""
echo "Key Metrics to Review:"
echo "  - Consumer lag should return to baseline"
echo "  - No container crashes"
echo "  - HDFS storage working"
echo "  - CPU/Memory within acceptable limits"
echo "=========================================="
