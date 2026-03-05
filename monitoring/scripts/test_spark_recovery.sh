#!/bin/bash
#
# Test Spark Recovery (Person 5)
# Verify Spark recovers from checkpoint after failure
#

echo "=========================================="
echo "TEST: Spark Process Recovery"
echo "=========================================="
echo ""

CONTAINER="spark-processor"

echo "Step 1: Check initial Spark status"
docker ps --filter "name=$CONTAINER" --format "table {{.Names}}\t{{.Status}}"
echo ""

echo "Step 2: Check checkpoint location"
docker exec namenode hdfs dfs -ls -R /spark-checkpoints 2>/dev/null || echo "No checkpoints yet"
echo ""

echo "Step 3: Kill Spark container"
docker kill $CONTAINER
sleep 5
echo ""

echo "Step 4: Verify Spark is down"
docker ps --filter "name=$CONTAINER" --format "table {{.Names}}\t{{.Status}}"
echo ""

echo "Step 5: Restart Spark container"
docker start $CONTAINER
sleep 20
echo ""

echo "Step 6: Verify Spark is running"
docker ps --filter "name=$CONTAINER" --format "table {{.Names}}\t{{.Status}}"
echo ""

echo "Step 7: Check Spark logs for recovery"
docker logs --tail 50 $CONTAINER
echo ""

echo "Step 8: Verify checkpoint still exists"
docker exec namenode hdfs dfs -ls -R /spark-checkpoints 2>/dev/null || echo "No checkpoints found"
echo ""

echo "=========================================="
echo "TEST COMPLETE"
echo "Expected: Spark restarted and recovered from checkpoint"
echo "=========================================="
