#!/bin/bash
#
# Test HDFS Replication (Person 5)
# Verify HDFS maintains replication on DataNode failure
#

echo "=========================================="
echo "TEST: HDFS DataNode Failure & Replication"
echo "=========================================="
echo ""

DATANODE="datanode1"

echo "Step 1: Check initial HDFS cluster status"
docker exec namenode hdfs dfsadmin -report
echo ""

echo "Step 2: Check current DataNode status"
docker ps --filter "name=datanode" --format "table {{.Names}}\t{{.Status}}"
echo ""

echo "Step 3: Kill $DATANODE"
docker kill $DATANODE
sleep 10
echo ""

echo "Step 4: Check HDFS status after killing $DATANODE"
docker exec namenode hdfs dfsadmin -report
echo ""

echo "Step 5: Check for under-replicated blocks"
docker exec namenode hdfs fsck / -blocks -locations
echo ""

echo "Step 6: Restart $DATANODE"
docker start $DATANODE
sleep 15
echo ""

echo "Step 7: Verify $DATANODE rejoined cluster"
docker ps --filter "name=datanode" --format "table {{.Names}}\t{{.Status}}"
echo ""

echo "Step 8: Final HDFS cluster status"
docker exec namenode hdfs dfsadmin -report
echo ""

echo "Step 9: Verify replication health"
docker exec namenode hdfs fsck / -blocks -locations
echo ""

echo "=========================================="
echo "TEST COMPLETE"
echo "Expected: HDFS maintained data availability"
echo "=========================================="
