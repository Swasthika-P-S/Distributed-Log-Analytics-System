#!/bin/bash
#
# Full System Test (Person 5)
# End-to-end validation of all components
#

echo "=========================================="
echo "FULL SYSTEM INTEGRATION TEST"
echo "Person 5: Fault Tolerance & Monitoring"
echo "=========================================="
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

test_passed=0
test_failed=0

# Function to run test
run_test() {
    local test_name=$1
    local test_script=$2
    
    echo -e "${YELLOW}Running: $test_name${NC}"
    echo "----------------------------------------"
    
    bash $test_script
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ $test_name PASSED${NC}"
        ((test_passed++))
    else
        echo -e "${RED}❌ $test_name FAILED${NC}"
        ((test_failed++))
    fi
    
    echo ""
    sleep 5
}

echo "Phase 1: Pre-flight checks"
echo "----------------------------------------"

echo "Checking Docker containers..."
docker ps --format "table {{.Names}}\t{{.Status}}"
echo ""

echo "Checking Kafka brokers..."
docker exec broker-1 kafka-topics --bootstrap-server broker-1:29092 --list
echo ""

echo "Checking HDFS..."
docker exec namenode hdfs dfsadmin -report | head -20
echo ""

echo "Checking Spark..."
docker logs --tail 10 spark-processor 2>/dev/null || echo "Spark not running"
echo ""

echo "Press Enter to start tests..."
read

# Run individual tests
run_test "Test 1: Kafka Broker Failover" "./test_kafka_failover.sh"
run_test "Test 2: HDFS Replication" "./test_hdfs_replication.sh"
run_test "Test 3: Spark Recovery" "./test_spark_recovery.sh"

# Summary
echo "=========================================="
echo "TEST SUMMARY"
echo "=========================================="
echo -e "${GREEN}Passed: $test_passed${NC}"
echo -e "${RED}Failed: $test_failed${NC}"
echo ""

if [ $test_failed -eq 0 ]; then
    echo -e "${GREEN}🎉 ALL TESTS PASSED!${NC}"
    echo "System is resilient and fault-tolerant!"
else
    echo -e "${YELLOW}⚠️  Some tests failed. Review logs above.${NC}"
fi

echo "=========================================="
