# 🚀 Person 5: Running & Verification Guide

## Complete Step-by-Step Instructions

---

## Prerequisites Check

First, let's make sure everything is ready:

```bash
# 1. Check you're in the right directory
cd /Users/priyadarshinianand/Documents/Distributed-Log-Analytics-System/monitoring

# 2. Check all files are present
ls -la

# You should see:
# - health_monitor.py
# - alert_system.py
# - fault_injector.py
# - metrics_collector.py
# - requirements.txt
# - README.md
# - scripts/ directory
# - config/ directory
```

---

## Step 1: Install Dependencies (5 minutes)

```bash
# Make sure you're in the monitoring directory
cd /Users/priyadarshinianand/Documents/Distributed-Log-Analytics-System/monitoring

# Install Python dependencies
pip install -r requirements.txt

# Expected output:
# Successfully installed kafka-python-2.0.2 requests-2.31.0 psutil-5.9.8 ...
```

**Verify installation:**
```bash
python3 -c "import kafka; import requests; import psutil; import yaml; import tabulate; import colorama; print('✅ All dependencies installed successfully!')"
```

---

## Step 2: Start the Main System (if not running)

```bash
# Go back to project root
cd /Users/priyadarshinianand/Documents/Distributed-Log-Analytics-System

# Start all services
docker-compose up -d

# Wait for all containers to be healthy (2-3 minutes)
# You should see containers starting:
# - zookeeper
# - broker-1, broker-2, broker-3
# - producer, consumer
# - namenode, datanode1, datanode2
# - spark-processor
```

**Verify all containers are running:**
```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```

Expected output:
```
NAMES               STATUS
spark-processor     Up X minutes
datanode2           Up X minutes
datanode1           Up X minutes
namenode            Up X minutes
consumer            Up X minutes
producer            Up X minutes
broker-3            Up X minutes (healthy)
broker-2            Up X minutes (healthy)
broker-1            Up X minutes (healthy)
zookeeper           Up X minutes (healthy)
```

---

## Step 3: Run Health Monitor (Real-time Dashboard)

Open a **NEW TERMINAL** and run:

```bash
cd /Users/priyadarshinianand/Documents/Distributed-Log-Analytics-System/monitoring

python3 health_monitor.py
```

**What you should see:**

```
Loading configuration...
Starting Health Monitor Dashboard...
Press Ctrl+C to stop

================================================================================
🛡️  DISTRIBUTED LOG ANALYTICS - HEALTH MONITOR (Person 5)
================================================================================
Last Updated: 2026-03-05 14:30:45
================================================================================

🐳 DOCKER CONTAINERS STATUS
────────────────────────────────────────────────────────────────────────────────
┌─────────────────┬────────────────────────────────────────────────────────────┐
│ Container       │ Status                                                     │
├─────────────────┼────────────────────────────────────────────────────────────┤
│ broker-1        │ ✅ Up 5 minutes (healthy)                                  │
│ broker-2        │ ✅ Up 5 minutes (healthy)                                  │
│ broker-3        │ ✅ Up 5 minutes (healthy)                                  │
│ zookeeper       │ ✅ Up 5 minutes (healthy)                                  │
│ spark-processor │ ✅ Up 5 minutes                                            │
│ namenode        │ ✅ Up 5 minutes                                            │
│ datanode1       │ ✅ Up 5 minutes                                            │
│ datanode2       │ ✅ Up 5 minutes                                            │
│ producer        │ ✅ Up 5 minutes                                            │
│ consumer        │ ✅ Up 5 minutes                                            │
└─────────────────┴────────────────────────────────────────────────────────────┘

📡 KAFKA CLUSTER STATUS
────────────────────────────────────────────────────────────────────────────────
┌────────────────────┬───────────────┐
│ Broker             │ Status        │
├────────────────────┼───────────────┤
│ localhost:9092     │ ✅ HEALTHY    │
│ localhost:9093     │ ✅ HEALTHY    │
│ localhost:9094     │ ✅ HEALTHY    │
└────────────────────┴───────────────┘

✅ Topic 'service-logs' exists

📊 CONSUMER LAG STATUS
────────────────────────────────────────────────────────────────────────────────
┌──────────────┬───────────┬────────────┬──────────┐
│ Partition    │ Committed │ End Offset │ Lag      │
├──────────────┼───────────┼────────────┼──────────┤
│ Partition 0  │ 1234      │ 1250       │ 16       │
│ Partition 1  │ 1189      │ 1200       │ 11       │
│ Partition 2  │ 1456      │ 1470       │ 14       │
└──────────────┴───────────┴────────────┴──────────┘

✅ Total Lag: 41 messages (HEALTHY)

💾 HDFS CLUSTER STATUS
────────────────────────────────────────────────────────────────────────────────
┌──────────────────────┬────────────────────────┐
│ Metric               │ Value                  │
├──────────────────────┼────────────────────────┤
│ NameNode             │ ✅ ACTIVE              │
│ Live DataNodes       │ 2                      │
│ Dead DataNodes       │ 0                      │
│ Total Blocks         │ 45                     │
│ Missing Blocks       │ 0                      │
│ Under-Replicated     │ 0                      │
│ Capacity Used        │ 2.34%                  │
└──────────────────────┴────────────────────────┘

✅ HDFS cluster is HEALTHY

⚡ SPARK PROCESSOR STATUS
────────────────────────────────────────────────────────────────────────────────
✅ Spark Container: RUNNING
   Status: Up 5 minutes

================================================================================
Refreshing in 30 seconds... (Press Ctrl+C to stop)
================================================================================
```

**This dashboard updates every 30 seconds automatically!**

✅ **Success indicator:** All components show ✅ green checkmarks

---

## Step 4: Run Alert System (In Parallel)

Open **ANOTHER NEW TERMINAL** and run:

```bash
cd /Users/priyadarshinianand/Documents/Distributed-Log-Analytics-System/monitoring

python3 alert_system.py
```

**What you should see:**

```
2026-03-05 14:32:10 | INFO     | 🛡️  Alert System Started (Person 5)
2026-03-05 14:32:10 | INFO     | Monitoring interval: 30 seconds
2026-03-05 14:32:10 | INFO     | Press Ctrl+C to stop

2026-03-05 14:32:15 | INFO     | --- Health Check: 2026-03-05 14:32:15 ---
2026-03-05 14:32:15 | INFO     | ✅ No active alerts - System healthy

2026-03-05 14:32:45 | INFO     | --- Health Check: 2026-03-05 14:32:45 ---
2026-03-05 14:32:45 | INFO     | ✅ No active alerts - System healthy
```

✅ **Success indicator:** "No active alerts - System healthy"

⚠️ **If you see alerts:** This is actually working! The system is detecting issues.

---

## Step 5: Verify Monitoring is Working

### Test 1: Quick Manual Check

In a **NEW TERMINAL**, run these commands to verify monitoring can see everything:

```bash
# Check Kafka topics (Person 2's work)
docker exec broker-1 kafka-topics --bootstrap-server broker-1:29092 --list

# Should show: service-logs

# Check consumer lag (Person 3's work)
docker exec broker-1 kafka-consumer-groups \
  --bootstrap-server broker-1:29092 \
  --describe \
  --group spark-log-processor

# Should show partition lag

# Check HDFS (Person 4's work)
docker exec namenode hdfs dfsadmin -report | head -30

# Should show 2 live DataNodes

# Check Spark logs (Person 3's work)
docker logs spark-processor --tail 20

# Should show processing logs
```

### Test 2: Verify Alerts Log

```bash
cd /Users/priyadarshinianand/Documents/Distributed-Log-Analytics-System/monitoring

# Check if alerts are being logged
cat alerts.log

# You should see health check entries
```

---

## Step 6: Run First Fault Injection Test

Now let's test the fault tolerance! Keep health_monitor.py and alert_system.py running.

In a **NEW TERMINAL**:

```bash
cd /Users/priyadarshinianand/Documents/Distributed-Log-Analytics-System/monitoring

python3 fault_injector.py --test kafka-broker
```

**What you should see:**

```
================================================================================
TEST: Kafka Broker Failover
================================================================================

PHASE 1: Pre-test verification
[2026-03-05 14:35:12] INFO: Checking broker-2 status...
[2026-03-05 14:35:12] SUCCESS: ✅ broker-2 is running

PHASE 2: Killing broker-2
[2026-03-05 14:35:13] TEST: Killing container: broker-2
[2026-03-05 14:35:14] SUCCESS: ✅ Container broker-2 killed

PHASE 3: Verifying leader election
[2026-03-05 14:35:19] INFO: Checking Kafka leader election...
Topic: service-logs     Partition: 0    Leader: 1       Replicas: 1,2   Isr: 1
Topic: service-logs     Partition: 1    Leader: 3       Replicas: 3,2   Isr: 3
Topic: service-logs     Partition: 2    Leader: 1       Replicas: 1,3   Isr: 1,3
[2026-03-05 14:35:19] SUCCESS: ✅ Leader election information available

PHASE 4: Checking remaining brokers
[2026-03-05 14:35:20] INFO: Checking Kafka topic health...
[2026-03-05 14:35:21] SUCCESS: ✅ Kafka topic 'service-logs' is accessible

PHASE 5: Restarting broker-2
[2026-03-05 14:35:22] TEST: Starting container: broker-2
[2026-03-05 14:35:23] SUCCESS: ✅ Container broker-2 started

PHASE 6: Verifying recovery
[2026-03-05 14:35:33] INFO: Checking Kafka topic health...
[2026-03-05 14:35:33] SUCCESS: ✅ Kafka topic 'service-logs' is accessible

================================================================================
TEST RESULTS:
================================================================================
[2026-03-05 14:35:34] INFO: Initial topic health: ✅ PASS
[2026-03-05 14:35:34] INFO: Topic accessible after broker failure: ✅ PASS
[2026-03-05 14:35:34] INFO: Broker restarted: ✅ PASS
[2026-03-05 14:35:34] INFO: Final topic health: ✅ PASS

🎉 TEST 1 PASSED: System survived Kafka broker failure!
```

**During this test, watch your health_monitor terminal** - you should see broker-2 go down and come back up!

✅ **Success indicator:** All phases PASS, test completes successfully

---

## Step 7: Verify Everything is Working

### Checklist:

- [ ] Health monitor shows all components healthy
- [ ] Alert system shows "No active alerts"
- [ ] Fault injection test passed
- [ ] Consumer lag is low (< 500 messages)
- [ ] All Docker containers running
- [ ] No errors in any terminal

### Quick Verification Commands:

```bash
# 1. All containers up?
docker ps | wc -l
# Should show 11 lines (10 containers + header)

# 2. Kafka working?
docker exec broker-1 kafka-topics --bootstrap-server broker-1:29092 --list | grep service-logs
# Should show: service-logs

# 3. HDFS working?
docker exec namenode hdfs dfs -ls /
# Should show directories

# 4. Spark working?
docker logs spark-processor 2>&1 | tail -5 | grep -i error
# Should show NO critical errors

# 5. Logs flowing?
docker logs producer 2>&1 | tail -5
# Should show recent log generation
```

---

## Step 8: Run More Tests (Optional)

If everything above works, try more tests:

```bash
# Test HDFS resilience
python3 fault_injector.py --test hdfs-datanode

# Test Spark recovery
python3 fault_injector.py --test spark

# Test everything
python3 fault_injector.py --test full
```

---

## Troubleshooting

### Problem: "Import Error: No module named kafka"

**Solution:**
```bash
pip3 install kafka-python==2.0.2
```

### Problem: "Can't connect to Kafka"

**Solution:**
```bash
# Check if Kafka is running
docker ps --filter name=broker

# Restart Kafka if needed
docker-compose restart broker-1 broker-2 broker-3
```

### Problem: "HDFS web UI not accessible"

**Solution:**
```bash
# Check if NameNode is running
docker ps --filter name=namenode

# Try accessing: http://localhost:9870
```

### Problem: Health monitor shows red ❌

**This is normal if:**
- System just started (wait 2-3 minutes)
- Running a fault injection test
- A component actually failed (check which one and restart it)

**Solution:**
```bash
# Restart the problematic container
docker restart <container-name>
```

---

## Expected Results Summary

### Health Monitor (Terminal 1)
✅ Updates every 30 seconds  
✅ Shows all components green  
✅ Consumer lag < 500  
✅ No missing HDFS blocks  

### Alert System (Terminal 2)
✅ "No active alerts - System healthy"  
✅ Logs to alerts.log  
✅ Detects issues immediately  

### Fault Injection Tests
✅ Kafka test: System survives broker failure  
✅ HDFS test: Blocks re-replicate  
✅ Spark test: Recovers from checkpoint  

---

## Quick Health Check Script

Save this as `quick_check.sh` in monitoring directory:

```bash
#!/bin/bash
echo "=== Quick Health Check ==="
echo ""
echo "1. Docker Containers:"
docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "(broker|spark|namenode|datanode|producer|consumer)"
echo ""
echo "2. Kafka Topic:"
docker exec broker-1 kafka-topics --bootstrap-server broker-1:29092 --list | grep service-logs && echo "✅ Topic exists" || echo "❌ Topic missing"
echo ""
echo "3. HDFS Status:"
docker exec namenode hdfs dfsadmin -report 2>&1 | grep "Live datanodes" || echo "❌ HDFS issue"
echo ""
echo "4. Spark Status:"
docker ps --filter name=spark-processor --format "{{.Status}}" | grep -q "Up" && echo "✅ Spark running" || echo "❌ Spark down"
echo ""
echo "=== Check Complete ==="
```

Then run:
```bash
chmod +x quick_check.sh
./quick_check.sh
```

---

## 🎉 Success!

If you can:
1. ✅ See the health monitor dashboard updating
2. ✅ See "No active alerts" in alert system
3. ✅ Run fault injection test successfully
4. ✅ All containers showing healthy

**Then Person 5 is working perfectly! 🛡️**

---

## What's Next?

1. **Keep monitoring running** - Leave health_monitor.py open
2. **Run all tests** - Complete all fault injection tests
3. **Document results** - Fill in TESTING_RESULTS.md
4. **Share with team** - Show them the monitoring dashboard

---

**Need more help?**
- Check `QUICK_START.md` for detailed setup
- Check `RECOVERY_PLAYBOOK.md` for troubleshooting
- Check `WALKTHROUGH.md` for day-by-day guide

**You're now monitoring the entire distributed log analytics system! 🚀**
