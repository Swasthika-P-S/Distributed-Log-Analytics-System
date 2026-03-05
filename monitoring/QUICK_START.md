# 🛡️ Person 5: Quick Start Guide

## Overview
You are the **Fault Tolerance & Monitoring Coordinator**. Your role is to ensure the entire distributed log analytics system is resilient, monitored, and recovers gracefully from failures.

---

## Prerequisites

1. **All other components running:**
   - Person 1: Producer (sending logs)
   - Person 2: Kafka cluster (3 brokers)
   - Person 3: Spark processor
   - Person 4: HDFS cluster

2. **Tools installed:**
   - Python 3.8+
   - Docker
   - Git

---

## Step 1: Install Dependencies

```bash
cd monitoring
pip install -r requirements.txt
```

This installs:
- `kafka-python` - Kafka monitoring
- `requests` - HTTP health checks
- `psutil` - System metrics
- `colorama` - Colored terminal output
- `tabulate` - Formatted tables
- `pyyaml` - Configuration parsing

---

## Step 2: Configure Monitoring

Edit `config/monitor_config.yaml` if needed:

```yaml
kafka:
  bootstrap_servers: "localhost:9092,localhost:9093,localhost:9094"
  
hdfs:
  namenode_web_ui: "http://localhost:9870"
  
spark:
  web_ui: "http://localhost:8080"
```

**Note:** Default configuration should work if all components are on localhost.

---

## Step 3: Start Health Monitor

```bash
python health_monitor.py
```

This will:
- ✅ Check all Docker containers
- ✅ Monitor Kafka brokers
- ✅ Track consumer lag
- ✅ Check HDFS health
- ✅ Verify Spark status
- 🔄 Refresh every 30 seconds

**Expected Output:**
```
🛡️  DISTRIBUTED LOG ANALYTICS - HEALTH MONITOR (Person 5)
================================================================================
Last Updated: 2026-03-05 10:30:45
================================================================================

🐳 DOCKER CONTAINERS STATUS
────────────────────────────────────────────────────────────────────────────────
Container          Status
─────────────────  ────────────────────────────────────────────────────────────
broker-1           ✅ Up 2 minutes (healthy)
broker-2           ✅ Up 2 minutes (healthy)
broker-3           ✅ Up 2 minutes (healthy)
...
```

---

## Step 4: Start Alert System

In a **new terminal**:

```bash
python alert_system.py
```

This will:
- 🚨 Alert on critical issues
- ⚠️ Warn on threshold violations
- ✅ Track alert history
- 📝 Log to `alerts.log`

**Example Alerts:**
```
2026-03-05 10:35:12 | INFO     | 🛡️  Alert System Started (Person 5)
2026-03-05 10:35:15 | INFO     | ✅ No active alerts - System healthy
2026-03-05 10:40:22 | WARNING  | ⚠️ [CONSUMER_LAG] High consumer lag: 750 messages
2026-03-05 10:42:10 | ERROR    | 🚨 [KAFKA] Kafka broker broker-2 down
```

---

## Step 5: Collect Metrics

In **another terminal**:

```bash
python metrics_collector.py
```

This will:
- 📊 Collect metrics every 60 seconds
- 💾 Save to `metrics/` directory
- 📈 Track trends over time

**Output files:**
```
metrics/
├── metrics_20260305_103012.json       # Individual snapshots
├── metrics_20260305_103112.json
└── metrics_20260305.jsonl             # Daily append log
```

---

## Step 6: Run Fault Injection Tests

### Test 1: Kafka Broker Failure

```bash
python fault_injector.py --test kafka-broker
```

**What it does:**
1. Kills `broker-2`
2. Verifies leader election
3. Confirms topic still accessible
4. Restarts broker
5. Verifies recovery

**Expected:** ✅ System continues working, no data loss

---

### Test 2: HDFS DataNode Failure

```bash
python fault_injector.py --test hdfs-datanode
```

**What it does:**
1. Kills `datanode1`
2. Checks for under-replicated blocks
3. Verifies auto re-replication
4. Restarts datanode
5. Confirms recovery

**Expected:** ✅ HDFS maintains availability, blocks re-replicate

---

### Test 3: Spark Recovery

```bash
python fault_injector.py --test spark
```

**What it does:**
1. Kills Spark container
2. Restarts Spark
3. Verifies checkpoint recovery
4. Checks for duplicates

**Expected:** ✅ Spark resumes from checkpoint, no duplicates

---

### Test 4: Full System Test

```bash
python fault_injector.py --test full
```

Runs all tests sequentially.

---

## Step 7: Run Shell Scripts (Alternative)

You can also use shell scripts directly:

```bash
# Make scripts executable
chmod +x scripts/*.sh

# Run individual tests
bash scripts/test_kafka_failover.sh
bash scripts/test_hdfs_replication.sh
bash scripts/test_spark_recovery.sh

# Or run full test suite
bash scripts/full_system_test.sh
```

---

## Step 8: Stress Test (Optional)

```bash
bash scripts/stress_test.sh
```

This simulates high load to test system limits.

---

## Common Commands

### Quick Health Check
```bash
# Check all containers
docker ps --format "table {{.Names}}\t{{.Status}}"

# Check Kafka topics
docker exec broker-1 kafka-topics --bootstrap-server broker-1:29092 --list

# Check HDFS
docker exec namenode hdfs dfsadmin -report

# Check Spark logs
docker logs --tail 50 spark-processor
```

### Manual Recovery Commands

**Restart Kafka broker:**
```bash
docker restart broker-2
```

**Restart HDFS DataNode:**
```bash
docker restart datanode1
```

**Restart Spark:**
```bash
docker restart spark-processor
```

**Restart all components:**
```bash
docker-compose -f ../docker-compose.yml restart
```

---

## Key Metrics to Monitor

| Component | Key Metrics | Healthy Range |
|-----------|-------------|---------------|
| **Kafka** | Brokers UP | 3/3 |
| | Consumer Lag | < 500 messages |
| | Leader Elections | Minimal |
| **HDFS** | Live DataNodes | 2/2 |
| | Missing Blocks | 0 |
| | Disk Usage | < 80% |
| **Spark** | Status | RUNNING |
| | Processing Rate | > 10 msg/sec |
| | Checkpoint Health | EXISTS |

---

## Troubleshooting

### Problem: Health monitor can't connect to Kafka

**Solution:**
```bash
# Check if Kafka is running
docker ps --filter name=broker

# Check Kafka logs
docker logs broker-1

# Restart Kafka
docker-compose -f ../docker-compose.yml restart broker-1 broker-2 broker-3
```

### Problem: HDFS web UI not accessible

**Solution:**
```bash
# Check NameNode
docker ps --filter name=namenode

# Check NameNode logs
docker logs namenode

# Restart HDFS
docker-compose -f ../docker-compose.yml restart namenode datanode1 datanode2
```

### Problem: High consumer lag

**Solution:**
1. Check Spark is running: `docker ps --filter name=spark`
2. Check Spark logs: `docker logs spark-processor`
3. Restart Spark if needed: `docker restart spark-processor`
4. Verify Kafka-Spark connectivity

### Problem: Missing blocks in HDFS

**Solution:**
```bash
# Check HDFS status
docker exec namenode hdfs dfsadmin -report

# Run HDFS fsck
docker exec namenode hdfs fsck / -blocks -locations

# If blocks are under-replicated, wait for auto-recovery
# Or manually trigger: hdfs dfsadmin -triggerBlockReport <datanode>
```

---

## Success Criteria

Before declaring system production-ready:

- [ ] All health checks pass continuously for 24 hours
- [ ] Kafka broker failure tested successfully (3 times)
- [ ] HDFS DataNode failure tested successfully (2 times)
- [ ] Spark recovery tested successfully
- [ ] Stress test completed (1000+ msg/sec)
- [ ] Consumer lag < 500 messages under normal load
- [ ] No data loss in any failure scenario
- [ ] Recovery time < 2 minutes for all failures
- [ ] All team members trained on recovery procedures

---

## Daily Checklist

Run this every day:

```bash
# 1. Check system health
python health_monitor.py  # Let it run for 1 cycle

# 2. Review alerts
tail -50 alerts.log

# 3. Check metrics
ls -lh metrics/

# 4. Quick smoke test
bash scripts/test_kafka_failover.sh
```

---

## Team Communication

**Notify team immediately if:**
- 🚨 All Kafka brokers down
- 🚨 HDFS has missing blocks
- 🚨 Spark fails to restart
- 🚨 Consumer lag > 2000 messages
- 🚨 Disk usage > 90%

**Share daily:**
- Current system health status
- Any alerts triggered
- Consumer lag trends
- Planned maintenance windows

---

## Next Steps

1. ✅ Complete initial setup (Steps 1-3)
2. ✅ Run all fault injection tests (Step 6)
3. ✅ Document results in `TESTING_RESULTS.md`
4. ✅ Share findings with team
5. ✅ Set up continuous monitoring
6. ✅ Create team runbook with recovery procedures

---

**Questions? Issues?**
- Check `README.md` for detailed documentation
- Review `TESTING_RESULTS.md` for test templates
- See `../TEAM_INTEGRATION_GUIDE.md` for team coordination

**You're the guardian of system reliability! 🛡️**
