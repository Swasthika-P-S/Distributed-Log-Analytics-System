# 🛡️ Person 5: Fault Tolerance & Monitoring Coordinator

> **Role:** Monitor all components (Kafka, Spark, HDFS) and ensure system resilience  
> **Pipeline Oversight:** Producer → Kafka → Spark → HDFS (All stages monitored)

---

## 🎯 Responsibilities

1. **Health Monitoring** - Track status of all services
2. **Fault Injection Testing** - Simulate failures to verify resilience
3. **Alerting System** - Detect and notify on anomalies
4. **Recovery Automation** - Auto-restart failed components
5. **Performance Metrics** - Track throughput, latency, and resource usage

---

## 📂 Directory Structure

```
monitoring/
├── health_monitor.py          ← Real-time health dashboard (all components)
├── fault_injector.py          ← Kill/restart services to test resilience
├── alert_system.py            ← Alert on threshold violations
├── metrics_collector.py       ← Collect and store performance metrics
├── recovery_orchestrator.py   ← Auto-recovery logic
├── prometheus.yml             ← Prometheus configuration (optional)
├── grafana_dashboard.json     ← Grafana dashboard config (optional)
├── scripts/
│   ├── test_kafka_failover.sh       ← Kill Kafka broker, verify ISR
│   ├── test_hdfs_replication.sh     ← Kill DataNode, verify block recovery
│   ├── test_spark_recovery.sh       ← Kill Spark, verify job restart
│   ├── stress_test.sh               ← High-load testing
│   └── full_system_test.sh          ← End-to-end validation
├── config/
│   ├── alert_rules.yaml       ← Alerting thresholds
│   └── monitor_config.yaml    ← Monitoring configuration
└── README.md                  ← This file
```

---

## 🚀 Quick Start

### Step 1: Install Dependencies

```bash
cd monitoring
pip install -r requirements.txt
```

### Step 2: Start Monitoring Dashboard

```bash
python health_monitor.py
```

This will:
- ✅ Check Kafka brokers (all 3)
- ✅ Check HDFS NameNode & DataNodes
- ✅ Check Spark processor
- ✅ Check Producer/Consumer status
- ✅ Display real-time metrics in terminal

### Step 3: Run Fault Injection Tests

**Test 1: Kafka Broker Failure**
```bash
python fault_injector.py --test kafka-broker --kill broker-2
```
Expected: System continues running, leader election happens, ISR recovers

**Test 2: HDFS DataNode Failure**
```bash
python fault_injector.py --test hdfs-datanode --kill datanode1
```
Expected: Blocks replicate to remaining DataNode, no data loss

**Test 3: Spark Process Failure**
```bash
python fault_injector.py --test spark --restart
```
Expected: Spark restarts from checkpoint, processes continue

**Test 4: Full Stress Test**
```bash
bash scripts/full_system_test.sh
```

---

## 📊 Monitoring Features

### 1. Health Checks (health_monitor.py)

Continuously monitors:
- **Kafka**: Broker availability, topic health, consumer lag
- **HDFS**: NameNode status, DataNode count, block health, replication factor
- **Spark**: Job status, processing rate, checkpoint health
- **Producer**: Message send rate, failure rate
- **Consumer**: Message receive rate, lag

### 2. Metrics Collection (metrics_collector.py)

Tracks:
- **Throughput**: Messages/sec through pipeline
- **Latency**: End-to-end processing time
- **Resource Usage**: CPU, memory, disk, network
- **Error Rates**: Failed messages, retries
- **Data Loss**: Verify no messages lost during failures

### 3. Alert System (alert_system.py)

Triggers alerts when:
- ⚠️ Kafka broker goes down
- ⚠️ Consumer lag > 1000 messages
- ⚠️ HDFS replication factor < 2
- ⚠️ Spark job fails/restarts
- ⚠️ Error rate > 5%
- ⚠️ Disk usage > 80%

### 4. Auto-Recovery (recovery_orchestrator.py)

Automatically:
- Restarts failed containers
- Rebalances Kafka partitions
- Triggers HDFS block re-replication
- Restarts Spark from last checkpoint

---

## 🧪 Fault Tolerance Test Scenarios

### Test 1: Kafka Broker Failure
**Objective:** Verify system handles broker failure without data loss

```bash
bash scripts/test_kafka_failover.sh
```

**Steps:**
1. Kill `broker-2`
2. Verify leader election for affected partitions
3. Check ISR (In-Sync Replicas) status
4. Verify producer/consumer continue working
5. Restart broker and verify re-sync

**Expected Result:**
- ✅ Leader elected from remaining brokers
- ✅ No message loss
- ✅ Producer retries successful
- ✅ Consumer continues reading

---

### Test 2: HDFS DataNode Failure
**Objective:** Verify HDFS maintains replication factor

```bash
bash scripts/test_hdfs_replication.sh
```

**Steps:**
1. Kill `datanode1`
2. Check block under-replication
3. Verify NameNode triggers re-replication
4. Restart DataNode
5. Verify blocks rebalance

**Expected Result:**
- ✅ Blocks under-replicated temporarily
- ✅ Automatic re-replication to datanode2
- ✅ No data loss
- ✅ System continues writing

---

### Test 3: Spark Job Failure
**Objective:** Verify Spark recovers from checkpoint

```bash
bash scripts/test_spark_recovery.sh
```

**Steps:**
1. Kill Spark container
2. Restart Spark
3. Verify reads from last checkpoint
4. Check for duplicate processing

**Expected Result:**
- ✅ Spark restarts from checkpoint
- ✅ No duplicate data in HDFS
- ✅ Processing continues without manual intervention

---

### Test 4: Network Partition
**Objective:** Simulate network issues between components

```bash
python fault_injector.py --test network --partition kafka-spark
```

**Steps:**
1. Block network between Kafka and Spark
2. Verify Kafka buffers messages
3. Restore network
4. Verify Spark catches up

**Expected Result:**
- ✅ Messages buffered in Kafka
- ✅ No message loss
- ✅ Consumer lag increases then recovers

---

## 📈 Success Metrics

### Key Performance Indicators (KPIs)

| Metric | Target | Critical Threshold |
|--------|--------|-------------------|
| **End-to-End Latency** | < 10 seconds | > 30 seconds |
| **Message Throughput** | > 100 msg/sec | < 10 msg/sec |
| **Data Loss** | 0% | > 0.1% |
| **Availability** | > 99.9% | < 95% |
| **Recovery Time** | < 2 minutes | > 5 minutes |
| **Consumer Lag** | < 500 messages | > 2000 messages |
| **HDFS Replication** | = 2 | < 2 |

---

## 🔧 Configuration Files

### alert_rules.yaml

Defines alert thresholds and notification channels.

### monitor_config.yaml

Monitoring intervals, endpoints, and component details.

---

## 📞 Alert Channels

Configure alerts to:
- **Console**: Print to terminal (default)
- **File**: Write to `alerts.log`
- **Email**: Send to team (requires SMTP config)
- **Slack**: Post to Slack channel (requires webhook)
- **PagerDuty**: For critical issues (requires API key)

---

## 🎓 Best Practices

1. **Run monitoring BEFORE starting other components**
2. **Test fault scenarios in order** (simple → complex)
3. **Document all failure observations**
4. **Keep metrics history for trend analysis**
5. **Automate recovery procedures**

---

## 🚨 Emergency Procedures

### If Kafka Cluster is Down
```bash
# Check broker logs
docker logs broker-1
docker logs broker-2
docker logs broker-3

# Restart all brokers
docker-compose -f ../docker-compose.yml restart broker-1 broker-2 broker-3
```

### If HDFS is Unavailable
```bash
# Check NameNode status
docker exec namenode hdfs dfsadmin -report

# Safe mode issues
docker exec namenode hdfs dfsadmin -safemode leave

# Restart HDFS
docker-compose -f ../docker-compose.yml restart namenode datanode1 datanode2
```

### If Spark Stops Processing
```bash
# Check Spark logs
docker logs spark-processor

# Check checkpoint location
docker exec namenode hdfs dfs -ls /spark-checkpoints

# Restart Spark
docker-compose -f ../docker-compose.yml restart spark
```

---

## 📚 Additional Resources

- [Kafka Monitoring Guide](https://kafka.apache.org/documentation/#monitoring)
- [HDFS Monitoring Best Practices](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html#HDFS_Monitoring)
- [Spark Monitoring](https://spark.apache.org/docs/latest/monitoring.html)

---

## 👥 Team Coordination

**Daily Standup Questions:**
1. Are all components healthy?
2. Any alerts in the last 24 hours?
3. What was the max consumer lag?
4. Any component restarts?
5. Current throughput vs baseline?

**Weekly Review:**
1. Analyze failure patterns
2. Review recovery times
3. Optimize alert thresholds
4. Update runbooks

---

## 🏆 Testing Checklist

Before declaring system production-ready:

- [ ] All health checks passing
- [ ] Kafka broker failure tested (3 times)
- [ ] HDFS DataNode failure tested (2 times)
- [ ] Spark recovery from checkpoint tested
- [ ] Network partition tested
- [ ] High load stress test (1000+ msg/sec)
- [ ] 24-hour stability test
- [ ] Data loss verification (0 messages lost)
- [ ] Recovery time < 2 minutes for all scenarios
- [ ] Monitoring dashboard functional
- [ ] Alert system working
- [ ] Team trained on recovery procedures

---

**Next Steps:**
1. Run `python health_monitor.py` to start monitoring
2. Execute fault injection tests one by one
3. Document results in `TESTING_RESULTS.md`
4. Share findings with team
