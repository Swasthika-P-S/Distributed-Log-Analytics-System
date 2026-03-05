# 🎯 Person 5: Complete Walkthrough

## Day 1: Setup and First Run

### Morning: Setup (30 minutes)

1. **Navigate to monitoring directory**
   ```bash
   cd /Users/priyadarshinianand/Documents/Distributed-Log-Analytics-System/monitoring
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Verify installation**
   ```bash
   python -c "import kafka; import requests; import psutil; print('✅ All dependencies installed')"
   ```

### Mid-Morning: First Monitoring Run (1 hour)

4. **Start the main system (if not already running)**
   ```bash
   cd ..
   docker-compose up -d
   
   # Wait for all containers to be healthy (2-3 minutes)
   watch -n 5 'docker ps --format "table {{.Names}}\t{{.Status}}"'
   ```

5. **Open 3 terminals and run monitoring tools**
   
   **Terminal 1: Health Monitor**
   ```bash
   cd monitoring
   python health_monitor.py
   ```
   
   **Terminal 2: Alert System**
   ```bash
   cd monitoring
   python alert_system.py
   ```
   
   **Terminal 3: Metrics Collector**
   ```bash
   cd monitoring
   python metrics_collector.py
   ```

6. **Observe for 5-10 minutes**
   - Watch the health monitor refresh
   - See if any alerts trigger
   - Check metrics being collected

### Afternoon: First Fault Injection Test (2 hours)

7. **Run Kafka broker failure test**
   ```bash
   cd monitoring
   python fault_injector.py --test kafka-broker
   ```
   
   **What to observe:**
   - Health monitor should show broker-2 going down
   - Alert system should trigger a warning
   - System should continue functioning
   - Broker should come back up
   
8. **Document results**
   - Open `TESTING_RESULTS.md`
   - Fill in Test 1 section
   - Note any unexpected behavior

9. **Review collected data**
   ```bash
   # Check alerts
   tail -50 alerts.log
   
   # Check metrics
   ls -lh metrics/
   cat metrics/metrics_$(date +%Y%m%d)*.json | head -100
   ```

---

## Day 2: Full Testing Suite

### Morning: Run All Tests (3 hours)

1. **Test HDFS resilience**
   ```bash
   python fault_injector.py --test hdfs-datanode
   ```
   
   Document results in `TESTING_RESULTS.md` (Test 2)

2. **Test Spark recovery**
   ```bash
   python fault_injector.py --test spark
   ```
   
   Document results in `TESTING_RESULTS.md` (Test 3)

3. **Run full system test**
   ```bash
   python fault_injector.py --test full
   ```
   
   This runs all tests sequentially. Document overall results.

### Afternoon: Shell Script Tests (2 hours)

4. **Run shell script versions**
   ```bash
   # These provide more detailed output
   bash scripts/test_kafka_failover.sh > test_kafka.log 2>&1
   bash scripts/test_hdfs_replication.sh > test_hdfs.log 2>&1
   bash scripts/test_spark_recovery.sh > test_spark.log 2>&1
   ```

5. **Review logs**
   ```bash
   cat test_kafka.log
   cat test_hdfs.log
   cat test_spark.log
   ```

6. **Run comprehensive test**
   ```bash
   bash scripts/full_system_test.sh
   ```

---

## Day 3: Stress Testing and Optimization

### Morning: Stress Test (2 hours)

1. **Prepare for stress test**
   ```bash
   # Check baseline metrics
   docker stats --no-stream
   ```

2. **Run stress test**
   ```bash
   bash scripts/stress_test.sh
   ```

3. **Monitor during stress test**
   - Keep health_monitor.py running in another terminal
   - Watch consumer lag
   - Monitor resource usage

4. **Analyze results**
   - Did consumer lag stay under control?
   - Did any components fail?
   - What was max throughput?

### Afternoon: Threshold Tuning (2 hours)

5. **Review alert history**
   ```bash
   cat alerts.log
   ```

6. **Adjust thresholds if needed**
   
   Edit `config/alert_rules.yaml`:
   ```yaml
   thresholds:
     consumer_lag_warning: 500    # Too sensitive? Increase
     consumer_lag_critical: 2000  # Adjust based on observations
   ```

7. **Test new thresholds**
   - Restart alert_system.py
   - Run some tests again
   - Verify alerts are appropriate

---

## Daily Routine (15-30 minutes)

### Morning Health Check

```bash
cd monitoring

# 1. Quick health check
python -c "
from health_monitor import HealthMonitor, load_config
config = load_config()
monitor = HealthMonitor(config)
metrics = monitor.collect_all_metrics()
monitor.print_summary(metrics)
"

# 2. Check alerts from last 24 hours
tail -100 alerts.log | grep -E "(WARNING|CRITICAL)"

# 3. Review metrics trend
ls -lh metrics/ | tail -20
```

### Evening Health Check

```bash
# Quick status of all components
docker ps --format "table {{.Names}}\t{{.Status}}"

# Check consumer lag
docker exec broker-1 kafka-consumer-groups \
  --bootstrap-server broker-1:29092 \
  --describe \
  --group spark-log-processor

# Check HDFS health
docker exec namenode hdfs dfsadmin -report | head -30
```

---

## Weekly Tasks (1-2 hours)

### Every Monday

1. **Run fault injection tests**
   ```bash
   python fault_injector.py --test full
   ```

2. **Review metrics trends**
   ```bash
   # Analyze weekly metrics
   cd metrics
   cat metrics_*.jsonl | grep "consumer_lag" | tail -100
   ```

3. **Update team**
   - Share test results
   - Report any issues found
   - Suggest improvements

### Every Friday

1. **Clean up old data**
   ```bash
   # Remove metrics older than 7 days
   find metrics/ -name "*.json" -mtime +7 -delete
   
   # Rotate alert logs if too large
   if [ $(wc -l < alerts.log) -gt 10000 ]; then
     mv alerts.log alerts.log.old
     touch alerts.log
   fi
   ```

2. **Review alert patterns**
   ```bash
   # Most common alerts
   grep -E "(WARNING|CRITICAL)" alerts.log | \
     awk '{print $5}' | sort | uniq -c | sort -rn
   ```

3. **Update documentation**
   - Document any new issues found
   - Update RECOVERY_PLAYBOOK.md if needed
   - Share learnings with team

---

## Incident Response Workflow

### When Alert Triggers

1. **Identify severity**
   - INFO: Just logging, no action needed
   - WARNING: Monitor, may need action soon
   - CRITICAL: Immediate action required

2. **Check health monitor**
   - What component is affected?
   - Are other components still running?
   - What is consumer lag?

3. **Follow playbook**
   - Open `RECOVERY_PLAYBOOK.md`
   - Find matching scenario
   - Follow step-by-step instructions

4. **Document incident**
   ```bash
   # Add to incident log
   echo "$(date): [INCIDENT] Description of issue and resolution" >> incidents.log
   ```

5. **Notify team**
   - Update team chat/email
   - Share resolution steps
   - Schedule post-mortem if severe

---

## Common Scenarios and Quick Fixes

### Scenario: High Consumer Lag

```bash
# 1. Check if Spark is running
docker ps --filter name=spark-processor

# 2. Check Spark logs
docker logs spark-processor --tail 50

# 3. If Spark stopped, restart
docker restart spark-processor

# 4. Monitor lag decrease
watch -n 10 'docker exec broker-1 kafka-consumer-groups \
  --bootstrap-server broker-1:29092 \
  --describe --group spark-log-processor | grep service-logs'
```

### Scenario: Kafka Broker Down

```bash
# 1. Identify which broker
docker ps -a --filter name=broker

# 2. Check logs
docker logs broker-X --tail 100  # Replace X with broker number

# 3. Restart
docker restart broker-X

# 4. Verify recovery
docker exec broker-1 kafka-topics \
  --bootstrap-server broker-1:29092 \
  --describe --topic service-logs
```

### Scenario: HDFS Missing Blocks

```bash
# 1. Check HDFS status
docker exec namenode hdfs dfsadmin -report

# 2. Run fsck
docker exec namenode hdfs fsck / -blocks -locations

# 3. Check DataNodes
docker ps --filter name=datanode

# 4. If DataNode down, restart
docker restart datanode1  # or datanode2

# 5. Wait for re-replication
# Check every few minutes until under-replicated = 0
```

---

## Performance Tuning Tips

### If Consumer Lag is Consistently High

1. **Check Spark processing rate**
   ```bash
   docker logs spark-processor | grep "processed"
   ```

2. **Consider scaling Spark** (coordinate with Person 3)
   - Increase Spark executors
   - Increase memory allocation
   - Optimize window sizes

3. **Check HDFS write performance**
   ```bash
   docker stats namenode datanode1 datanode2 --no-stream
   ```

### If Alerts are Too Noisy

1. **Adjust thresholds in `config/alert_rules.yaml`**
   ```yaml
   thresholds:
     consumer_lag_warning: 1000  # Increased from 500
   ```

2. **Enable alert suppression**
   ```yaml
   suppression:
     duplicate_alert_window: 600  # 10 minutes instead of 5
   ```

### If Metrics Collection is Slow

1. **Increase collection interval**
   ```yaml
   # In monitor_config.yaml
   monitoring:
     refresh_interval: 60  # Increase from 30
   ```

2. **Reduce parallel checks**
   - Monitor only critical components during high load

---

## Integration with Team

### Share with Person 1 (Producer)

```bash
# Producer message rate
docker logs producer | tail -50

# If producer issues, notify Person 1
```

### Share with Person 2 (Kafka)

```bash
# Kafka cluster health
docker exec broker-1 kafka-topics --bootstrap-server broker-1:29092 --describe --topic service-logs

# If Kafka issues, notify Person 2
```

### Share with Person 3 (Spark)

```bash
# Spark processing status
docker logs spark-processor --tail 100

# Consumer lag trends
cat metrics/metrics_*.jsonl | grep "total_lag"

# If Spark issues, notify Person 3
```

### Share with Person 4 (HDFS)

```bash
# HDFS health
docker exec namenode hdfs dfsadmin -report

# Block health
docker exec namenode hdfs fsck / | head -50

# If HDFS issues, notify Person 4
```

---

## Success Checklist

### After 1 Week

- [ ] Monitoring tools running continuously
- [ ] All fault injection tests completed
- [ ] Test results documented
- [ ] Alert thresholds tuned
- [ ] Team trained on recovery procedures
- [ ] Incident response process established

### After 1 Month

- [ ] System stability > 99%
- [ ] Recovery time < 2 minutes for all scenarios
- [ ] No data loss incidents
- [ ] Alert noise minimized
- [ ] Metrics trends analyzed
- [ ] Performance optimization completed

### Production Readiness

- [ ] 24-hour stability test passed
- [ ] All fault scenarios tested successfully
- [ ] Stress test completed (1000+ msg/sec)
- [ ] Recovery playbook validated
- [ ] Team confidence in system reliability
- [ ] Monitoring dashboards established
- [ ] Escalation procedures defined

---

## Tips for Success

1. **Start Simple**
   - Run health monitor first
   - Get familiar with normal operation
   - Then introduce fault injection

2. **Document Everything**
   - Keep detailed notes in TESTING_RESULTS.md
   - Log all incidents
   - Share learnings with team

3. **Test Regularly**
   - Weekly fault injection tests
   - Monthly stress tests
   - Continuous monitoring

4. **Communicate Proactively**
   - Share daily health status
   - Alert team early on issues
   - Celebrate stability milestones

5. **Continuously Improve**
   - Review alert patterns
   - Optimize thresholds
   - Update playbooks
   - Automate more recovery

---

## Resources

- **Quick Reference:** `QUICK_START.md`
- **Detailed Docs:** `README.md`
- **Emergency Guide:** `RECOVERY_PLAYBOOK.md`
- **Architecture:** `ARCHITECTURE.md`
- **Test Templates:** `TESTING_RESULTS.md`

---

## Questions?

If you encounter issues:

1. Check the relevant documentation
2. Review similar scenarios in RECOVERY_PLAYBOOK.md
3. Ask component owner (Person 1-4)
4. Document the issue for future reference

---

**Remember: You're the guardian of system reliability! Every test makes the system stronger. Every incident is a learning opportunity.** 🛡️

Good luck, Person 5! 🚀
