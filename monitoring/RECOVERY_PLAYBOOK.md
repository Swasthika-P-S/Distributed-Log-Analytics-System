# 🚨 Recovery Playbook
## Person 5: Emergency Procedures

This document contains step-by-step recovery procedures for all failure scenarios.

---

## 📋 Emergency Contacts

| Person | Component | Contact |
|--------|-----------|---------|
| Person 1 | Producer | ___________ |
| Person 2 | Kafka | ___________ |
| Person 3 | Spark | ___________ |
| Person 4 | HDFS | ___________ |
| Person 5 | Monitoring | ___________ |

---

## 🔥 Severity Levels

- **🚨 CRITICAL (P0):** Complete system down, data loss imminent
- **⚠️ HIGH (P1):** Major component down, degraded performance
- **⚠️ MEDIUM (P2):** Minor issues, system functional
- **ℹ️ LOW (P3):** Warnings, no immediate action needed

---

## SCENARIO 1: All Kafka Brokers Down 🚨

**Severity:** CRITICAL (P0)  
**Impact:** No logs being processed, system down  
**Detection:** Health monitor shows 0/3 brokers UP

### Immediate Actions

1. **Notify team immediately**
   ```bash
   # Contact Person 2 (Kafka Manager)
   ```

2. **Check broker status**
   ```bash
   docker ps -a --filter name=broker --format "table {{.Names}}\t{{.Status}}"
   ```

3. **Check logs for root cause**
   ```bash
   docker logs broker-1 --tail 100
   docker logs broker-2 --tail 100
   docker logs broker-3 --tail 100
   ```

4. **Attempt restart (if containers stopped)**
   ```bash
   docker start broker-1 broker-2 broker-3
   ```

5. **If restart fails, recreate containers**
   ```bash
   cd /path/to/project
   docker-compose restart broker-1 broker-2 broker-3
   ```

6. **Verify recovery**
   ```bash
   # Wait 30 seconds for startup
   sleep 30
   
   # Check broker health
   docker exec broker-1 kafka-broker-api-versions \
     --bootstrap-server broker-1:29092
   
   # Check topic exists
   docker exec broker-1 kafka-topics \
     --bootstrap-server broker-1:29092 \
     --list | grep service-logs
   ```

7. **Verify data integrity**
   ```bash
   # Check consumer lag
   docker exec broker-1 kafka-consumer-groups \
     --bootstrap-server broker-1:29092 \
     --describe \
     --group spark-log-processor
   ```

### Root Cause Analysis
- [ ] Check disk space on host
- [ ] Check Zookeeper health
- [ ] Review Kafka logs for errors
- [ ] Check network connectivity
- [ ] Review recent config changes

### Prevention
- Set up disk space alerts
- Enable automatic container restarts
- Regular Kafka cluster health checks

---

## SCENARIO 2: Single Kafka Broker Down ⚠️

**Severity:** MEDIUM (P2)  
**Impact:** Reduced redundancy, system functional  
**Detection:** Health monitor shows 2/3 brokers UP

### Immediate Actions

1. **Verify system is still functioning**
   ```bash
   # Topic should still be accessible via other brokers
   docker exec broker-1 kafka-topics \
     --bootstrap-server broker-1:29092 \
     --list | grep service-logs
   ```

2. **Check which broker is down**
   ```bash
   docker ps -a --filter name=broker
   ```

3. **Restart the down broker**
   ```bash
   docker restart broker-<X>  # Replace <X> with broker number
   ```

4. **Wait for re-sync**
   ```bash
   # Wait 15-30 seconds
   sleep 30
   ```

5. **Verify leader election**
   ```bash
   docker exec broker-1 kafka-topics \
     --bootstrap-server broker-1:29092 \
     --describe \
     --topic service-logs
   ```

6. **Check ISR (In-Sync Replicas)**
   ```bash
   # ISR should include all brokers again
   bash ../kafka-cluster/scripts/verify_replication.sh
   ```

### When to Escalate to P1
- Broker won't restart after 3 attempts
- ISR doesn't recover after 5 minutes
- Second broker fails

---

## SCENARIO 3: HDFS NameNode Down 🚨

**Severity:** CRITICAL (P0)  
**Impact:** Cannot write processed logs, system will fill buffers  
**Detection:** HDFS health check fails

### Immediate Actions

1. **URGENT: Contact Person 4 (HDFS Manager)**

2. **Check NameNode status**
   ```bash
   docker ps -a --filter name=namenode
   docker logs namenode --tail 100
   ```

3. **Check if in safe mode**
   ```bash
   docker exec namenode hdfs dfsadmin -safemode get
   ```

4. **If in safe mode, try leaving**
   ```bash
   docker exec namenode hdfs dfsadmin -safemode leave
   ```

5. **If NameNode stopped, restart**
   ```bash
   docker restart namenode
   sleep 60  # NameNode takes longer to start
   ```

6. **Verify NameNode is up**
   ```bash
   # Check web UI
   curl http://localhost:9870
   
   # Check HDFS command
   docker exec namenode hdfs dfsadmin -report
   ```

7. **Check for missing blocks**
   ```bash
   docker exec namenode hdfs fsck / -blocks -locations
   ```

### Important Notes
- ⚠️ **DO NOT** force leave safe mode without Person 4's approval
- ⚠️ Missing blocks = potential data loss
- ⚠️ If NameNode won't start, metadata may be corrupted

### Temporary Workaround
If NameNode can't be restored immediately:
1. Spark will buffer data in checkpoints
2. Can tolerate ~2-4 hours before checkpoint disk fills
3. Consider temporarily stopping producer to reduce backlog

---

## SCENARIO 4: HDFS DataNode Down ⚠️

**Severity:** MEDIUM (P2) if 1 down, HIGH (P1) if both down  
**Impact:** Reduced replication, risk of data loss  
**Detection:** HDFS shows < 2 live DataNodes

### Immediate Actions (1 DataNode Down)

1. **Check DataNode status**
   ```bash
   docker ps -a --filter name=datanode
   docker logs datanode1 --tail 50
   docker logs datanode2 --tail 50
   ```

2. **Restart failed DataNode**
   ```bash
   docker restart datanode<X>  # Replace <X> with 1 or 2
   ```

3. **Wait for DataNode to register**
   ```bash
   sleep 30
   ```

4. **Verify DataNode rejoined**
   ```bash
   docker exec namenode hdfs dfsadmin -report
   ```

5. **Check for under-replicated blocks**
   ```bash
   docker exec namenode hdfs fsck / | grep "Under replicated"
   ```

6. **Wait for re-replication (automatic)**
   ```bash
   # HDFS will automatically re-replicate blocks
   # Check progress every few minutes
   watch -n 60 'docker exec namenode hdfs fsck / | grep "Under replicated"'
   ```

### Immediate Actions (Both DataNodes Down) 🚨

1. **CRITICAL: Notify Person 4 immediately**

2. **Check if data still accessible (read-only from NameNode cache)**
   ```bash
   docker exec namenode hdfs dfs -ls /logs/processed
   ```

3. **Restart both DataNodes**
   ```bash
   docker restart datanode1 datanode2
   sleep 60
   ```

4. **Check if blocks are corrupt**
   ```bash
   docker exec namenode hdfs fsck / -files -blocks -locations
   ```

5. **If blocks missing, check for backups**
   - Contact Person 4 for backup procedures
   - May need to restore from external backup

---

## SCENARIO 5: Spark Processor Down ⚠️

**Severity:** HIGH (P1)  
**Impact:** Logs accumulating in Kafka, not being processed  
**Detection:** Spark container not running, high consumer lag

### Immediate Actions

1. **Check Spark container status**
   ```bash
   docker ps -a --filter name=spark-processor
   docker logs spark-processor --tail 100
   ```

2. **Restart Spark**
   ```bash
   docker restart spark-processor
   sleep 20
   ```

3. **Verify Spark started**
   ```bash
   docker logs spark-processor --tail 50
   ```

4. **Check if recovering from checkpoint**
   ```bash
   # Look for checkpoint recovery messages in logs
   docker logs spark-processor | grep -i checkpoint
   ```

5. **Monitor consumer lag**
   ```bash
   docker exec broker-1 kafka-consumer-groups \
     --bootstrap-server broker-1:29092 \
     --describe \
     --group spark-log-processor
   ```

6. **If lag not decreasing after 5 minutes**
   ```bash
   # Check Spark can connect to Kafka
   docker logs spark-processor | grep -i "kafka"
   
   # Check Spark can connect to HDFS
   docker logs spark-processor | grep -i "hdfs"
   ```

### If Spark Won't Start

1. **Check checkpoint corruption**
   ```bash
   docker exec namenode hdfs dfs -ls /spark-checkpoints
   ```

2. **As last resort, delete checkpoint and restart**
   ```bash
   # ⚠️ WARNING: This may cause duplicate processing
   # Get approval from Person 3 first
   docker exec namenode hdfs dfs -rm -r /spark-checkpoints/*
   docker restart spark-processor
   ```

---

## SCENARIO 6: Producer Stopped ⚠️

**Severity:** MEDIUM (P2)  
**Impact:** No new logs being generated  
**Detection:** Consumer lag not increasing, message rate = 0

### Immediate Actions

1. **Contact Person 1 (Producer Manager)**

2. **Check producer container**
   ```bash
   docker ps -a --filter name=producer
   docker logs producer --tail 50
   ```

3. **Restart producer**
   ```bash
   docker restart producer
   ```

4. **Verify logs flowing again**
   ```bash
   # Watch consumer lag - should start increasing
   watch -n 5 'docker exec broker-1 kafka-consumer-groups \
     --bootstrap-server broker-1:29092 \
     --describe \
     --group spark-log-processor | grep service-logs'
   ```

---

## SCENARIO 7: High Consumer Lag ⚠️

**Severity:** MEDIUM (P2) if < 2000, HIGH (P1) if > 2000  
**Impact:** Increased latency, buffering in Kafka  
**Detection:** Alert system triggers lag warning

### Root Cause Analysis

1. **Is Spark processing?**
   ```bash
   docker ps --filter name=spark-processor
   docker logs spark-processor --tail 20
   ```

2. **Is HDFS accepting writes?**
   ```bash
   docker exec namenode hdfs dfsadmin -report
   ```

3. **Is producer rate too high?**
   ```bash
   # Check message rate
   docker logs producer --tail 50 | grep "messages/sec"
   ```

### Resolution Options

**Option A: Spark is slow**
```bash
# Check Spark resource usage
docker stats spark-processor --no-stream

# If high CPU/memory, may need to scale
# Contact Person 3 for Spark tuning
```

**Option B: HDFS is slow**
```bash
# Check HDFS disk I/O
docker stats namenode datanode1 datanode2 --no-stream

# Check for under-replicated blocks
docker exec namenode hdfs fsck / | grep "Under replicated"
```

**Option C: Producer rate too high (temporary)**
```bash
# Temporarily reduce producer rate
# Contact Person 1 to adjust config
```

---

## SCENARIO 8: Disk Space Critical 🚨

**Severity:** CRITICAL (P0)  
**Impact:** System will crash when disk full  
**Detection:** Disk usage > 90%

### Immediate Actions

1. **Check disk usage**
   ```bash
   df -h
   docker system df
   ```

2. **Clean up Docker**
   ```bash
   # Remove stopped containers
   docker container prune -f
   
   # Remove unused images
   docker image prune -a -f
   
   # Remove unused volumes (⚠️ careful!)
   docker volume prune -f
   ```

3. **Clean up logs**
   ```bash
   # Truncate Docker logs
   sudo truncate -s 0 /var/lib/docker/containers/*/*-json.log
   
   # Or rotate logs
   docker logs producer > /dev/null 2>&1
   ```

4. **Clean up HDFS old data**
   ```bash
   # Remove old processed logs (adjust date as needed)
   docker exec namenode hdfs dfs -rm -r /logs/processed/2026/02/*
   ```

5. **Clean up metrics**
   ```bash
   cd monitoring
   find metrics/ -name "*.json" -mtime +7 -delete
   ```

### Prevention
- Set up disk space alerts at 80%
- Configure log rotation
- Implement data retention policies
- Monitor disk growth trends

---

## SCENARIO 9: Network Partition ⚠️

**Severity:** HIGH (P1)  
**Impact:** Components can't communicate  
**Detection:** Timeouts, connection errors

### Immediate Actions

1. **Identify affected components**
   ```bash
   # Test connectivity between containers
   docker exec producer ping -c 3 broker-1
   docker exec spark-processor ping -c 3 broker-1
   docker exec spark-processor ping -c 3 namenode
   ```

2. **Check Docker network**
   ```bash
   docker network ls
   docker network inspect kafka-cluster-net
   ```

3. **Restart networking**
   ```bash
   # Restart Docker daemon (⚠️ will restart all containers)
   sudo systemctl restart docker
   ```

4. **Or recreate network**
   ```bash
   docker-compose down
   docker network rm kafka-cluster-net
   docker-compose up -d
   ```

---

## 🔄 Post-Recovery Checklist

After resolving any incident:

- [ ] Verify all components running
- [ ] Check consumer lag is decreasing
- [ ] Verify data integrity (no missing blocks)
- [ ] Review logs for related errors
- [ ] Document incident in TESTING_RESULTS.md
- [ ] Update runbook if new issue found
- [ ] Notify team that incident is resolved
- [ ] Schedule post-mortem meeting

---

## 📊 Health Check Command

Run this after any recovery:

```bash
# Quick health check
echo "=== Docker Containers ==="
docker ps --format "table {{.Names}}\t{{.Status}}"

echo -e "\n=== Kafka Health ==="
docker exec broker-1 kafka-topics --bootstrap-server broker-1:29092 --list | grep service-logs && echo "✅ Kafka OK" || echo "❌ Kafka FAIL"

echo -e "\n=== Consumer Lag ==="
docker exec broker-1 kafka-consumer-groups --bootstrap-server broker-1:29092 --describe --group spark-log-processor 2>/dev/null | grep service-logs || echo "No consumer group active"

echo -e "\n=== HDFS Health ==="
docker exec namenode hdfs dfsadmin -report | head -20

echo -e "\n=== Spark Health ==="
docker ps --filter name=spark-processor --format "{{.Status}}" | grep -q "Up" && echo "✅ Spark OK" || echo "❌ Spark DOWN"
```

---

## 📞 Escalation Path

**Level 1 (Self):** Use this playbook  
**Level 2 (Component Owner):** Contact relevant team member  
**Level 3 (Team Lead):** Notify if can't resolve in 30 minutes  
**Level 4 (Management):** Notify if system down > 1 hour

---

**Remember:** Stay calm, follow the playbook, document everything! 🛡️
