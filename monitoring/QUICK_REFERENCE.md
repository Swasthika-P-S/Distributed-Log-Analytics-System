# 🛡️ Person 5 Quick Reference Card

## One-Page Cheat Sheet

---

## 🚀 Quick Start Commands

```bash
# Setup (once)
cd monitoring
pip install -r requirements.txt

# Start monitoring (3 terminals)
python health_monitor.py       # Terminal 1
python alert_system.py         # Terminal 2  
python metrics_collector.py    # Terminal 3

# Run tests
python fault_injector.py --test kafka-broker
python fault_injector.py --test hdfs-datanode
python fault_injector.py --test spark
python fault_injector.py --test full
```

---

## 📊 Health Check Commands

```bash
# All containers
docker ps --format "table {{.Names}}\t{{.Status}}"

# Kafka
docker exec broker-1 kafka-topics --bootstrap-server broker-1:29092 --list

# Consumer lag
docker exec broker-1 kafka-consumer-groups --bootstrap-server broker-1:29092 \
  --describe --group spark-log-processor

# HDFS
docker exec namenode hdfs dfsadmin -report

# Spark
docker logs spark-processor --tail 20
```

---

## 🚨 Alert Thresholds

| Metric | WARNING | CRITICAL |
|--------|---------|----------|
| Consumer Lag | 500 | 2000 |
| Kafka Brokers | 2/3 UP | 0-1/3 UP |
| HDFS DataNodes | 1/2 live | 0/2 live |
| Missing Blocks | - | > 0 |
| Disk Usage | 80% | 90% |

---

## 🔧 Quick Fixes

### High Consumer Lag
```bash
docker restart spark-processor
```

### Kafka Broker Down
```bash
docker restart broker-X  # Replace X with 1, 2, or 3
```

### HDFS DataNode Down
```bash
docker restart datanode1  # or datanode2
```

### Spark Not Running
```bash
docker restart spark-processor
docker logs spark-processor --tail 50
```

---

## 📁 Key Files

| File | Purpose |
|------|---------|
| `health_monitor.py` | Real-time dashboard |
| `alert_system.py` | Alert monitoring |
| `fault_injector.py` | Fault testing |
| `RECOVERY_PLAYBOOK.md` | Emergency guide |
| `QUICK_START.md` | Setup guide |
| `alerts.log` | Alert history |

---

## 🧪 Test Results Expected

✅ **Kafka Test:** System continues, no data loss  
✅ **HDFS Test:** Blocks re-replicate automatically  
✅ **Spark Test:** Recovers from checkpoint  
✅ **Full Test:** All above pass  

---

## 📞 Escalation

1. **Level 1:** Use this card + playbook
2. **Level 2:** Contact component owner
3. **Level 3:** Team lead (if >30 min)
4. **Level 4:** Management (if >1 hour)

---

## 🎯 Success Metrics

- Latency: < 10 sec
- Throughput: > 100 msg/sec
- Data Loss: 0%
- Uptime: > 99.9%
- Recovery: < 2 min
- Lag: < 500 msg

---

## 📚 Documentation

- Setup: `QUICK_START.md`
- Emergency: `RECOVERY_PLAYBOOK.md`
- Complete: `README.md`
- Daily: `WALKTHROUGH.md`

---

**Person 5: You're the guardian of system reliability! 🛡️**

Print this card and keep it handy! 📄
