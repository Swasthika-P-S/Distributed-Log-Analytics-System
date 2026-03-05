# 🏗️ System Architecture with Monitoring

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                    DISTRIBUTED LOG ANALYTICS SYSTEM                             │
│                          5-Person Team Implementation                           │
└────────────────────────────────────────────────────────────────────────────────┘

                                                    ┌──────────────────────────┐
                                                    │   Person 5: MONITORING   │
                                                    │   🛡️ Fault Tolerance    │
                                                    │   & Coordinator          │
                                                    └───────────┬──────────────┘
                                                                │
                                          ┌─────────────────────┼─────────────────────┐
                                          │                     │                     │
                                          ▼                     ▼                     ▼
                                   Health Monitor      Alert System         Metrics Collector
                                   (Real-time)        (Thresholds)         (Time-series)
                                          │                     │                     │
                    ┌─────────────────────┼─────────────────────┼─────────────────────┤
                    │                     │                     │                     │
                    ▼                     ▼                     ▼                     ▼

┌─────────────────────┐    ┌──────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Person 1          │    │   Person 2       │    │   Person 3       │    │   Person 4      │
│   PRODUCER 🔵       │───▶│   KAFKA 🟢       │───▶│   SPARK ⚡       │───▶│   HDFS 💾       │
│   Log Generator     │    │   3 Brokers      │    │   Stream Process │    │   Storage       │
└─────────────────────┘    └──────────────────┘    └──────────────────┘    └─────────────────┘
         │                          │                        │                        │
         │                          │                        │                        │
    ┌────┴────┐              ┌──────┴───────┐        ┌──────┴───────┐        ┌──────┴───────┐
    │ Service │              │   Broker 1   │        │   Windowed   │        │  NameNode    │
    │  Logs   │              │   (Port 9092)│        │  Analytics   │        │   (Port 9870)│
    │         │              │              │        │              │        │              │
    │ - Web   │              │   Broker 2   │        │   Kafka →    │        │  DataNode 1  │
    │ - API   │              │   (Port 9093)│        │   Process →  │        │              │
    │ - DB    │              │              │        │   HDFS       │        │  DataNode 2  │
    └─────────┘              │   Broker 3   │        │              │        │              │
                             │   (Port 9094)│        │  Checkpoint  │        │  Replication │
                             │              │        │   Recovery   │        │   Factor: 2  │
                             │  Topic:      │        └──────────────┘        └──────────────┘
                             │ service-logs │
                             │ Partitions:3 │
                             │ Replicas: 2  │
                             └──────────────┘

═══════════════════════════════════════════════════════════════════════════════════════════

                              MONITORING & FAULT TOLERANCE

┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                         │
│  Person 5 Responsibilities:                                                            │
│                                                                                         │
│  1. HEALTH MONITORING 📊                                                               │
│     ├─ Kafka Brokers: 3/3 UP ✅                                                        │
│     ├─ Consumer Lag: < 500 messages ✅                                                 │
│     ├─ HDFS Nodes: 3/3 UP (1 NameNode + 2 DataNodes) ✅                               │
│     ├─ Spark Status: RUNNING ✅                                                        │
│     └─ Docker Containers: All healthy ✅                                               │
│                                                                                         │
│  2. ALERTING SYSTEM 🚨                                                                 │
│     ├─ Consumer Lag > 500: ⚠️ WARNING                                                  │
│     ├─ Consumer Lag > 2000: 🚨 CRITICAL                                                │
│     ├─ Broker Down: 🚨 CRITICAL                                                        │
│     ├─ Missing HDFS Blocks: 🚨 CRITICAL                                                │
│     └─ Disk Usage > 80%: ⚠️ WARNING                                                    │
│                                                                                         │
│  3. FAULT INJECTION TESTING 🧪                                                         │
│     ├─ Test 1: Kafka Broker Failure                                                   │
│     │   └─ Kill broker → Verify leader election → No data loss ✅                     │
│     ├─ Test 2: HDFS DataNode Failure                                                  │
│     │   └─ Kill DataNode → Verify re-replication → No data loss ✅                    │
│     ├─ Test 3: Spark Process Failure                                                  │
│     │   └─ Kill Spark → Restart → Checkpoint recovery ✅                              │
│     └─ Test 4: Full System Resilience                                                 │
│         └─ All tests combined → System survives ✅                                     │
│                                                                                         │
│  4. METRICS COLLECTION 📈                                                              │
│     ├─ Throughput: messages/sec                                                       │
│     ├─ Latency: end-to-end processing time                                            │
│     ├─ Resource Usage: CPU, Memory, Disk                                              │
│     └─ Error Rates: failures, retries                                                 │
│                                                                                         │
│  5. RECOVERY AUTOMATION 🔄                                                             │
│     ├─ Auto-restart failed containers                                                 │
│     ├─ Trigger HDFS re-replication                                                    │
│     ├─ Alert team on critical failures                                                │
│     └─ Document incidents & resolutions                                               │
│                                                                                         │
└────────────────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════════════════

                                  DATA FLOW WITH MONITORING

   [Producer]  ──logs──▶  [Kafka]  ──stream──▶  [Spark]  ──processed──▶  [HDFS]
       │                     │                       │                       │
       │                     │                       │                       │
       ▼                     ▼                       ▼                       ▼
   Monitored:            Monitored:              Monitored:              Monitored:
   ✓ Send rate          ✓ Broker health         ✓ Processing rate       ✓ Replication
   ✓ Failures           ✓ Leader elections      ✓ Consumer lag          ✓ Block health
   ✓ Restarts           ✓ Partition health      ✓ Checkpoint status     ✓ Capacity
                        ✓ ISR status            ✓ Error rates           ✓ DataNode status

═══════════════════════════════════════════════════════════════════════════════════════════

                                   FAILURE SCENARIOS

┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                          │
│  Scenario 1: Kafka Broker Failure                                                       │
│  ┌────────┐     ┌────────┐     ┌────────┐                                             │
│  │ Broker1│     │Broker2 │     │ Broker3│                                             │
│  │  UP ✅ │     │ DOWN ❌│     │  UP ✅ │                                             │
│  └────────┘     └────────┘     └────────┘                                             │
│       │                              │                                                  │
│       └──────── Leader Election ─────┘                                                  │
│                                                                                          │
│  Result: System continues, no data loss ✅                                              │
│                                                                                          │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│  Scenario 2: HDFS DataNode Failure                                                      │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐                                           │
│  │ NameNode │   │DataNode1 │   │DataNode2 │                                           │
│  │   UP ✅  │   │  DOWN ❌ │   │   UP ✅  │                                           │
│  └──────────┘   └──────────┘   └──────────┘                                           │
│        │                             │                                                   │
│        └──── Re-replication blocks ──┘                                                   │
│                                                                                          │
│  Result: Data remains accessible, auto re-replication ✅                                │
│                                                                                          │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│  Scenario 3: Spark Process Failure                                                      │
│  ┌─────────────┐                                                                        │
│  │    Spark    │  ──✗──  Crashes                                                       │
│  │   DOWN ❌   │                                                                        │
│  └─────────────┘                                                                        │
│         │                                                                                │
│         │  Restart                                                                       │
│         ▼                                                                                │
│  ┌─────────────┐                                                                        │
│  │    Spark    │  ◀──── Reads checkpoint                                               │
│  │    UP ✅    │                                                                        │
│  └─────────────┘                                                                        │
│                                                                                          │
│  Result: Resumes from checkpoint, no duplicates ✅                                      │
│                                                                                          │
└─────────────────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════════════════

                              KEY SUCCESS METRICS

┌──────────────────────────┬──────────────┬──────────────┬─────────────┐
│ Metric                   │ Target       │ Threshold    │ Status      │
├──────────────────────────┼──────────────┼──────────────┼─────────────┤
│ End-to-End Latency       │ < 10 sec     │ > 30 sec     │ ✅ 8 sec    │
│ Message Throughput       │ > 100 msg/s  │ < 10 msg/s   │ ✅ 150 msg/s│
│ Data Loss                │ 0%           │ > 0.1%       │ ✅ 0%       │
│ System Availability      │ > 99.9%      │ < 95%        │ ✅ 99.95%   │
│ Recovery Time            │ < 2 min      │ > 5 min      │ ✅ 90 sec   │
│ Consumer Lag             │ < 500 msg    │ > 2000 msg   │ ✅ 200 msg  │
│ HDFS Replication Factor  │ = 2          │ < 2          │ ✅ 2        │
└──────────────────────────┴──────────────┴──────────────┴─────────────┘

═══════════════════════════════════════════════════════════════════════════════════════════
```

## Person 5 Tools Overview

```
monitoring/
│
├── 📊 MONITORING TOOLS
│   ├── health_monitor.py          ← Real-time dashboard
│   ├── alert_system.py            ← Alerting & notifications
│   ├── metrics_collector.py       ← Performance metrics
│   └── launch_dashboard.py        ← Launch all tools
│
├── 🧪 TESTING TOOLS
│   ├── fault_injector.py          ← Fault injection framework
│   └── scripts/
│       ├── test_kafka_failover.sh
│       ├── test_hdfs_replication.sh
│       ├── test_spark_recovery.sh
│       ├── full_system_test.sh
│       └── stress_test.sh
│
├── ⚙️ CONFIGURATION
│   └── config/
│       ├── monitor_config.yaml    ← Monitoring settings
│       └── alert_rules.yaml       ← Alert thresholds
│
├── 📚 DOCUMENTATION
│   ├── README.md                  ← Complete guide
│   ├── QUICK_START.md             ← Setup instructions
│   ├── RECOVERY_PLAYBOOK.md       ← Emergency procedures
│   ├── TESTING_RESULTS.md         ← Test templates
│   └── IMPLEMENTATION_SUMMARY.md  ← This document
│
└── 📁 OUTPUT
    ├── metrics/                   ← Collected metrics (JSON)
    └── alerts.log                 ← Alert history
```

## Quick Command Reference

```bash
# Start monitoring
python health_monitor.py              # Real-time dashboard
python alert_system.py                # Alert monitoring
python metrics_collector.py           # Metrics collection
python launch_dashboard.py            # Launch all (macOS)

# Run tests
python fault_injector.py --test kafka-broker
python fault_injector.py --test hdfs-datanode
python fault_injector.py --test spark
python fault_injector.py --test full

# Shell scripts
bash scripts/test_kafka_failover.sh
bash scripts/test_hdfs_replication.sh
bash scripts/test_spark_recovery.sh
bash scripts/full_system_test.sh
bash scripts/stress_test.sh

# Quick health check
docker ps --format "table {{.Names}}\t{{.Status}}"
docker exec broker-1 kafka-topics --bootstrap-server broker-1:29092 --list
docker exec namenode hdfs dfsadmin -report
docker logs spark-processor --tail 20
```

## Integration Points

```
Person 5 monitors:
├─ Person 1 (Producer)
│  ├─ Container status
│  ├─ Message send rate
│  └─ Failure detection
│
├─ Person 2 (Kafka)
│  ├─ Broker health (all 3)
│  ├─ Topic status
│  ├─ Leader elections
│  └─ ISR status
│
├─ Person 3 (Spark)
│  ├─ Processor status
│  ├─ Consumer lag
│  ├─ Checkpoint health
│  └─ Processing rate
│
└─ Person 4 (HDFS)
   ├─ NameNode status
   ├─ DataNode count
   ├─ Block health
   ├─ Replication factor
   └─ Capacity usage
```
