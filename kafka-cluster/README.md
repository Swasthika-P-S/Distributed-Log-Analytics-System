# Person 2 – Kafka Cluster Manager

> **Pipeline:** Log Producers → **Kafka Cluster** → Spark Streaming → HDFS  
> **Phase 0 Contract**: this component follows the team agreement exactly.

---

## ✅ Phase 0 Contract Values (Person 2's responsibility)

| Setting | Value |
|---------|-------|
| **Bootstrap servers** | `localhost:9092,localhost:9093,localhost:9094` |
| **Topic name** | `service-logs` |
| **Partitions** | `3` (3 Spark tasks in parallel) |
| **Replication factor** | `2` (survive 1 broker failure) |
| **Key strategy** | service name → hash partitioning |
| **Consumer group (Spark)** | `spark-log-processor` |
| **Retention** | 24 hours |
| **Zookeeper** | `localhost:2181` |

---

## 📂 Directory Structure

```
kafka-cluster/
├── docker-compose.yml         ← 3 brokers + Zookeeper  (single command)
├── config/
│   ├── broker-1.properties    ← Broker 1 reference config
│   ├── broker-2.properties    ← Broker 2 reference config
│   └── broker-3.properties    ← Broker 3 reference config
├── scripts/
│   ├── create_topics.sh       ← Creates 'service-logs' topic
│   ├── show_leaders.py        ← Live leader election (every 5s)
│   ├── monitor_cluster.py     ← Health dashboard
│   ├── verify_replication.sh  ← Confirms ISR = replicas
│   ├── test_produce.py        ← Sends Phase 0-schema messages
│   └── test_consume.py        ← Reads + validates schema
├── PARTITION_STRATEGY.md      ← Hash vs Round-Robin analysis
└── README.md                  ← This file
```

---

## 🚀 How to Start the Cluster

### Step 1 – Start containers

```bash
cd kafka-cluster
docker-compose up -d
```

This starts: `zookeeper`, `broker-1` (9092), `broker-2` (9093), `broker-3` (9094)

Check all are healthy:
```bash
docker-compose ps
```
All 4 should show **Up (healthy)**. Wait ~30 seconds after startup.

---

### Step 2 – Create the topic

**Linux / macOS / Git Bash (WSL):**
```bash
bash scripts/create_topics.sh
```

**Windows PowerShell (manual):**
```powershell
docker exec broker-1 kafka-topics `
  --bootstrap-server broker-1:29092 `
  --create --topic service-logs `
  --partitions 3 --replication-factor 2 `
  --config retention.ms=86400000
```

---

### Step 3 – Verify replication

```bash
bash scripts/verify_replication.sh
```

Expected: `✅ REPLICATION HEALTH: PASS`

---

### Step 4 – Test produce & consume

**Windows (PowerShell) — use a virtual environment:**
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install kafka-python==2.0.2

# Send 20 test messages (Phase 0 schema, hash partitioning by service)
python scripts/test_produce.py

# Read messages + validate Phase 0 schema
python scripts/test_consume.py
```

**Linux / WSL / macOS:**
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install kafka-python==2.0.2

python scripts/test_produce.py
python scripts/test_consume.py
```

> ⚠️ On Debian/Ubuntu 23+, bare `pip install` is blocked (PEP 668). Always use a venv.

---

### Step 5 – Live monitoring

```bash
# Live partition leaders (every 5s)
python scripts/show_leaders.py

# Full dashboard: broker status, lag, rates
python scripts/monitor_cluster.py
```

---

## 🔌 Integration Points for Teammates

### For Person 1 (Log Producer)
```python
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="localhost:9092,localhost:9093,localhost:9094",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    acks="all",
)

# Phase 0 schema — must match exactly
event = {
    "timestamp": "2024-01-15T10:30:00Z",   # ISO-8601
    "service":   "web_server",
    "level":     "INFO",
    "message":   "Request processed in 42ms",
    "metadata": {
        "ip":          "192.168.1.10",
        "latency_ms":  42,
        "status_code": 200,
    }
}
producer.send("service-logs", key=b"web_server", value=event)
```

### For Person 3 (Spark Streaming)
```python
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094") \
    .option("subscribe", "service-logs") \
    .option("kafka.group.id", "spark-log-processor") \
    .option("startingOffsets", "latest") \
    .load()
```

### For Person 4 (HDFS Storage)
Kafka does **not** write to HDFS directly. Person 3 (Spark) reads from Kafka and writes to HDFS.
Person 4 only needs to ensure the HDFS paths are ready:

```bash
# HDFS paths Person 3 will write to (Phase 0 contract)
hdfs://namenode:9000/logs/YYYY/MM/DD/<service_name>/

# Example actual paths:
hdfs://namenode:9000/logs/2026/03/03/web_server/
hdfs://namenode:9000/logs/2026/03/03/database/
hdfs://namenode:9000/logs/2026/03/03/auth_service/
```

Files will be written as `.json` or `.parquet` by Spark.

---

## ✅ Live Verification Results (2026-03-03)

Smoke test run against the live cluster — **all Phase 0 checks passed**:

| Check | Result |
|-------|--------|
| Topic `service-logs` created | ✅ |
| Partitions = 3 | ✅ (Leader: 1→P0, 2→P1, 3→P2) |
| Replication factor = 2 | ✅ ISR = Replicas on all partitions |
| Key strategy = service name hash | ✅ Same service → same partition every time |
| 20 messages sent (Phase 0 schema) | ✅ |
| 20 messages consumed & schema valid | ✅ 0 invalid |
| Consumer group `spark-log-processor` | ✅ |

---

## 🛑 Common Commands

| Action | Command |
|--------|---------|
| Stop cluster | `docker-compose down` |
| Stop + wipe data | `docker-compose down -v` |
| Broker logs | `docker-compose logs -f broker-1` |
| List topics | `docker exec broker-1 kafka-topics --bootstrap-server broker-1:29092 --list` |
| Describe topic | `docker exec broker-1 kafka-topics --bootstrap-server broker-1:29092 --describe --topic service-logs` |
| Check consumer lag | `docker exec broker-1 kafka-consumer-groups --bootstrap-server broker-1:29092 --group spark-log-processor --describe` |
| CLI producer | `docker exec -it broker-1 kafka-console-producer --bootstrap-server broker-1:29092 --topic service-logs` |
| CLI consumer | `docker exec broker-1 kafka-console-consumer --bootstrap-server broker-1:29092 --topic service-logs --from-beginning --max-messages 5` |

---

## 🔍 Partition Strategy

See [`PARTITION_STRATEGY.md`](PARTITION_STRATEGY.md) for full trade-off analysis.

**Phase 0 choice: Hash** — key = service name → all logs from same service → same partition.  
Guarantees ordering per service for Spark's stateful aggregations.

---

## 🚧 Troubleshooting

| Problem | Fix |
|---------|-----|
| `NoBrokersAvailable` | Wait 30s after `docker-compose up -d` then retry |
| Port already in use | Stop any local Kafka; only Docker uses 9092-9094 |
| `auto.create.topics.enable=false` | Run `create_topics.sh` before producing |
| Under-replicated partitions | Broker restarted; auto-resync in ~30s |
| `bash` not found (Windows) | Use Git Bash / WSL or run Docker exec command directly |

---

_Phase 0 compliant — Distributed Log Analytics Case Study_
