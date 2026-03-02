# Fault Tolerance — Stage 5

> **Branch:** `fault-tolerance`  
> **Component:** 5 of the Distributed Log Analytics System

## 🧠 Fault Tolerance Mechanisms

### 1. Kafka — 3-Broker Cluster with Replication

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│   kafka-1    │    │   kafka-2    │    │   kafka-3    │
│ (broker id=1)│    │ (broker id=2)│    │ (broker id=3)│
└──────┬───────┘    └──────┬───────┘    └──────┬───────┘
       │                   │                   │
       └───────────────────┼───────────────────┘
                           │
                     Replication Factor = 3
                     min.insync.replicas = 2
                     (writes require 2/3 brokers alive)
```
- Topic `logs` — 6 partitions, RF=3, minISR=2
- Topic `logs-dlq` — 3 partitions, RF=3
- **Cluster survives loss of 1 broker** (2 of 3 still in ISR)

---

### 2. Producer — Exactly-Once Delivery

| Setting | Value | Effect |
|---|---|---|
| `acks` | `'all'` | Write confirmed by ALL ISR replicas |
| `enable_idempotence` | `True` | No duplicate messages on retry |
| `retries` | `10` | Auto-retry transient failures |
| `retry_backoff_ms` | `500` | 500 ms between retries |
| `max_in_flight_requests` | `1` | Preserve message order on retry |

---

### 3. Consumer — At-Least-Once with Auto-Failover

```
Consumer Group: ft-log-consumers
   ├── ft-consumer-1  (partitions 0, 1, 2)
   └── ft-consumer-2  (partitions 3, 4, 5)

If ft-consumer-1 dies:
   └── ft-consumer-2 rebalances → takes ALL 6 partitions
```

- `enable_auto_commit=False` — offset committed only AFTER successful disk write
- `session_timeout_ms=10000` — dead member detected within 10 s
- Supports processing exactly where it left off after restart

---

### 4. Dead-Letter Queue (DLQ)

```
Kafka topic: logs
   ↓ Consumer parses message
   ↓ [FAIL: bad JSON / missing fields]
   → Kafka topic: logs-dlq
   → dlq_handler writes to /dlq/dlq_events.log
```
- No messages are **silently dropped** — every failure is traceable
- DLQ dashboard shows failure reason breakdown

---

### 5. Spark Streaming — Checkpoint Recovery

```
Container restart:
  ft-spark-streaming restarts
       ↓
  Reads /tmp/checkpoints/ (mounted volume)
       ↓
  Resumes from last committed Kafka offset
       ↓
  No data loss, no duplicate processing window starts
```
- `failOnDataLoss=False` — survives broker restarts / offset gaps
- 3 separate checkpoint locations: `ft_alerts`, `ft_tumbling`, `ft_sliding`

---

## 📦 Services

| Service | Replicas | Role |
|---|---|---|
| `zookeeper-ft` | 1 | Kafka co-ordinator |
| `kafka-1`, `kafka-2`, `kafka-3` | 3 | Kafka cluster (RF=3) |
| `kafka-ft-init` | 1 (one-shot) | Creates topics |
| `ft-producer` | 1 | Resilient log emitter |
| `ft-consumer-1`, `ft-consumer-2` | 2 | Consumer group |
| `dlq-handler` | 1 | Dead-Letter Queue monitor |
| `ft-spark` | 1 | Fault-tolerant Spark streaming |

---

## 🚀 Quick Start

```powershell
cd "d:\sem6\distributed sys\case study\5-fault-tolerance"

# Build and start full fault-tolerant stack
docker-compose up --build

# Watch specific service logs
docker-compose logs -f ft-producer
docker-compose logs -f ft-consumer-1
docker-compose logs -f dlq-handler
docker-compose logs -f ft-spark

# Stop everything
docker-compose down -v
```

---

## 🔥 Fault Simulation Tests

### Test 1 — Kill a Kafka Broker
```powershell
docker stop kafka-2
# Producer should auto-reconnect to kafka-1 and kafka-3
# Watch: docker-compose logs -f ft-producer
docker start kafka-2   # restore
```

### Test 2 — Kill a Consumer Instance
```powershell
docker stop ft-consumer-1
# ft-consumer-2 rebalances and takes over all partitions
# Watch: docker-compose logs -f ft-consumer-2
docker start ft-consumer-1  # restore, group rebalances again
```

### Test 3 — Restart Spark (Checkpoint Recovery)
```powershell
docker restart ft-spark
# Spark resumes from checkpoints — check logs for "Resuming from checkpoint"
docker-compose logs -f ft-spark
```

### Test 4 — Inject Bad Message → DLQ
```powershell
# Run chaos test script (from within the repo)
docker run --rm --network 5-fault-tolerance_default `
  -e KAFKA_BROKERS="kafka-1:29092,kafka-2:29093,kafka-3:29094" `
  ft-consumer python chaos/chaos_test.py
# Watch DLQ: docker-compose logs -f dlq-handler
```

---

## 📁 File Structure

```
5-fault-tolerance/
├── docker-compose.yml              ← 3-broker cluster + all services
├── README.md                       ← This file
├── producer/
│   ├── ft_producer.py              ← acks=all, idempotent, retries
│   ├── Dockerfile
│   └── requirements.txt
├── consumer/
│   ├── ft_consumer.py              ← consumer group, manual commit, DLQ
│   ├── Dockerfile
│   └── requirements.txt
├── dlq_handler/
│   ├── dlq_handler.py              ← DLQ monitor + dashboard
│   ├── Dockerfile
│   └── requirements.txt
├── spark-streaming/
│   ├── ft_window_processing.py     ← Checkpoint recovery + HA brokers
│   ├── Dockerfile
│   └── requirements.txt
└── chaos/
    ├── chaos_test.py               ← Fault simulation suite
    └── requirements.txt
```
