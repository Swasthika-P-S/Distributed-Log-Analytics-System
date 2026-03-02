# 📝 Project Files Summary

## Total Files Created: 58

---

### Root Docker Stack — 3 Dockerised Systems

| Service | Directory | Role |
|---|---|---|
| **System 1** – Producer | `producer/` | Kafka log event emitter |
| **System 2** – Consumer | `consumer/` | Log receiver + file writer |
| **System 3** – Spark | `spark/` | Batch log analyser |
| Infrastructure | `docker-compose.yml` | Orchestrates all 3 |

---

### Component 4 — Window-based Stream Processing ✅

**Branch:** `window-algorithm`

| Algorithm | Config | Purpose |
|---|---|---|
| Tumbling Window | 1 min, non-overlapping | Per-minute log count per service/level |
| Sliding Window | 2 min / 30 s slide | Rolling avg latency + error rate % |
| Watermark | 30 s tolerance | Late event handling |
| Error Alerts | foreachBatch | Instant alert on every ERROR |

**Files:** `4-window-streaming/` (8 files: producer, spark-streaming, docker-compose, README)  
**Detailed Analysis:** [WINDOW_ALGORITHM_REPORT.md](WINDOW_ALGORITHM_REPORT.md)

---

### Component 5 — Fault Tolerance ✅

**Branch:** `fault-tolerance`

| Layer | Mechanism |
|---|---|
| Kafka Cluster | 3 brokers (kafka-1/2/3), RF=3, minISR=2 |
| Producer | `acks=all`, `enable_idempotence=True`, `retries=10` |
| Consumer Group | 2 instances, manual offset commit, auto-rebalance |
| Dead-Letter Queue | Bad messages → `logs-dlq` → `dlq_handler` |
| Spark Streaming | Checkpoint recovery, `failOnDataLoss=False`, HA brokers |
| Chaos Testing | `chaos_test.py` validates all 4 fault scenarios |

**Files:** `5-fault-tolerance/` (16 files: producer, consumer, dlq_handler, spark-streaming, chaos, docker-compose, README)

---

## 🚀 How to Run

### Stage 4 — Window Streaming
```powershell
cd "d:\sem6\distributed sys\case study\4-window-streaming"
docker-compose up --build
docker-compose logs -f spark-streaming
```

### Stage 5 — Fault Tolerance
```powershell
cd "d:\sem6\distributed sys\case study\5-fault-tolerance"
docker-compose up --build
# Kill a broker to test resilience:
docker stop kafka-2
docker-compose logs -f ft-producer   # should keep sending
docker start kafka-2
```

---

## 🗺️ Git Branches

| Branch | Component |
|---|---|
| `basic-setup` | Infrastructure verification |
| `kafka-integration` | Producer-Consumer model |
| `spark-batch` | Spark batch processing |
| `window-algorithm` | Window-based stream processing |
| `fault-tolerance` | ⭐ Fault tolerance (current stage) |

---

**Status: ✅ Stage 4 (Window Streaming) + Stage 5 (Fault Tolerance) Complete**
