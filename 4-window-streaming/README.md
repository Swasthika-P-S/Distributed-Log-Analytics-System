# Window-based Stream Processing Algorithm

> **Branch:** `window-algorithm`  
> **Component:** 4 of the Distributed Log Analytics System

## 🧠 Algorithm Explained

### What is Window-based Stream Processing?

Stream processing algorithms divide a continuous, unbounded data stream into **finite chunks called windows**, then compute aggregations over each window. This lets us answer questions like:

- *"How many ERRORs occurred in the last 1 minute?"*
- *"What is the rolling average latency over the past 2 minutes?"*

---

### Window Types Implemented

#### 1. Tumbling Window (1 minute, non-overlapping)

```
Time:   0──────60s──────120s──────180s
Win 1:  [████████████]
Win 2:               [████████████]
Win 3:                            [████████████]
```
- Each event belongs to **exactly one** window
- Windows do not overlap
- Used for: discrete period summaries (per-minute log counts per service/level)

#### 2. Sliding Window (2 min window, 30-sec slide)

```
Time:   0───30s───60s───90s───120s
Win 1:  [████████████████████]        (0 → 120s)
Win 2:       [████████████████████]   (30s → 150s)
Win 3:            [████████████████████] (60s → 180s)
```
- Windows **overlap** – each event can appear in multiple windows
- Slide interval (30 s) < window size (2 min)
- Used for: smooth rolling trends (avg latency, error rate %)

#### 3. Watermark (Late Data Tolerance)
- Watermark of **30 seconds** is applied before all window operations
- Events arriving up to 30 s late are still included in their correct window
- Events older than the watermark are dropped gracefully

---

## 📦 Services

| Service | Description |
|---------|-------------|
| `zookeeper` | Kafka co-ordinator |
| `kafka` | Message broker (topic: `window-logs`) |
| `kafka-init` | Creates the Kafka topic on startup |
| `window-producer` | Sends 1 structured JSON log/sec to Kafka |
| `spark-streaming` | PySpark Structured Streaming — runs all 3 window queries |

---

## 🚀 Quick Start

```powershell
# Navigate to this directory
cd "d:\sem6\distributed sys\case study\4-window-streaming"

# Build and start all services
docker-compose up --build

# View logs from the spark job only
docker-compose logs -f spark-streaming

# Stop everything
docker-compose down -v
```

---

## 📊 Sample Output

**Tumbling Window (every 30 s):**
```
══════════════════════════════════════════════════════════════════════
🪟  TUMBLING WINDOW (1 min)  —  batch 3
══════════════════════════════════════════════════════════════════════
  Window Start           End                    Level  Service              Count  Avg ms
  2026-03-02 12:00:00    2026-03-02 12:01:00   🔴ERROR DatabaseService       4    812.3
  2026-03-02 12:00:00    2026-03-02 12:01:00   🟢INFO  UserService          36     52.1
  2026-03-02 12:00:00    2026-03-02 12:01:00   🟡WARN  PaymentService        9    203.7
```

**Sliding Window (every 30 s):**
```
══════════════════════════════════════════════════════════════════════
📊  SLIDING WINDOW (2 min / 30 s slide)  —  batch 3
══════════════════════════════════════════════════════════════════════
  Window Start           Service              Events  AvgMs  Errors   Err%
  2026-03-02 11:59:00    AuthService              71   98.4       9   12.7%  █
  2026-03-02 11:59:00    DatabaseService          68  142.1      11   16.2%  █
```

**Real-time ERROR Alert:**
```
🚨  ALERT  [batch=12]  2026-03-02T12:01:15Z  |  DatabaseService      |  1024ms  |  Database unavailable
```

---

## 🔧 Configuration

| Env Variable | Default | Description |
|---|---|---|
| `KAFKA_BROKER` | `localhost:9092` | Kafka bootstrap server |
| `KAFKA_TOPIC`  | `window-logs`    | Topic to read from |

---

## 📁 File Structure

```
4-window-streaming/
├── docker-compose.yml           ← Orchestrates all services
├── README.md                    ← This file
├── producer/
│   ├── producer.py              ← JSON log emitter (1/sec)
│   ├── Dockerfile
│   └── requirements.txt
└── spark-streaming/
    ├── window_processing.py     ← ⭐ Core window algorithm
    ├── Dockerfile
    └── requirements.txt
```
