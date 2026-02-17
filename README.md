# 📊 Distributed Log Analytics System

A comprehensive distributed system case study implementing various log processing algorithms using Kafka and Spark.

---

## 🗂️ Project Structure

This project is organized into **4 independent folders**, each representing a different component that can be pushed to separate Git branches:

```
distributed-log-analytics/
├── 1-basic-setup/                    (Branch: basic-setup)
├── 2-kafka-producer-consumer/        (Branch: kafka-integration)
├── 3-spark-batch-processing/         (Branch: spark-batch)
├── 4-window-streaming/                (Branch: window-algorithm)
└── README.md
```

---

## 📦 Components Overview

### 1️⃣ Basic Setup (Branch: `basic-setup`)
**Directory:** `1-basic-setup/`

Basic Kafka and Zookeeper infrastructure setup.

**Features:**
- Zookeeper container
- Kafka broker
- Connection test script

**Run:**
```bash
cd 1-basic-setup
docker-compose up -d
python test-connection.py
```

---

### 2️⃣ Kafka Producer-Consumer (Branch: `kafka-integration`)
**Directory:** `2-kafka-producer-consumer/`

**Algorithm:** Producer-Consumer Streaming Model

**Features:**
- Real-time log generation (2 logs/sec)
- Kafka topic-based streaming
- Log categorization (INFO, WARN, ERROR)
- File-based log storage

**Run:**
```bash
cd 2-kafka-producer-consumer
docker-compose up --build
docker-compose logs -f producer
docker-compose logs -f consumer
```

**Check outputs:**
```bash
ls -lh storage/logs/
cat storage/logs/all_logs.txt
```

---

### 3️⃣ Spark Batch Processing (Branch: `spark-batch`)
**Directory:** `3-spark-batch-processing/`

**Algorithm:** Batch Log Analysis with Apache Spark

**Features:**
- Batch analysis on stored logs
- Aggregations by log level
- Service-level analysis
- Error rate calculations

**Run:**
```bash
cd 3-spark-batch-processing
docker-compose up -d
docker-compose exec spark-app spark-submit /app/batch_analysis.py
```

---

### 4️⃣ Window-Based Streaming (Branch: `window-algorithm`)
**Directory:** `4-window-streaming/`

**Algorithm:** Window-Based Stream Processing

**Features:**
- **Tumbling Windows** (1 minute - non-overlapping)
- **Sliding Windows** (2 min window, 30 sec slide - overlapping)
- Real-time error alerts
- Service-level statistics
- Live aggregations

**Run:**
```bash
cd 4-window-streaming
docker-compose up --build
docker-compose logs -f spark-streaming
```

**Access Spark UI:**
```
http://localhost:4040
```

---

## 🚀 Git Branch Strategy

Each folder can be independently committed to its own branch:

```bash
# Initialize repository
git init

# Main branch - project structure
git add README.md
git commit -m "Initial project structure"

# Branch 1: Basic Setup
git checkout -b basic-setup
git add 1-basic-setup/
git commit -m "Add basic Kafka setup"
git push origin basic-setup

# Branch 2: Kafka Integration
git checkout main
git checkout -b kafka-integration
git add 2-kafka-producer-consumer/
git commit -m "Add Kafka producer-consumer streaming"
git push origin kafka-integration

# Branch 3: Spark Batch
git checkout main
git checkout -b spark-batch
git add 3-spark-batch-processing/
git commit -m "Add Spark batch processing"
git push origin spark-batch

# Branch 4: Window Algorithm
git checkout main
git checkout -b window-algorithm
git add 4-window-streaming/
git commit -m "Add window-based streaming algorithm"
git push origin window-algorithm
```

---

## 📋 Prerequisites

- Docker
- Docker Compose
- Python 3.9+ (for test scripts)
- Git

---

## 🎯 Learning Outcomes

1. **Producer-Consumer Pattern** - Asynchronous message streaming
2. **Batch Processing** - Large-scale data analysis with Spark
3. **Stream Processing** - Real-time data pipelines
4. **Window Algorithms** - Tumbling vs Sliding windows
5. **Distributed Systems** - Kafka, Zookeeper, Spark integration

---

## 🛠️ Technology Stack

- **Apache Kafka** - Distributed streaming platform
- **Apache Zookeeper** - Coordination service
- **Apache Spark** - Unified analytics engine
- **Docker** - Containerization
- **Python** - Application logic

---

## 📊 Architecture Diagrams

### Producer-Consumer Model
```
Producer → Kafka Topic → Consumer → File Storage
```

### Window Processing Model
```
Producer → Kafka → Spark Streaming
                    ├── Tumbling Window (1 min)
                    ├── Sliding Window (2 min, 30s slide)
                    ├── Error Alerts
                    └── Statistics
```

---

## ✅ Benefits of This Structure

1. ✅ **Independent Components** - Each folder is self-contained
2. ✅ **Progressive Complexity** - Build from basic to advanced
3. ✅ **Easy Demonstration** - Show each algorithm separately
4. ✅ **Clean Git History** - Focused commits per branch
5. ✅ **Modular Testing** - Test components independently

---

## 📚 Further Enhancements

- Add Elasticsearch for log indexing
- Implement Grafana dashboards
- Add machine learning for anomaly detection
- Implement partitioning and scaling
- Add authentication and security

---

## 📝 License

MIT License - Feel free to use for educational purposes

---

**Created for:** Distributed Systems Case Study  
**Semester:** 6  
**Year:** 2024
