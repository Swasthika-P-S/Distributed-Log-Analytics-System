# 🚀 Quick Start Guide

## Running Each Component

### 1️⃣ Basic Setup (Test Kafka Connection)
```bash
cd "1-basic-setup"
docker-compose up -d
python test-connection.py
docker-compose down
```

### 2️⃣ Producer-Consumer Streaming
```bash
cd "2-kafka-producer-consumer"
docker-compose up --build

# In another terminal, watch logs:
docker-compose logs -f producer
docker-compose logs -f consumer

# Check output files:
cat storage/logs/all_logs.txt
cat storage/logs/error_logs.txt

# Stop:
docker-compose down
```

### 3️⃣ Spark Batch Processing
```bash
cd "3-spark-batch-processing"
docker-compose up -d

# Wait a few seconds, then run analysis:
docker-compose exec spark-app spark-submit /app/batch_analysis.py

# Stop:
docker-compose down
```

### 4️⃣ Window-Based Streaming
```bash
cd "4-window-streaming"
docker-compose up --build

# In another terminal, watch the streaming output:
docker-compose logs -f spark-streaming

# Open Spark UI in browser:
# http://localhost:4040

# Stop:
docker-compose down
```

---

## 🌿 Git Branch Setup

### Option 1: Automated (Recommended)
```bash
# Windows (PowerShell):
.\setup-git-branches.ps1

# Linux/Mac:
chmod +x setup-git-branches.sh
./setup-git-branches.sh
```

### Option 2: Manual
```bash
# Initialize git
git init

# Main branch
git add README.md QUICK_START.md
git commit -m "Add project documentation"

# Branch 1: basic-setup
git checkout -b basic-setup
git add 1-basic-setup/
git commit -m "Add basic Kafka setup"

# Branch 2: kafka-integration
git checkout main
git checkout -b kafka-integration
git add 2-kafka-producer-consumer/
git commit -m "Add Kafka producer-consumer"

# Branch 3: spark-batch
git checkout main
git checkout -b spark-batch
git add 3-spark-batch-processing/
git commit -m "Add Spark batch processing"

# Branch 4: window-algorithm
git checkout main
git checkout -b window-algorithm
git add 4-window-streaming/
git commit -m "Add window streaming"

# Return to main
git checkout main
```

### Push to Remote
```bash
# Add remote
git remote add origin <your-repo-url>

# Push all branches
git push -u origin main
git push -u origin basic-setup
git push -u origin kafka-integration
git push -u origin spark-batch
git push -u origin window-algorithm
```

---

## 🛠️ Troubleshooting

### Docker Issues
```bash
# Clean up all containers
docker-compose down -v

# Remove all stopped containers and images
docker system prune -a

# Check running containers
docker ps
```

### Kafka Connection Issues
```bash
# Check if Kafka is ready
docker-compose logs kafka | grep "started"

# Check topics
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

### Port Conflicts
If you get port conflicts, make sure no other services are using:
- 2181 (Zookeeper)
- 9092 (Kafka)
- 4040 (Spark UI)

---

## 📊 Expected Output

### Producer-Consumer
- Producer: "✅ Produced 10 logs | Latest: [ERROR] PaymentService"
- Consumer: "📊 Consumed 10 logs | INFO: 6 | WARN: 3 | ERROR: 1"

### Batch Processing
```
📊 ANALYSIS RESULTS
1️⃣ Logs by Level:
+-----+-----+
|level|count|
+-----+-----+
| INFO|    6|
| WARN|    2|
|ERROR|    2|
+-----+-----+
```

### Window Streaming
```
📊 TUMBLING WINDOW (1 minute - non-overlapping)
+------------------------------------------+-----+-----+
|window                                    |level|count|
+------------------------------------------+-----+-----+
|{2024-02-17 09:00:00, 2024-02-17 09:01:00}|INFO |   36|
|{2024-02-17 09:00:00, 2024-02-17 09:01:00}|WARN |   15|
|{2024-02-17 09:00:00, 2024-02-17 09:01:00}|ERROR|    9|
+------------------------------------------+-----+-----+
```

---

## ✅ Verification Checklist

- [ ] All 4 folders created
- [ ] Each folder has README.md
- [ ] Each folder has docker-compose.yml
- [ ] Producer-Consumer runs successfully
- [ ] Batch Processing completes analysis
- [ ] Window Streaming shows real-time output
- [ ] Git branches created
- [ ] All branches pushed to remote

---

**Need Help?** Check the README.md in each folder for detailed instructions!
