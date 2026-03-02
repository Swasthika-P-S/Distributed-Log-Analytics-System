# 🐳 Docker Compose Commands - Quick Reference

## ⚠️ Common Error Fix

**Wrong:** `docker compose --build`  
**Correct:** `docker-compose up --build` or `docker compose up --build`

The `--build` flag must come with the `up` command!

---

## 🚀 How to Run Each Component

### 1️⃣ Basic Setup (Test Infrastructure)
```bash
cd "1-basic-setup"
docker-compose up -d
python test-connection.py

# Stop:
docker-compose down
```

### 2️⃣ Producer-Consumer Streaming ⭐ RECOMMENDED START
```bash
cd "2-kafka-producer-consumer"
docker-compose up --build

# Watch logs in real-time (open new terminal):
docker-compose logs -f producer
docker-compose logs -f consumer

# Stop (Ctrl+C, then):
docker-compose down
```

### 3️⃣ Spark Batch Processing
```bash
cd "3-spark-batch-processing"
docker-compose up -d --build

# Wait ~10 seconds, then run analysis:
docker-compose exec spark-app spark-submit /app/batch_analysis.py

# Stop:
docker-compose down
```

### 4️⃣ Window-Based Streaming (Advanced)
```bash
cd "4-window-streaming"
docker-compose up --build

# Watch streaming output (open new terminal):
docker-compose logs -f spark-streaming

# Open Spark UI in browser:
# http://localhost:4040

# Stop (Ctrl+C, then):
docker-compose down
```

---

## 🛠️ Common Docker Commands

### View Running Containers
```bash
docker ps
```

### View Logs
```bash
# All services
docker-compose logs

# Specific service
docker-compose logs producer
docker-compose logs consumer

# Follow logs (live)
docker-compose logs -f
```

### Stop Everything
```bash
# Stop (keeps containers)
docker-compose stop

# Stop and remove containers
docker-compose down

# Stop and remove everything including volumes
docker-compose down -v
```

### Rebuild After Changes
```bash
docker-compose up --build
```

### Run in Background (Detached Mode)
```bash
docker-compose up -d
```

---

## 🔍 Troubleshooting

### Port Already in Use
```bash
# Find what's using the port
netstat -ano | findstr :9092
netstat -ano | findstr :2181

# Kill the process (replace PID)
taskkill /PID <PID> /F

# Or stop all Docker containers
docker-compose down
```

### Clean Everything
```bash
# Stop all containers
docker-compose down -v

# Remove unused Docker resources
docker system prune -a

# Remove all stopped containers
docker container prune
```

### View Container Status
```bash
# All containers
docker ps -a

# Container logs
docker logs <container-name>

# Enter container shell
docker exec -it <container-name> bash
```

---

## 📝 Step-by-Step: Running Producer-Consumer

Since you're new to this, here's the easiest component to start with:

**Terminal 1:**
```bash
cd "d:\sem6\distributed sys\case study\2-kafka-producer-consumer"
docker-compose up --build
```

**Terminal 2 (optional - to view logs):**
```bash
cd "d:\sem6\distributed sys\case study\2-kafka-producer-consumer"

# Watch producer
docker-compose logs -f producer

# OR watch consumer
docker-compose logs -f consumer
```

**Terminal 3 (optional - check output files):**
```bash
cd "d:\sem6\distributed sys\case study\2-kafka-producer-consumer"

# View all logs
cat storage/logs/all_logs.txt

# View just errors
cat storage/logs/error_logs.txt

# View warnings
cat storage/logs/warn_logs.txt
```

**To Stop:**
- Press `Ctrl+C` in Terminal 1
- Then run: `docker-compose down`

---

## ✅ Expected Output

When running **Producer-Consumer**, you should see:

**Producer:**
```
🚀 LOG PRODUCER - Producer-Consumer Streaming Model
✅ Connected to Kafka: kafka:29092
📡 Starting log generation...
✅ Produced 10 logs | Latest: [ERROR] PaymentService
✅ Produced 20 logs | Latest: [WARN] AuthService
```

**Consumer:**
```
📥 LOG CONSUMER - Producer-Consumer Streaming Model
✅ Connected to Kafka: kafka:29092
📡 Listening for logs on topic 'logs'...
📊 Consumed 10 logs | INFO: 6 | WARN: 3 | ERROR: 1
📊 Consumed 20 logs | INFO: 12 | WARN: 5 | ERROR: 3
```

---

**Ready to start?** Navigate to a component folder and run `docker-compose up --build`! 🚀
