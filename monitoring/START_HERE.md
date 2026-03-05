# 🚀 START HERE - Person 5 Quick Launch

## The Easiest Way to Run and Verify

---

## ⚡ Super Quick Start (3 Steps)

### Step 1: Start Docker Desktop
1. Open **Docker Desktop** application on your Mac
2. Wait for it to start (whale icon appears in menu bar)
3. Make sure it says "Docker Desktop is running"

### Step 2: Run the Startup Script
```bash
cd /Users/priyadarshinianand/Documents/Distributed-Log-Analytics-System/monitoring
bash start_monitoring.sh
```

This script will:
- ✅ Check if Docker is running
- ✅ Start all system components
- ✅ Verify everything is healthy
- ✅ Show you next steps

### Step 3: Start Monitoring
The script will tell you what to do next. Simply follow the instructions!

---

## 📱 Manual Method (If You Prefer)

### Prerequisites:
1. **Docker Desktop MUST be running** ✅

### Then run these commands:

```bash
# 1. Go to project directory
cd /Users/priyadarshinianand/Documents/Distributed-Log-Analytics-System

# 2. Start all services
docker-compose up -d

# 3. Wait 2-3 minutes for containers to be healthy

# 4. Check status
docker ps

# 5. Start health monitor (in a NEW terminal)
cd monitoring
python3 health_monitor.py

# 6. Run a test (in another terminal)
cd monitoring
python3 fault_injector.py --test kafka-broker
```

---

## ✅ How to Verify It's Working

### You'll see:

**1. Health Monitor shows:**
```
✅ All containers running
✅ Kafka brokers: 3/3 healthy
✅ HDFS: 2 DataNodes live
✅ Consumer lag: low (< 500)
```

**2. Test passes:**
```
🎉 TEST PASSED: System survived Kafka broker failure!
```

**3. No errors in terminals**

---

## 🆘 Troubleshooting

### Problem: "Docker is not running"
**Solution:** 
1. Open Docker Desktop app
2. Wait for it to start
3. Try again

### Problem: "Cannot connect to Docker"
**Solution:**
```bash
# Check if Docker is running
docker info

# If not, start Docker Desktop
```

### Problem: Containers not starting
**Solution:**
```bash
# Check what's wrong
docker-compose logs

# Restart everything
docker-compose down
docker-compose up -d
```

### Problem: "Import Error" in Python scripts
**Solution:**
```bash
cd monitoring
pip3 install -r requirements.txt
```

---

## 🎯 Expected Timeline

- **Minute 0:** Start script
- **Minute 1-2:** Containers starting
- **Minute 3:** All containers healthy
- **Minute 4:** Health monitor running
- **Minute 5:** First test complete

**Total time: ~5 minutes** ⏱️

---

## 📞 Quick Help

- **Full guide:** `RUN_AND_VERIFY.md`
- **Setup details:** `QUICK_START.md`
- **Emergency help:** `RECOVERY_PLAYBOOK.md`

---

## ✨ That's It!

The automated script does all the heavy lifting. Just:
1. ✅ Make sure Docker is running
2. ✅ Run the script
3. ✅ Follow the instructions

**You'll be monitoring in 5 minutes! 🛡️**

---

**Ready? Let's go!**

```bash
cd /Users/priyadarshinianand/Documents/Distributed-Log-Analytics-System/monitoring
bash start_monitoring.sh
```
