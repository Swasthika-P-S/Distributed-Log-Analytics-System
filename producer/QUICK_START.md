# 🚀 Quick Start Guide - Person 1 (Kafka Producer)

## What You Need to Do

You are **Person 1** - responsible for the **Kafka Producer System** that generates logs and sends them to Kafka.

## Your Deliverables

✅ **Already Complete:**
1. Log generator simulating 4 services (Web Server, Database, API Gateway, Auth Service)
2. Kafka producer with batching and error handling
3. Configuration files
4. Docker setup

## Running Your System

### Step 1: Test Locally (Week 1)

```bash
# Navigate to the project directory
cd "e:\6th sem\distributed systems\case study\Distributed-Log-Analytics-System"

# Start the entire system (Kafka + Producer)
docker-compose up --build

# In another terminal, watch your producer logs
docker-compose logs -f producer
```

You should see output like:
```
🚀 KAFKA PRODUCER SYSTEM - PERSON 1
✅ Configuration loaded from config.yaml
✅ Kafka producer initialized: kafka:29092
✅ Enabled services: web_server, database, api_gateway, auth_service
📤 [web_server     ] ERROR    | Request failed: /api/users - 500
📤 [database       ] INFO     | SELECT query completed on users (45ms)
📤 [api_gateway    ] WARNING  | Rate limit approaching for auth
```

### Step 2: Verify It's Working

```bash
# Check if Kafka topics are created
docker exec -it distributed-log-analytics-system-kafka-1 kafka-topics --list --bootstrap-server localhost:9092

# You should see:
# logs-web-server
# logs-database
# logs-api-gateway
# logs-auth-service
```

### Step 3: Customize Settings (Optional)

Edit `producer/config.yaml` to change:
- **Log generation rate**: `rate_per_service: 5` (logs per second)
- **Enable/disable services**: Set `enabled: false` for any service
- **Kafka settings**: Update broker addresses for Week 2 integration

### Step 4: Stop the System

```bash
# Press Ctrl+C in the docker-compose terminal
# Or run:
docker-compose down
```

## Week 2: Integration with Person 2

### What Person 2 Needs from You

1. **Your log format** (already documented in PERSON1_README.md)
2. **Topics you're sending to**:
   - `logs-web-server`
   - `logs-database`
   - `logs-api-gateway`
   - `logs-auth-service`

### What You Need from Person 2

1. **Kafka broker IP addresses** (e.g., `192.168.1.100:9092`)
2. **Confirmation that topics are created**
3. **Replication factor** they're using

### Update Your Configuration

Once you get Person 2's Kafka broker IPs:

1. Open `producer/config.yaml`
2. Update the `bootstrap_servers` section:
   ```yaml
   kafka:
     bootstrap_servers:
       - "192.168.1.100:9092"  # Person 2's Kafka broker
   ```
3. Restart your producer

## Troubleshooting

### Problem: "NoBrokersAvailable"
**Solution**: Kafka isn't ready yet. Wait 10-15 seconds and try again.

### Problem: "Connection refused"
**Solution**: 
- Check if Kafka container is running: `docker ps`
- Verify broker address in config.yaml

### Problem: No logs appearing
**Solution**:
- Check if services are enabled in config.yaml
- Verify rate_per_service > 0

## File Structure

```
producer/
├── config.yaml              ← Configuration (edit this!)
├── producer.py              ← Main application
├── log_generator.py         ← Generates realistic logs
├── kafka_producer_wrapper.py ← Kafka integration
├── requirements.txt         ← Python dependencies
├── Dockerfile               ← Docker configuration
├── PERSON1_README.md        ← Detailed documentation
└── QUICK_START.md           ← This file
```

## Key Commands

```bash
# Start everything
docker-compose up --build

# View producer logs
docker-compose logs -f producer

# Stop everything
docker-compose down

# Rebuild after code changes
docker-compose up --build producer

# Check Kafka topics
docker exec -it distributed-log-analytics-system-kafka-1 kafka-topics --list --bootstrap-server localhost:9092
```

## Success Checklist

Week 1 (Individual Setup):
- [ ] Docker and Docker Compose installed
- [ ] Repository cloned
- [ ] `docker-compose up` runs successfully
- [ ] Producer logs show messages being sent
- [ ] Kafka topics are created
- [ ] No error messages in logs

Week 2 (Integration with Person 2):
- [ ] Received Kafka broker IPs from Person 2
- [ ] Updated config.yaml with broker addresses
- [ ] Producer connects to Person 2's Kafka cluster
- [ ] Person 2 confirms receiving logs
- [ ] Logs are distributed across partitions

## Need Help?

1. Check `PERSON1_README.md` for detailed documentation
2. Check main project `README.md` for overall architecture
3. Ask Person 2 about Kafka cluster issues
4. Ask Person 5 for help with testing

---

**Your Role**: Person 1 - Kafka Producer System  
**Next Integration**: Person 2 (Kafka Cluster Manager) in Week 2  
**Status**: ✅ Ready to run!
