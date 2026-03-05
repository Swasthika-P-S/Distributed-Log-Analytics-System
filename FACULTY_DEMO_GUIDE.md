# 🎓 Faculty Presentation Guide: Distributed Log Analytics System

This guide provides a structured flow to demonstrate your implementation to a faculty member. It highlights the **Distributed Systems** concepts you've applied.

---

## 🏗️ 1. Architecture Overview (The Big Picture)
**Explain to Faculty:**
> "We have built a four-tier distributed pipeline:
> 1. **Producer**: Generates high-volume system logs simulating a real-world microservices environment.
> 2. **Kafka Cluster**: A fault-tolerant message bus with **3 brokers** to ensure no data loss if one node fails.
> 3. **Spark Streaming**: Performs real-time windowed analytics (Error counts, trends, and anomaly detection).
> 4. **HDFS**: A distributed file system that stores the final processed results across multiple DataNodes."

---

## 📤 2. Demonstrate Log Generation (The Source)
**Command to Run:**
```powershell
docker-compose logs -f producer
```
**What to Point Out:**
- Show the different "services" (`auth_service`, `db_server`, etc.) generating logs.
- Point out the log levels (`INFO`, `WARNING`, `ERROR`).
- **Concept**: High-throughput data ingestion.

---

## 📡 3. Demonstrate Kafka Cluster (The Backbone)
**Command to Run:**
```powershell
# Show that the topic is distributed
docker exec broker-1 kafka-topics --bootstrap-server broker-1:29092 --describe --topic service-logs
```
**What to Point Out:**
- Show the **3 Partitions** and **Replication Factor of 2**.
- Explain that even if one broker goes down, the logs are still available on the others.
- **Concept**: Replication and Partitioning for Fault Tolerance.

---

## ⚡ 4. Demonstrate Spark Analytics (The Engine)
**Command to Run:**
```powershell
docker-compose logs -f spark
```
**What to Point Out:**
- Look for the line: `✅ Analytics Pipelines active. Monitoring logs...`
- Explain that Spark is processing logs in **Time Windows** (e.g., counting errors every minute).
- Mention **Anomaly Detection**: If a service produces too many errors, Spark flags it immediately.
- **Concept**: Real-time Stream Processing.

---

## 💾 5. Demonstrate HDFS Storage (The Persistence)
**Command to Run:**
```powershell
# List the processed results in HDFS
docker exec namenode hdfs dfs -ls -R /logs/processed/
```
**What to Point Out:**
- Show the three folders: `anomalies`, `error_metrics`, and `service_trends`.
- Explain that these are stored as **Parquet/JSON** files across multiple DataNodes.
- **Concept**: Distributed Storage and Data Durability.

---

## 🎯 6. Real-World Use Case Demonstrations (Scenario Mode)
**Command to Run (in a new terminal):**
```powershell
# This script lets you choose a scenario to "Inject" into the system
python producer\scenario_trigger.py
```

### **Use Case 1: Error Spike Detection**
- **Trigger**: Select **Option 1** in the script.
- **Monitor**: Watch the `spark-processor` logs (cmd: `docker-compose logs -f spark`).
- **Explanation**: Spark detects > 20 errors/min and triggers a `SPIKE_DETECTED` alert in the console.

### **Use Case 2: Most Active Service**
- **Trigger**: Select **Option 2** (Sends 100 logs for Auth, 40 for Order).
- **Monitor**:
    ```powershell
    docker exec namenode hdfs dfs -tail /logs/processed/service_trends/part...json
    ```
- **Explanation**: HDFS storage updates to show that `auth-service` is the high-load bottleneck.

### **Use Case 3: Security Threat (Brute Force)**
- **Trigger**: Select **Option 3** (Simulates 40 failed login attempts on 'admin').
- **Monitor**: Watch for a spike alert in Spark specifically for `auth-service`.
- **Explanation**: Demonstrates how real-time monitoring can catch potential attacks before they scale.

---

## 🛡️ 7. Demonstrate Fault Tolerance & Monitoring (The Guardian)
**Command to Run (in a new terminal):**
```powershell
cd monitoring
python health_monitor.py
```
**What to Point Out:**
- Show the dashboard displaying the status of **Kafka**, **Spark**, and **HDFS**.
- Explain that this system detects failures and can trigger **Alerts** based on thresholds (e.g., if a broker goes down).
- Mention that this is the final piece of the 5-person team, ensuring the high availability of the entire pipeline.
- **Concept**: Distributed System Reliability and Observability.

---

## 🛠️ 7. Advanced Question: "How would this scale?"
**Your Answer:**
- "Because we use **Kafka Partitions**, we can add more Spark workers to process logs in parallel."
- "Because we use **HDFS**, we can add more DataNodes to increase storage capacity without downtime."
- "We used **ZeroTier** to simulate this across different laptops, proving the networking works over a WAN/VPN."

---

### 🛑 To Reset for the next demo:
```powershell
docker-compose down -v  # The -v removes old data/volumes
docker-compose up -d
```
