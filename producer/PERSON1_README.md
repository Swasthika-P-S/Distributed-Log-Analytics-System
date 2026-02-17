# 🔵 Person 1: Kafka Producer System

## Overview
This is the **Kafka Producer System** component for the distributed log analytics case study. As Person 1, you're responsible for generating realistic logs from multiple services and sending them to Kafka.

## Features

### ✅ Implemented
- **Multiple Service Simulators**: Web Server, Database, API Gateway, Auth Service
- **Realistic Log Generation**: Using Faker library for authentic data
- **Structured JSON Logs**: Timestamp, level, service, message, metadata
- **Kafka Producer with Batching**: Efficient message sending
- **Configurable Settings**: YAML-based configuration
- **Error Handling & Retries**: Robust error management
- **Statistics Tracking**: Real-time metrics on logs sent
- **Graceful Shutdown**: Proper cleanup on exit

### 📊 Log Levels
- **DEBUG** (10%): Detailed debugging information
- **INFO** (60%): General informational messages
- **WARNING** (20%): Warning messages
- **ERROR** (8%): Error messages
- **CRITICAL** (2%): Critical system failures

## Project Structure

```
producer/
├── config.yaml                    # Configuration file
├── producer.py                    # Main application
├── log_generator.py               # Log generation logic
├── kafka_producer_wrapper.py      # Kafka producer wrapper
├── requirements.txt               # Python dependencies
├── Dockerfile                     # Docker configuration
└── README.md                      # This file
```

## Configuration

Edit `config.yaml` to customize:

### Kafka Settings
```yaml
kafka:
  bootstrap_servers:
    - "kafka:29092"  # Update with Person 2's Kafka broker IPs
  topics:
    web_server: "logs-web-server"
    database: "logs-database"
    # ... more topics
```

### Producer Settings
```yaml
kafka:
  producer_config:
    acks: "all"              # Wait for all replicas
    retries: 3               # Retry failed sends
    batch_size: 16384        # 16KB batches
    linger_ms: 10            # Wait 10ms to batch
    compression_type: "gzip" # Compress messages
```

### Log Generation
```yaml
log_generation:
  rate_per_service: 5  # Logs per second per service
```

## Running the Producer

### Option 1: Docker (Recommended)
```bash
# From the main project directory
docker-compose up --build producer

# View logs
docker-compose logs -f producer
```

### Option 2: Local Python
```bash
cd producer

# Install dependencies
pip install -r requirements.txt

# Run the producer
python producer.py
```

## Integration with Person 2 (Kafka Cluster)

### Week 2 Integration Steps

1. **Get Kafka Broker IPs from Person 2**
   - Ask Person 2 for their Kafka broker addresses
   - Example: `192.168.1.100:9092`, `192.168.1.101:9092`

2. **Update Configuration**
   ```yaml
   kafka:
     bootstrap_servers:
       - "192.168.1.100:9092"
       - "192.168.1.101:9092"
   ```

3. **Test Connection**
   ```bash
   # Person 2 should verify topics are created
   # You should see logs being sent successfully
   ```

4. **Verify Data Flow**
   - Person 2 checks Kafka topics for incoming messages
   - Verify partition distribution
   - Check replication status

## Sample Log Output

### Web Server Log
```json
{
  "timestamp": "2026-02-17T15:22:31.123Z",
  "level": "ERROR",
  "service": "web-server",
  "message": "Request failed: /api/users - 500",
  "metadata": {
    "endpoint": "/api/users",
    "status_code": 500,
    "latency_ms": 1234,
    "request_id": "abc-123-def",
    "user_id": "user-456",
    "ip_address": "192.168.1.50"
  }
}
```

### Database Log
```json
{
  "timestamp": "2026-02-17T15:22:32.456Z",
  "level": "WARNING",
  "service": "database",
  "message": "Slow query detected: SELECT on users - 450ms",
  "metadata": {
    "operation": "SELECT",
    "table": "users",
    "execution_time_ms": 450,
    "connection_pool_size": 15
  }
}
```

## Statistics Output

Every 30 seconds, you'll see statistics like:
```
======================================================================
📊 PRODUCER STATISTICS
======================================================================
Total Sent: 1500 | Failed: 2

By Service:
  web_server           :    600 logs
  database             :    300 logs
  api_gateway          :    450 logs
  auth_service         :    150 logs

By Level:
  DEBUG      :    150 logs
  INFO       :    900 logs
  WARNING    :    300 logs
  ERROR      :    120 logs
  CRITICAL   :     30 logs
======================================================================
```

## Troubleshooting

### Connection Errors
```
❌ Failed to initialize Kafka producer: NoBrokersAvailable
```
**Solution**: 
- Check if Kafka is running
- Verify broker addresses in `config.yaml`
- Ensure network connectivity to Person 2's Kafka cluster

### Slow Performance
```
⚠️ Producer lag detected
```
**Solution**:
- Increase `batch_size` in config
- Reduce `rate_per_service`
- Check network bandwidth

## Testing Checklist

- [ ] Producer connects to Kafka successfully
- [ ] Logs are generated for all enabled services
- [ ] Messages appear in Kafka topics (verify with Person 2)
- [ ] Statistics show successful sends
- [ ] Graceful shutdown works (Ctrl+C)
- [ ] Configuration changes take effect

## Key Metrics

### Success Criteria
✅ Connection to Kafka cluster established  
✅ Logs sent at configured rate (default: 20 logs/sec total)  
✅ All logs properly formatted as JSON  
✅ Batching reduces network calls by >80%  
✅ Zero data loss with `acks=all`  
✅ Graceful shutdown with no message loss  

### Performance Targets
- **Throughput**: 100-1000 logs/second
- **Latency**: < 100ms per batch
- **Success Rate**: > 99.9%
- **Batch Efficiency**: > 80% reduction in network calls

## Next Steps

1. ✅ **Week 1**: Set up and test locally with Docker
2. 🔄 **Week 2**: Integrate with Person 2's Kafka cluster
3. ⏳ **Week 3**: Full pipeline testing
4. ⏳ **Week 4**: Fault tolerance testing with Person 5

## Contact Points

**Integration with:**
- **Person 2** (Kafka Cluster Manager): Provide broker IPs, verify topics
- **Person 5** (Fault Tolerance Coordinator): Participate in failure testing

## Additional Resources

- [Kafka Producer Documentation](https://kafka.apache.org/documentation/#producerapi)
- [kafka-python Library](https://kafka-python.readthedocs.io/)
- Project main README: `../README.md`

---

**Role**: Person 1 - Kafka Producer System  
**Algorithm**: Producer-Consumer Model (Producer Side)  
**Status**: ✅ Ready for Week 2 Integration
