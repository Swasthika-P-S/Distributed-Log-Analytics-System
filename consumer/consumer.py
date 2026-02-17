from kafka import KafkaConsumer
import os
import time

# Wait for Kafka to be ready
time.sleep(15)

# Connect to Kafka
kafka_broker = os.getenv('KAFKA_BROKER', 'localhost:9092')
consumer = KafkaConsumer(
    'logs',
    bootstrap_servers=kafka_broker,
    auto_offset_reset='earliest',
    value_deserializer=lambda m: m.decode('utf-8')
)

print("👂 Consumer started - Listening for logs...")

# Create log files
os.makedirs('/app/logs', exist_ok=True)

# Consume logs
count = 0
for message in consumer:
    log = message.value
    count += 1
    
    print(f"📨 Received ({count}): {log}")
    
    # Save all logs
    with open('/app/logs/all_logs.txt', 'a') as f:
        f.write(log + '\n')
    
    # Save by log level
    if "ERROR" in log:
        with open('/app/logs/error_logs.txt', 'a') as f:
            f.write(log + '\n')
    elif "WARN" in log:
        with open('/app/logs/warn_logs.txt', 'a') as f:
            f.write(log + '\n')
    else:
        with open('/app/logs/info_logs.txt', 'a') as f:
            f.write(log + '\n')

