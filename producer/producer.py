from kafka import KafkaProducer
import random
import time
from datetime import datetime
import os

# Wait for Kafka to be ready
time.sleep(10)

# Connect to Kafka
kafka_broker = os.getenv('KAFKA_BROKER', 'localhost:9092')
producer = KafkaProducer(
    bootstrap_servers=kafka_broker,
    value_serializer=lambda v: v.encode('utf-8')
)

# Log data
log_levels = ["INFO", "WARN", "ERROR"]
services = ["UserService", "PaymentService", "AuthService", "DatabaseService"]
messages = {
    "INFO": ["User logged in", "Request processed", "Transaction completed", "Cache hit"],
    "WARN": ["Slow response time", "High memory usage", "Retry attempt", "Deprecated API call"],
    "ERROR": ["Connection failed", "Null pointer exception", "Timeout error", "Database unavailable"]
}

print("🚀 Producer started - Sending logs to Kafka...")

# Generate and send logs continuously
count = 0
while True:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    level = random.choice(log_levels)
    service = random.choice(services)
    message = random.choice(messages[level])
    
    log = f"{timestamp} | {level} | {service} | {message}"
    
    # Send to Kafka topic 'logs'
    producer.send('logs', log)
    count += 1
    
    print(f"✅ Sent ({count}): {log}")
    
    time.sleep(2)  # Send 1 log every 2 seconds