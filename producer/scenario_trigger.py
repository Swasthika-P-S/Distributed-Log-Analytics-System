import time
import os
import json
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv
import sys

# Ensure UTF-8 encoding for Windows terminals to handle emojis
if sys.platform == 'win32':
    import codecs
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    else:
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.detach())
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(encoding='utf-8')
    else:
        sys.stderr = codecs.getwriter('utf-8')(sys.stderr.detach())

# Load environment variables from the producer directory
env_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path=env_path)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092,localhost:9093,localhost:9094")
TOPIC = "service-logs"

def get_producer():
    print(f"📡 Connecting to Kafka at: {KAFKA_BROKER}")
    try:
        return KafkaProducer(
            bootstrap_servers=KAFKA_BROKER.split(','),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=(3, 4, 1), # Match the Kafka version in Spark
            request_timeout_ms=10000,
            retries=5
        )
    except Exception as e:
        print(f"❌ Failed to create producer: {e}")
        raise

def send_log(producer, service, level, message, metadata=None):
    log = {
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'service': service,
        'level': level,
        'message': message,
        'metadata': metadata or {}
    }
    producer.send(TOPIC, log)
    print(f"   [SENT] {level} | {service} | {message}")

def scenario_1_error_spike():
    print("\n🚀 TRIGGERING USE CASE 1: Error Spike Detection (PaymentService)")
    producer = get_producer()
    print("Sending spike of 200 errors...")
    for i in range(200):
        send_log(producer, "payment-service", "ERROR", "Transaction Failed: Timeout reaching bank gateway")
        if i % 10 == 0: time.sleep(0.01) # Blast them out fast
    producer.flush()
    print("✅ Burst of 200 errors sent. Watch Spark console for 'SPIKE_DETECTED'!")

def scenario_2_active_service():
    print("\n🚀 TRIGGERING USE CASE 2: Most Active Service Monitoring")
    producer = get_producer()
    print("Sending high volume logs for 'auth-service' (250)...")
    for i in range(250):
        send_log(producer, "auth-service", "INFO", "User login successful", {"user_id": f"user_{i}"})
    
    print("\nSending medium volume logs for 'order-service' (50)...")
    for i in range(50):
        send_log(producer, "order-service", "INFO", "Order created successfully", {"order_id": f"order_{i}"})
    
    producer.flush()
    print("✅ Volume logs sent (250 vs 50). Check Spark trends or HDFS '/logs/processed/service_trends'!")

def scenario_3_security_threat():
    print("\n🚀 TRIGGERING USE CASE 3: Security Threat Detection (Brute Force)")
    producer = get_producer()
    print("Sending brute-force attempt (150 logs)...")
    for i in range(150):
        send_log(producer, "auth-service", "WARN", "Login Failed - user=admin", {"ip": "192.168.1.105"})
        time.sleep(0.01)
    producer.flush()
    print("✅ Brute force attempt simulated. Watch Spark console for 'SECURITY_ALERT' on auth-service!")

if __name__ == "__main__":
    print("Select a scenario to trigger:")
    print("1. Error Spike (PaymentService)")
    print("2. Active Service Monitoring (Auth vs Order)")
    print("3. Security Threat (Brute Force on Admin)")
    
    choice = input("\nEnter choice (1, 2, or 3): ")
    
    if choice == '1':
        scenario_1_error_spike()
    elif choice == '2':
        scenario_2_active_service()
    elif choice == '3':
        scenario_3_security_threat()
    else:
        print("Invalid choice.")
