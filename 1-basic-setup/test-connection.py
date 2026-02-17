from kafka import KafkaProducer, KafkaConsumer
import time

print("Testing Kafka Connection...")

try:
    # Test Producer
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    print("✅ Producer connected successfully!")
    
    # Test Consumer
    consumer = KafkaConsumer(bootstrap_servers='localhost:9092')
    print("✅ Consumer connected successfully!")
    
    print("\n🎉 Kafka setup is working!")
    
except Exception as e:
    print(f"❌ Error: {e}")
