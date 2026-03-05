"""
Health Check Script for Person 5 (Monitoring)
Verifies if the Kafka Producer is reachable and the environment is healthy.
"""

import os
import sys
import yaml
import socket
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

def check_kafka_connection(brokers):
    """Check if Kafka brokers are reachable at the network level"""
    for broker in brokers:
        host, port = broker.split(':')
        try:
            with socket.create_connection((host, int(port)), timeout=2):
                logger.info(f"✅ Network connection to Kafka broker {broker} successful.")
        except (socket.timeout, ConnectionRefusedError):
            logger.warning(f"❌ Network connection to Kafka broker {broker} failed.")
            return False
    return True

def main():
    config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
    
    if not os.path.exists(config_path):
        logger.error(f"❌ config.yaml not found at {config_path}")
        sys.exit(1)
        
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    # Prioritize KAFKA_BROKER env var, fall back to config.yaml
    env_brokers = os.getenv('KAFKA_BROKER')
    if env_brokers:
        brokers = [b.strip() for b in env_brokers.split(',')]
        logger.info(f"🌐 Using Kafka brokers from environment: {brokers}")
    else:
        brokers = config['kafka']['bootstrap_servers']
        logger.info(f"📂 Using Kafka brokers from config: {brokers}")
    
    logger.info("Starting Person 1 Producer Health Check...")
    
    kafka_ok = check_kafka_connection(brokers)
    
    # Check if faker is installed
    try:
        from faker import Faker
        logger.info("✅ Python dependency 'faker' is installed.")
    except ImportError:
        logger.error("❌ Python dependency 'faker' is missing.")
        kafka_ok = False

    if kafka_ok:
        logger.info("\n🟢 SYSTEM HEALTHY - READY FOR PRODUCTION")
    else:
        logger.info("\n🔴 SYSTEM UNHEALTHY - CHECK NETWORK OR DEPENDENCIES")
        sys.exit(1)

if __name__ == "__main__":
    main()
