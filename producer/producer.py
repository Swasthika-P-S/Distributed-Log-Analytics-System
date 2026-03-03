"""
Main Producer Application (Person 1 - Kafka Producer System)
Simulates multiple services generating logs and sending to Kafka
"""

import os
import sys
import time
import yaml
import logging
import signal
from threading import Thread, Event
from log_generator import LogGenerator
from kafka_producer_wrapper import KafkaProducerWrapper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Global shutdown event
shutdown_event = Event()


def load_config(config_path='config.yaml'):
    """Load configuration from YAML file"""
    # If path is relative and doesn't exist, try looking in the script's directory
    if not os.path.isabs(config_path) and not os.path.exists(config_path):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        potential_path = os.path.join(script_dir, config_path)
        if os.path.exists(potential_path):
            config_path = potential_path

    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        logger.info(f"✅ Configuration loaded from {config_path}")
        return config
    except Exception as e:
        logger.error(f"❌ Failed to load configuration: {e}")
        sys.exit(1)


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info("\n🛑 Shutdown signal received. Stopping producers...")
    shutdown_event.set()


class ServiceSimulator(Thread):
    """Simulates a single service generating logs"""
    
    def __init__(self, service_name, service_config, log_generator, kafka_producer, rate):
        super().__init__(daemon=True)
        self.service_name = service_name
        self.service_config = service_config
        self.log_generator = log_generator
        self.kafka_producer = kafka_producer
        self.rate = rate  # logs per second
        self.interval = 1.0 / rate if rate > 0 else 1.0
        
    def run(self):
        """Run the service simulator"""
        logger.info(f"🚀 Started {self.service_name} simulator (rate: {self.rate} logs/sec)")
        
        while not shutdown_event.is_set():
            try:
                # Generate log
                log_entry = self.log_generator.generate_log(
                    self.service_name,
                    self.service_config
                )
                
                # Send to Kafka
                success = self.kafka_producer.send_log(self.service_name, log_entry)
                
                if success:
                    logger.info(
                        f"📤 [{self.service_name:15}] {log_entry['level']:8} | {log_entry['message'][:60]}"
                    )
                
                # Wait before next log
                shutdown_event.wait(self.interval)
                
            except Exception as e:
                logger.error(f"❌ Error in {self.service_name} simulator: {e}")
                shutdown_event.wait(1)
        
        logger.info(f"🛑 Stopped {self.service_name} simulator")


def print_statistics(kafka_producer, interval=30):
    """Print statistics periodically"""
    while not shutdown_event.is_set():
        shutdown_event.wait(interval)
        
        if not shutdown_event.is_set():
            stats = kafka_producer.get_stats()
            logger.info("\n" + "="*70)
            logger.info("📊 PRODUCER STATISTICS")
            logger.info("="*70)
            logger.info(f"Total Sent: {stats['sent']} | Failed: {stats['failed']}")
            
            logger.info("\nBy Service:")
            for service, count in stats['by_service'].items():
                logger.info(f"  {service:20} : {count:6} logs")
            
            logger.info("\nBy Level:")
            for level, count in stats['by_level'].items():
                logger.info(f"  {level:10} : {count:6} logs")
            logger.info("="*70 + "\n")


def main():
    """Main entry point"""
    logger.info("="*70)
    logger.info("🚀 KAFKA PRODUCER SYSTEM - PERSON 1")
    logger.info("="*70)
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Wait for Kafka to be ready (when running in Docker)
    kafka_wait = int(os.getenv('KAFKA_WAIT_TIME', '10'))
    if kafka_wait > 0:
        logger.info(f"⏳ Waiting {kafka_wait} seconds for Kafka to be ready...")
        time.sleep(kafka_wait)
    
    # Load configuration
    config = load_config()
    
    # Initialize components
    log_generator = LogGenerator(config)
    kafka_producer = KafkaProducerWrapper(config)
    
    # Get enabled services
    services_config = config['services']
    enabled_services = {
        name: svc_config 
        for name, svc_config in services_config.items() 
        if svc_config.get('enabled', True)
    }
    
    logger.info(f"\n✅ Enabled services: {', '.join(enabled_services.keys())}")
    
    # Get rate per service
    rate_per_service = config['log_generation']['rate_per_service']
    
    # Start service simulators
    simulators = []
    for service_name, service_config in enabled_services.items():
        simulator = ServiceSimulator(
            service_name,
            service_config,
            log_generator,
            kafka_producer,
            rate_per_service
        )
        simulator.start()
        simulators.append(simulator)
    
    # Start statistics thread
    stats_thread = Thread(target=print_statistics, args=(kafka_producer, 30), daemon=True)
    stats_thread.start()
    
    logger.info("\n✅ All services started. Press Ctrl+C to stop.\n")
    
    # Wait for shutdown signal
    try:
        while not shutdown_event.is_set():
            shutdown_event.wait(1)
    except KeyboardInterrupt:
        logger.info("\n🛑 Keyboard interrupt received")
        shutdown_event.set()
    
    # Wait for simulators to finish
    logger.info("⏳ Waiting for simulators to finish...")
    for simulator in simulators:
        simulator.join(timeout=2)
    
    # Flush and close producer
    logger.info("⏳ Flushing pending messages...")
    kafka_producer.flush()
    kafka_producer.close()
    
    # Print final statistics
    stats = kafka_producer.get_stats()
    logger.info("\n" + "="*70)
    logger.info("📊 FINAL STATISTICS")
    logger.info("="*70)
    logger.info(f"Total Sent: {stats['sent']} | Failed: {stats['failed']}")
    logger.info("="*70)
    
    logger.info("\n✅ Producer shutdown complete")


if __name__ == "__main__":
    main()