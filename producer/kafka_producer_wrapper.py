"""
Kafka Producer Wrapper
Handles Kafka producer initialization, batching, and error handling
"""

from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


class KafkaProducerWrapper:
    """Wrapper for Kafka producer with enhanced features"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Kafka producer with configuration
        
        Args:
            config: Configuration dictionary containing Kafka settings
        """
        self.config = config
        kafka_config = config['kafka']
        producer_config = kafka_config['producer_config']
        
        # Get bootstrap servers
        bootstrap_servers = kafka_config['bootstrap_servers']
        
        # Initialize Kafka producer
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks=producer_config['acks'],
                retries=producer_config['retries'],
                retry_backoff_ms=producer_config['retry_backoff_ms'],
                batch_size=producer_config['batch_size'],
                linger_ms=producer_config['linger_ms'],
                compression_type=producer_config['compression_type'],
                buffer_memory=producer_config['buffer_memory'],
                max_block_ms=producer_config['max_block_ms']
            )
            logger.info(f"✅ Kafka producer initialized: {bootstrap_servers}")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Kafka producer: {e}")
            raise
        
        # Topic mapping from Phase 0
        self.topic = kafka_config['topic']
        
        # Statistics
        self.stats = {
            'sent': 0,
            'failed': 0,
            'by_service': {},
            'by_level': {}
        }
    
    def send_log(self, service_name: str, log_entry: Dict[str, Any]) -> bool:
        """
        Send a log entry to the appropriate Kafka topic
        
        Args:
            service_name: Name of the service (web_server, database, etc.)
            log_entry: Log entry dictionary
            
        Returns:
            bool: True if sent successfully, False otherwise
        """
        try:
            # Phase 0 Agreement: Use service name as key for partitioning
            # This ensures all logs from the same service go to the same partition
            key = self.config['kafka']['service_keys'].get(service_name, service_name)
            topic = self.config['kafka']['topic']
            
            # Send to Kafka with callback
            future = self.producer.send(
                topic,
                key=key,
                value=log_entry
            )
            
            # Add callback for success/failure
            future.add_callback(self._on_send_success, log_entry)
            future.add_errback(self._on_send_error, log_entry)
            
            # Update statistics
            self._update_stats(service_name, log_entry['level'], success=True)
            
            return True
            
        except KafkaError as e:
            logger.error(f"❌ Kafka error sending log: {e}")
            self._update_stats(service_name, log_entry['level'], success=False)
            return False
        except Exception as e:
            logger.error(f"❌ Unexpected error sending log: {e}")
            self._update_stats(service_name, log_entry['level'], success=False)
            return False
    
    def _on_send_success(self, log_entry, metadata):
        """Callback for successful send"""
        logger.debug(f"✅ Log sent to partition {metadata.partition} at offset {metadata.offset}")
    
    def _on_send_error(self, log_entry, exception):
        """Callback for send error"""
        logger.error(f"❌ Failed to send log: {exception}")
        self.stats['failed'] += 1
    
    def _update_stats(self, service_name: str, level: str, success: bool):
        """Update statistics"""
        if success:
            self.stats['sent'] += 1
            
            # By service
            if service_name not in self.stats['by_service']:
                self.stats['by_service'][service_name] = 0
            self.stats['by_service'][service_name] += 1
            
            # By level
            if level not in self.stats['by_level']:
                self.stats['by_level'][level] = 0
            self.stats['by_level'][level] += 1
        else:
            self.stats['failed'] += 1
    
    def get_stats(self) -> Dict[str, Any]:
        """Get producer statistics"""
        return self.stats.copy()
    
    def flush(self):
        """Flush pending messages"""
        try:
            self.producer.flush()
            logger.info("✅ Flushed pending messages")
        except Exception as e:
            logger.error(f"❌ Error flushing messages: {e}")
    
    def close(self):
        """Close the producer gracefully"""
        try:
            self.producer.flush()
            self.producer.close()
            logger.info("✅ Kafka producer closed gracefully")
        except Exception as e:
            logger.error(f"❌ Error closing producer: {e}")
