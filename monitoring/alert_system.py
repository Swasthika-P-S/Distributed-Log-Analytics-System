"""
Person 5: Alert System
Detect and alert on threshold violations
"""

import time
import sys
import os
from datetime import datetime
import yaml
import logging
from kafka import KafkaConsumer, KafkaAdminClient
from kafka.admin import ConfigResource, ConfigResourceType
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('alerts.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AlertSystem:
    def __init__(self, config_path='config/alert_rules.yaml'):
        self.config = self.load_config(config_path)
        self.alert_history = []
        self.active_alerts = {}
    
    def load_config(self, config_path):
        """Load alert configuration"""
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        return self.get_default_config()
    
    def get_default_config(self):
        """Default alert configuration"""
        return {
            'kafka': {
                'bootstrap_servers': 'localhost:9092,localhost:9093,localhost:9094',
                'topic': 'service-logs'
            },
            'hdfs': {
                'namenode_url': 'http://localhost:9870'
            },
            'thresholds': {
                'consumer_lag_warning': 500,
                'consumer_lag_critical': 2000,
                'error_rate_warning': 0.05,  # 5%
                'error_rate_critical': 0.10,  # 10%
                'datanode_min': 2,
                'replication_factor_min': 2,
                'disk_usage_warning': 0.80,  # 80%
                'disk_usage_critical': 0.90  # 90%
            },
            'alert_channels': {
                'console': True,
                'file': True,
                'email': False,  # Configure SMTP if needed
                'slack': False   # Configure webhook if needed
            },
            'check_interval': 30  # seconds
        }
    
    def send_alert(self, alert_type, severity, message, details=None):
        """Send alert through configured channels"""
        alert = {
            'timestamp': datetime.now().isoformat(),
            'type': alert_type,
            'severity': severity,  # INFO, WARNING, CRITICAL
            'message': message,
            'details': details or {}
        }
        
        # Store in history
        self.alert_history.append(alert)
        
        # Mark as active if not INFO
        if severity != 'INFO':
            alert_key = f"{alert_type}:{message}"
            self.active_alerts[alert_key] = alert
        
        # Send to configured channels
        if self.config['alert_channels']['console']:
            self._alert_to_console(alert)
        
        if self.config['alert_channels']['file']:
            self._alert_to_file(alert)
        
        # Additional channels (email, slack) would be implemented here
        
    def _alert_to_console(self, alert):
        """Print alert to console"""
        severity_symbol = {
            'INFO': '✅',
            'WARNING': '⚠️',
            'CRITICAL': '🚨'
        }.get(alert['severity'], 'ℹ️')
        
        logger.log(
            logging.INFO if alert['severity'] == 'INFO' else 
            logging.WARNING if alert['severity'] == 'WARNING' else 
            logging.ERROR,
            f"{severity_symbol} [{alert['type']}] {alert['message']}"
        )
    
    def _alert_to_file(self, alert):
        """Already handled by logging FileHandler"""
        pass
    
    def clear_alert(self, alert_type, message):
        """Clear an active alert"""
        alert_key = f"{alert_type}:{message}"
        if alert_key in self.active_alerts:
            del self.active_alerts[alert_key]
            self.send_alert(alert_type, 'INFO', f"RESOLVED: {message}")
    
    def check_kafka_health(self):
        """Check Kafka cluster health and alert on issues"""
        try:
            brokers = self.config['kafka']['bootstrap_servers'].split(',')
            down_brokers = []
            
            for broker in brokers:
                try:
                    admin = KafkaAdminClient(
                        bootstrap_servers=broker,
                        request_timeout_ms=3000
                    )
                    admin.close()
                except Exception:
                    down_brokers.append(broker)
            
            if len(down_brokers) == len(brokers):
                self.send_alert('KAFKA', 'CRITICAL', 'All Kafka brokers are down!',
                              {'down_brokers': down_brokers})
            elif len(down_brokers) > 0:
                self.send_alert('KAFKA', 'WARNING', f'{len(down_brokers)} Kafka broker(s) down',
                              {'down_brokers': down_brokers})
            else:
                # Clear any existing broker down alerts
                self.clear_alert('KAFKA', 'Kafka brokers are down')
                
        except Exception as e:
            self.send_alert('KAFKA', 'CRITICAL', f'Kafka health check failed: {str(e)}')
    
    def check_consumer_lag(self):
        """Check consumer lag and alert if exceeds thresholds"""
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=self.config['kafka']['bootstrap_servers'],
                group_id='spark-log-processor',
                consumer_timeout_ms=3000
            )
            
            topic = self.config['kafka']['topic']
            partitions = consumer.partitions_for_topic(topic)
            
            if partitions:
                total_lag = 0
                
                for partition in partitions:
                    tp = {'topic': topic, 'partition': partition}
                    committed = consumer.committed(tp) or 0
                    
                    consumer.assign([tp])
                    consumer.seek_to_end(tp)
                    end_offset = consumer.position(tp)
                    
                    lag = end_offset - committed
                    total_lag += lag
                
                # Check thresholds
                if total_lag >= self.config['thresholds']['consumer_lag_critical']:
                    self.send_alert('CONSUMER_LAG', 'CRITICAL', 
                                  f'Critical consumer lag: {total_lag} messages',
                                  {'total_lag': total_lag})
                elif total_lag >= self.config['thresholds']['consumer_lag_warning']:
                    self.send_alert('CONSUMER_LAG', 'WARNING',
                                  f'High consumer lag: {total_lag} messages',
                                  {'total_lag': total_lag})
                else:
                    self.clear_alert('CONSUMER_LAG', 'consumer lag')
            
            consumer.close()
            
        except Exception as e:
            self.send_alert('CONSUMER_LAG', 'WARNING', f'Consumer lag check failed: {str(e)}')
    
    def check_hdfs_health(self):
        """Check HDFS cluster health and alert on issues"""
        try:
            url = f"{self.config['hdfs']['namenode_url']}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystem"
            response = requests.get(url, timeout=5)
            
            if response.status_code == 200:
                data = response.json()
                beans = data.get('beans', [])
                
                if beans:
                    fs_data = beans[0]
                    
                    live_nodes = fs_data.get('NumLiveDataNodes', 0)
                    dead_nodes = fs_data.get('NumDeadDataNodes', 0)
                    missing_blocks = fs_data.get('MissingBlocks', 0)
                    under_replicated = fs_data.get('UnderReplicatedBlocks', 0)
                    capacity_used_percent = fs_data.get('PercentUsed', 0) / 100
                    
                    # Check DataNode count
                    if live_nodes < self.config['thresholds']['datanode_min']:
                        self.send_alert('HDFS', 'CRITICAL',
                                      f'Insufficient DataNodes: {live_nodes}/{self.config["thresholds"]["datanode_min"]}',
                                      {'live_nodes': live_nodes, 'dead_nodes': dead_nodes})
                    
                    # Check missing blocks
                    if missing_blocks > 0:
                        self.send_alert('HDFS', 'CRITICAL',
                                      f'Missing blocks detected: {missing_blocks}',
                                      {'missing_blocks': missing_blocks})
                    
                    # Check under-replicated blocks
                    if under_replicated > 0:
                        self.send_alert('HDFS', 'WARNING',
                                      f'Under-replicated blocks: {under_replicated}',
                                      {'under_replicated_blocks': under_replicated})
                    
                    # Check disk usage
                    if capacity_used_percent >= self.config['thresholds']['disk_usage_critical']:
                        self.send_alert('HDFS', 'CRITICAL',
                                      f'Critical disk usage: {capacity_used_percent*100:.1f}%',
                                      {'disk_usage': capacity_used_percent})
                    elif capacity_used_percent >= self.config['thresholds']['disk_usage_warning']:
                        self.send_alert('HDFS', 'WARNING',
                                      f'High disk usage: {capacity_used_percent*100:.1f}%',
                                      {'disk_usage': capacity_used_percent})
                    
                    # Clear alerts if all OK
                    if (live_nodes >= self.config['thresholds']['datanode_min'] and 
                        missing_blocks == 0 and under_replicated == 0):
                        self.clear_alert('HDFS', 'HDFS issues detected')
                        
        except Exception as e:
            self.send_alert('HDFS', 'WARNING', f'HDFS health check failed: {str(e)}')
    
    def check_spark_health(self):
        """Check Spark processor health"""
        try:
            import subprocess
            result = subprocess.run(
                ['docker', 'ps', '--filter', 'name=spark-processor', '--format', '{{.Status}}'],
                capture_output=True, text=True, timeout=5
            )
            
            if result.returncode == 0 and result.stdout.strip():
                if 'Up' in result.stdout:
                    self.clear_alert('SPARK', 'Spark processor is down')
                else:
                    self.send_alert('SPARK', 'CRITICAL', 'Spark processor is not running')
            else:
                self.send_alert('SPARK', 'CRITICAL', 'Spark processor container not found')
                
        except Exception as e:
            self.send_alert('SPARK', 'WARNING', f'Spark health check failed: {str(e)}')
    
    def run(self):
        """Run continuous monitoring and alerting"""
        logger.info("🛡️  Alert System Started (Person 5)")
        logger.info(f"Monitoring interval: {self.config['check_interval']} seconds")
        logger.info("Press Ctrl+C to stop\n")
        
        try:
            while True:
                logger.info(f"--- Health Check: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
                
                # Run all health checks
                self.check_kafka_health()
                self.check_consumer_lag()
                self.check_hdfs_health()
                self.check_spark_health()
                
                # Show active alerts summary
                if self.active_alerts:
                    logger.warning(f"Active Alerts: {len(self.active_alerts)}")
                    for alert_key, alert in self.active_alerts.items():
                        logger.warning(f"  - [{alert['severity']}] {alert['message']}")
                else:
                    logger.info("✅ No active alerts - System healthy")
                
                logger.info("")
                time.sleep(self.config['check_interval'])
                
        except KeyboardInterrupt:
            logger.info("\n🛑 Alert system stopped by user")
            logger.info(f"Total alerts generated: {len(self.alert_history)}")
            sys.exit(0)

def main():
    alert_system = AlertSystem()
    alert_system.run()

if __name__ == '__main__':
    main()
