"""
Person 5: Health Monitor Dashboard
Real-time monitoring of all system components
"""

import time
import sys
import os
from datetime import datetime
from kafka import KafkaAdminClient, KafkaConsumer
from kafka.admin import ConfigResource, ConfigResourceType
from kafka.errors import KafkaError
import requests
import subprocess
from tabulate import tabulate
from colorama import Fore, Style, init
import yaml

# Initialize colorama
init(autoreset=True)

# Load configuration
def load_config():
    config_path = os.path.join(os.path.dirname(__file__), 'config', 'monitor_config.yaml')
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    return get_default_config()

def get_default_config():
    """Default configuration if config file doesn't exist"""
    return {
        'kafka': {
            'bootstrap_servers': 'localhost:9092,localhost:9093,localhost:9094',
            'topic': 'service-logs',
            'check_interval': 10
        },
        'hdfs': {
            'namenode_web_ui': 'http://localhost:9870',
            'check_interval': 15
        },
        'spark': {
            'web_ui': 'http://localhost:8080',
            'check_interval': 10
        },
        'thresholds': {
            'consumer_lag_warning': 500,
            'consumer_lag_critical': 2000,
            'replication_factor_min': 2,
            'datanode_count_min': 2
        }
    }

class HealthMonitor:
    def __init__(self, config):
        self.config = config
        self.kafka_brokers = config['kafka']['bootstrap_servers']
        self.kafka_topic = config['kafka']['topic']
        self.hdfs_url = config['hdfs']['namenode_web_ui']
        self.spark_url = config['spark']['web_ui']
        self.thresholds = config['thresholds']
        
    def clear_screen(self):
        """Clear terminal screen"""
        os.system('clear' if os.name == 'posix' else 'cls')
    
    def print_header(self):
        """Print dashboard header"""
        print(f"\n{Fore.CYAN}{'='*80}")
        print(f"{Fore.CYAN}🛡️  DISTRIBUTED LOG ANALYTICS - HEALTH MONITOR (Person 5)")
        print(f"{Fore.CYAN}{'='*80}")
        print(f"{Fore.WHITE}Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{Fore.CYAN}{'='*80}\n")
    
    def check_kafka_brokers(self):
        """Check Kafka broker health"""
        print(f"{Fore.YELLOW}📡 KAFKA CLUSTER STATUS")
        print(f"{Fore.YELLOW}{'-'*80}")
        
        results = []
        try:
            # Test each broker individually
            for broker in self.kafka_brokers.split(','):
                try:
                    admin = KafkaAdminClient(
                        bootstrap_servers=broker,
                        request_timeout_ms=5000
                    )
                    cluster_meta = admin.describe_cluster()
                    broker_id = broker.split(':')[1]
                    status = f"{Fore.GREEN}✅ HEALTHY"
                    admin.close()
                except Exception as e:
                    broker_id = broker.split(':')[1]
                    status = f"{Fore.RED}❌ DOWN"
                
                results.append([broker, status])
            
            print(tabulate(results, headers=['Broker', 'Status'], tablefmt='grid'))
            
            # Check topic health
            try:
                admin = KafkaAdminClient(bootstrap_servers=self.kafka_brokers)
                topics = admin.list_topics()
                if self.kafka_topic in topics:
                    print(f"\n{Fore.GREEN}✅ Topic '{self.kafka_topic}' exists")
                else:
                    print(f"\n{Fore.RED}❌ Topic '{self.kafka_topic}' NOT FOUND")
                admin.close()
            except Exception as e:
                print(f"\n{Fore.RED}❌ Cannot connect to Kafka cluster: {e}")
                
        except Exception as e:
            print(f"{Fore.RED}❌ Kafka health check failed: {e}")
        
        print()
    
    def check_consumer_lag(self):
        """Check consumer lag for Spark processor"""
        print(f"{Fore.YELLOW}📊 CONSUMER LAG STATUS")
        print(f"{Fore.YELLOW}{'-'*80}")
        
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=self.kafka_brokers,
                group_id='spark-log-processor',
                consumer_timeout_ms=5000
            )
            
            # Get partition info
            partitions = consumer.partitions_for_topic(self.kafka_topic)
            if partitions:
                lag_data = []
                total_lag = 0
                
                for partition in partitions:
                    try:
                        tp = {'topic': self.kafka_topic, 'partition': partition}
                        committed = consumer.committed(tp)
                        if committed is None:
                            committed = 0
                        
                        # Get end offset
                        consumer.assign([tp])
                        consumer.seek_to_end(tp)
                        end_offset = consumer.position(tp)
                        
                        lag = end_offset - committed
                        total_lag += lag
                        
                        # Color code based on lag
                        if lag < self.thresholds['consumer_lag_warning']:
                            lag_status = f"{Fore.GREEN}{lag}"
                        elif lag < self.thresholds['consumer_lag_critical']:
                            lag_status = f"{Fore.YELLOW}{lag} ⚠️"
                        else:
                            lag_status = f"{Fore.RED}{lag} 🚨"
                        
                        lag_data.append([f"Partition {partition}", committed, end_offset, lag_status])
                    except Exception as e:
                        lag_data.append([f"Partition {partition}", "N/A", "N/A", f"{Fore.RED}ERROR"])
                
                print(tabulate(lag_data, 
                             headers=['Partition', 'Committed', 'End Offset', 'Lag'],
                             tablefmt='grid'))
                
                # Overall status
                if total_lag < self.thresholds['consumer_lag_warning']:
                    print(f"\n{Fore.GREEN}✅ Total Lag: {total_lag} messages (HEALTHY)")
                elif total_lag < self.thresholds['consumer_lag_critical']:
                    print(f"\n{Fore.YELLOW}⚠️  Total Lag: {total_lag} messages (WARNING)")
                else:
                    print(f"\n{Fore.RED}🚨 Total Lag: {total_lag} messages (CRITICAL)")
            else:
                print(f"{Fore.YELLOW}⚠️  No partitions found for topic '{self.kafka_topic}'")
            
            consumer.close()
        except Exception as e:
            print(f"{Fore.RED}❌ Consumer lag check failed: {e}")
        
        print()
    
    def check_hdfs_health(self):
        """Check HDFS cluster health"""
        print(f"{Fore.YELLOW}💾 HDFS CLUSTER STATUS")
        print(f"{Fore.YELLOW}{'-'*80}")
        
        try:
            # Check NameNode web UI
            response = requests.get(f"{self.hdfs_url}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystem", 
                                  timeout=5)
            
            if response.status_code == 200:
                data = response.json()
                beans = data.get('beans', [])
                
                if beans:
                    fs_data = beans[0]
                    
                    # Extract key metrics
                    live_nodes = fs_data.get('NumLiveDataNodes', 0)
                    dead_nodes = fs_data.get('NumDeadDataNodes', 0)
                    total_blocks = fs_data.get('BlocksTotal', 0)
                    missing_blocks = fs_data.get('MissingBlocks', 0)
                    under_replicated = fs_data.get('UnderReplicatedBlocks', 0)
                    capacity_used_percent = fs_data.get('PercentUsed', 0)
                    
                    # Display metrics
                    metrics = [
                        ['NameNode', f"{Fore.GREEN}✅ ACTIVE"],
                        ['Live DataNodes', f"{Fore.GREEN}{live_nodes}" if live_nodes >= self.thresholds['datanode_count_min'] else f"{Fore.RED}{live_nodes} 🚨"],
                        ['Dead DataNodes', f"{Fore.GREEN}{dead_nodes}" if dead_nodes == 0 else f"{Fore.RED}{dead_nodes} ⚠️"],
                        ['Total Blocks', total_blocks],
                        ['Missing Blocks', f"{Fore.GREEN}{missing_blocks}" if missing_blocks == 0 else f"{Fore.RED}{missing_blocks} 🚨"],
                        ['Under-Replicated', f"{Fore.GREEN}{under_replicated}" if under_replicated == 0 else f"{Fore.YELLOW}{under_replicated} ⚠️"],
                        ['Capacity Used', f"{capacity_used_percent:.2f}%"]
                    ]
                    
                    print(tabulate(metrics, headers=['Metric', 'Value'], tablefmt='grid'))
                    
                    # Overall status
                    if live_nodes >= self.thresholds['datanode_count_min'] and missing_blocks == 0:
                        print(f"\n{Fore.GREEN}✅ HDFS cluster is HEALTHY")
                    elif live_nodes < self.thresholds['datanode_count_min']:
                        print(f"\n{Fore.RED}🚨 CRITICAL: Insufficient DataNodes!")
                    elif missing_blocks > 0:
                        print(f"\n{Fore.RED}🚨 CRITICAL: Missing blocks detected!")
                    else:
                        print(f"\n{Fore.YELLOW}⚠️  WARNING: Under-replicated blocks exist")
                else:
                    print(f"{Fore.RED}❌ Cannot parse NameNode metrics")
            else:
                print(f"{Fore.RED}❌ NameNode web UI not accessible (HTTP {response.status_code})")
                
        except requests.exceptions.RequestException:
            print(f"{Fore.RED}❌ Cannot connect to HDFS NameNode at {self.hdfs_url}")
        except Exception as e:
            print(f"{Fore.RED}❌ HDFS health check failed: {e}")
        
        print()
    
    def check_spark_status(self):
        """Check Spark processor status"""
        print(f"{Fore.YELLOW}⚡ SPARK PROCESSOR STATUS")
        print(f"{Fore.YELLOW}{'-'*80}")
        
        try:
            # Check if Spark container is running
            result = subprocess.run(
                ['docker', 'ps', '--filter', 'name=spark-processor', '--format', '{{.Status}}'],
                capture_output=True, text=True, timeout=5
            )
            
            if result.returncode == 0 and result.stdout.strip():
                status = result.stdout.strip()
                print(f"{Fore.GREEN}✅ Spark Container: RUNNING")
                print(f"   Status: {status}")
                
                # Try to check Spark web UI
                try:
                    response = requests.get(f"{self.spark_url}/api/v1/applications", timeout=5)
                    if response.status_code == 200:
                        apps = response.json()
                        if apps:
                            print(f"{Fore.GREEN}✅ Spark UI: ACCESSIBLE")
                            print(f"   Active Applications: {len(apps)}")
                        else:
                            print(f"{Fore.YELLOW}⚠️  Spark UI accessible but no active applications")
                    else:
                        print(f"{Fore.YELLOW}⚠️  Spark UI returned HTTP {response.status_code}")
                except requests.exceptions.RequestException:
                    print(f"{Fore.YELLOW}⚠️  Spark UI not accessible (may not be exposed)")
                    
            else:
                print(f"{Fore.RED}❌ Spark container is NOT RUNNING")
                
        except subprocess.TimeoutExpired:
            print(f"{Fore.RED}❌ Docker command timed out")
        except FileNotFoundError:
            print(f"{Fore.YELLOW}⚠️  Docker not found (cannot check container status)")
        except Exception as e:
            print(f"{Fore.RED}❌ Spark status check failed: {e}")
        
        print()
    
    def check_docker_containers(self):
        """Check all Docker container statuses"""
        print(f"{Fore.YELLOW}🐳 DOCKER CONTAINERS STATUS")
        print(f"{Fore.YELLOW}{'-'*80}")
        
        try:
            result = subprocess.run(
                ['docker', 'ps', '-a', '--format', '{{.Names}}\t{{.Status}}'],
                capture_output=True, text=True, timeout=5
            )
            
            if result.returncode == 0:
                containers = result.stdout.strip().split('\n')
                container_data = []
                
                for container in containers:
                    if container:
                        name, status = container.split('\t', 1)
                        
                        # Filter relevant containers
                        if any(keyword in name.lower() for keyword in 
                              ['broker', 'zookeeper', 'spark', 'namenode', 'datanode', 'producer', 'consumer']):
                            
                            if 'Up' in status and 'healthy' in status.lower():
                                status_colored = f"{Fore.GREEN}✅ {status}"
                            elif 'Up' in status:
                                status_colored = f"{Fore.YELLOW}⚠️  {status}"
                            else:
                                status_colored = f"{Fore.RED}❌ {status}"
                            
                            container_data.append([name, status_colored])
                
                if container_data:
                    print(tabulate(container_data, headers=['Container', 'Status'], tablefmt='grid'))
                else:
                    print(f"{Fore.YELLOW}⚠️  No relevant containers found")
            else:
                print(f"{Fore.RED}❌ Failed to list Docker containers")
                
        except subprocess.TimeoutExpired:
            print(f"{Fore.RED}❌ Docker command timed out")
        except FileNotFoundError:
            print(f"{Fore.YELLOW}⚠️  Docker not found")
        except Exception as e:
            print(f"{Fore.RED}❌ Container status check failed: {e}")
        
        print()
    
    def run(self, interval=30):
        """Run monitoring dashboard continuously"""
        print(f"{Fore.GREEN}Starting Health Monitor Dashboard...")
        print(f"{Fore.GREEN}Press Ctrl+C to stop\n")
        time.sleep(2)
        
        try:
            while True:
                self.clear_screen()
                self.print_header()
                
                # Run all health checks
                self.check_docker_containers()
                self.check_kafka_brokers()
                self.check_consumer_lag()
                self.check_hdfs_health()
                self.check_spark_status()
                
                # Footer
                print(f"{Fore.CYAN}{'='*80}")
                print(f"{Fore.WHITE}Refreshing in {interval} seconds... (Press Ctrl+C to stop)")
                print(f"{Fore.CYAN}{'='*80}\n")
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print(f"\n{Fore.YELLOW}Monitoring stopped by user")
            sys.exit(0)

def main():
    """Main entry point"""
    print(f"{Fore.CYAN}Loading configuration...")
    config = load_config()
    
    monitor = HealthMonitor(config)
    monitor.run(interval=30)

if __name__ == '__main__':
    main()
