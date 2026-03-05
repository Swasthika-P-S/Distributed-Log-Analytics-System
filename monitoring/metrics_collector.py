"""
Person 5: Metrics Collector
Collect and store performance metrics for trend analysis
"""

import time
import json
import os
from datetime import datetime
from kafka import KafkaConsumer, KafkaAdminClient
import requests
import psutil
import subprocess

class MetricsCollector:
    def __init__(self, output_dir='metrics'):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self.metrics_history = []
    
    def collect_kafka_metrics(self):
        """Collect Kafka cluster metrics"""
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'component': 'kafka',
            'brokers': {},
            'topics': {}
        }
        
        try:
            # Check each broker
            for broker_id, port in [(1, 9092), (2, 9093), (3, 9094)]:
                try:
                    admin = KafkaAdminClient(
                        bootstrap_servers=f'localhost:{port}',
                        request_timeout_ms=3000
                    )
                    metrics['brokers'][f'broker-{broker_id}'] = {
                        'status': 'UP',
                        'port': port
                    }
                    admin.close()
                except Exception:
                    metrics['brokers'][f'broker-{broker_id}'] = {
                        'status': 'DOWN',
                        'port': port
                    }
            
            # Get topic metrics
            try:
                consumer = KafkaConsumer(
                    bootstrap_servers='localhost:9092,localhost:9093,localhost:9094',
                    group_id='spark-log-processor',
                    consumer_timeout_ms=3000
                )
                
                topic = 'service-logs'
                partitions = consumer.partitions_for_topic(topic)
                
                if partitions:
                    total_lag = 0
                    partition_metrics = []
                    
                    for partition in partitions:
                        tp = {'topic': topic, 'partition': partition}
                        committed = consumer.committed(tp) or 0
                        consumer.assign([tp])
                        consumer.seek_to_end(tp)
                        end_offset = consumer.position(tp)
                        lag = end_offset - committed
                        total_lag += lag
                        
                        partition_metrics.append({
                            'partition': partition,
                            'committed_offset': committed,
                            'end_offset': end_offset,
                            'lag': lag
                        })
                    
                    metrics['topics']['service-logs'] = {
                        'partitions': partition_metrics,
                        'total_lag': total_lag
                    }
                
                consumer.close()
            except Exception as e:
                metrics['topics']['error'] = str(e)
                
        except Exception as e:
            metrics['error'] = str(e)
        
        return metrics
    
    def collect_hdfs_metrics(self):
        """Collect HDFS cluster metrics"""
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'component': 'hdfs'
        }
        
        try:
            response = requests.get(
                'http://localhost:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystem',
                timeout=5
            )
            
            if response.status_code == 200:
                data = response.json()
                beans = data.get('beans', [])
                
                if beans:
                    fs_data = beans[0]
                    metrics.update({
                        'live_datanodes': fs_data.get('NumLiveDataNodes', 0),
                        'dead_datanodes': fs_data.get('NumDeadDataNodes', 0),
                        'total_blocks': fs_data.get('BlocksTotal', 0),
                        'missing_blocks': fs_data.get('MissingBlocks', 0),
                        'under_replicated_blocks': fs_data.get('UnderReplicatedBlocks', 0),
                        'capacity_total': fs_data.get('CapacityTotal', 0),
                        'capacity_used': fs_data.get('CapacityUsed', 0),
                        'capacity_remaining': fs_data.get('CapacityRemaining', 0),
                        'percent_used': fs_data.get('PercentUsed', 0)
                    })
        except Exception as e:
            metrics['error'] = str(e)
        
        return metrics
    
    def collect_spark_metrics(self):
        """Collect Spark processor metrics"""
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'component': 'spark'
        }
        
        try:
            # Check container status
            result = subprocess.run(
                ['docker', 'ps', '--filter', 'name=spark-processor', '--format', '{{.Status}}'],
                capture_output=True, text=True, timeout=5
            )
            
            if result.returncode == 0 and result.stdout.strip():
                metrics['status'] = 'RUNNING' if 'Up' in result.stdout else 'DOWN'
                metrics['status_detail'] = result.stdout.strip()
            else:
                metrics['status'] = 'NOT_FOUND'
            
            # Try to get container stats
            result = subprocess.run(
                ['docker', 'stats', 'spark-processor', '--no-stream', '--format', 
                 '{{.CPUPerc}},{{.MemUsage}},{{.NetIO}}'],
                capture_output=True, text=True, timeout=5
            )
            
            if result.returncode == 0 and result.stdout.strip():
                parts = result.stdout.strip().split(',')
                if len(parts) >= 3:
                    metrics['cpu_percent'] = parts[0]
                    metrics['memory_usage'] = parts[1]
                    metrics['network_io'] = parts[2]
                    
        except Exception as e:
            metrics['error'] = str(e)
        
        return metrics
    
    def collect_system_metrics(self):
        """Collect host system metrics"""
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'component': 'system',
            'cpu_percent': psutil.cpu_percent(interval=1),
            'memory': {
                'total': psutil.virtual_memory().total,
                'available': psutil.virtual_memory().available,
                'percent': psutil.virtual_memory().percent
            },
            'disk': {
                'total': psutil.disk_usage('/').total,
                'used': psutil.disk_usage('/').used,
                'free': psutil.disk_usage('/').free,
                'percent': psutil.disk_usage('/').percent
            }
        }
        
        return metrics
    
    def collect_all_metrics(self):
        """Collect all metrics"""
        all_metrics = {
            'timestamp': datetime.now().isoformat(),
            'kafka': self.collect_kafka_metrics(),
            'hdfs': self.collect_hdfs_metrics(),
            'spark': self.collect_spark_metrics(),
            'system': self.collect_system_metrics()
        }
        
        return all_metrics
    
    def save_metrics(self, metrics):
        """Save metrics to file"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = os.path.join(self.output_dir, f'metrics_{timestamp}.json')
        
        with open(filename, 'w') as f:
            json.dump(metrics, f, indent=2)
        
        # Also append to daily log
        daily_file = os.path.join(
            self.output_dir,
            f'metrics_{datetime.now().strftime("%Y%m%d")}.jsonl'
        )
        with open(daily_file, 'a') as f:
            f.write(json.dumps(metrics) + '\n')
    
    def print_summary(self, metrics):
        """Print metrics summary to console"""
        print(f"\n{'='*80}")
        print(f"Metrics Summary - {metrics['timestamp']}")
        print(f"{'='*80}")
        
        # Kafka
        kafka = metrics['kafka']
        broker_status = sum(1 for b in kafka['brokers'].values() if b['status'] == 'UP')
        total_brokers = len(kafka['brokers'])
        print(f"\n📡 Kafka: {broker_status}/{total_brokers} brokers UP")
        
        if 'service-logs' in kafka.get('topics', {}):
            lag = kafka['topics']['service-logs'].get('total_lag', 0)
            print(f"   Consumer Lag: {lag} messages")
        
        # HDFS
        hdfs = metrics['hdfs']
        if 'live_datanodes' in hdfs:
            print(f"\n💾 HDFS: {hdfs['live_datanodes']} DataNodes live")
            print(f"   Capacity: {hdfs['percent_used']:.2f}% used")
            if hdfs['missing_blocks'] > 0:
                print(f"   ⚠️  Missing blocks: {hdfs['missing_blocks']}")
        
        # Spark
        spark = metrics['spark']
        print(f"\n⚡ Spark: {spark.get('status', 'UNKNOWN')}")
        if 'cpu_percent' in spark:
            print(f"   CPU: {spark['cpu_percent']}")
            print(f"   Memory: {spark['memory_usage']}")
        
        # System
        system = metrics['system']
        print(f"\n🖥️  System:")
        print(f"   CPU: {system['cpu_percent']}%")
        print(f"   Memory: {system['memory']['percent']}%")
        print(f"   Disk: {system['disk']['percent']}%")
        
        print(f"\n{'='*80}\n")
    
    def run(self, interval=60):
        """Run continuous metrics collection"""
        print("📊 Metrics Collector Started (Person 5)")
        print(f"Collection interval: {interval} seconds")
        print(f"Output directory: {self.output_dir}")
        print("Press Ctrl+C to stop\n")
        
        try:
            while True:
                metrics = self.collect_all_metrics()
                self.save_metrics(metrics)
                self.print_summary(metrics)
                self.metrics_history.append(metrics)
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("\n🛑 Metrics collection stopped by user")
            print(f"Total metrics collected: {len(self.metrics_history)}")

def main():
    collector = MetricsCollector()
    collector.run(interval=60)

if __name__ == '__main__':
    main()
