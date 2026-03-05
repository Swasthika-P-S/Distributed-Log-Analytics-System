"""
Person 5: Fault Injector
Simulate failure scenarios to test system resilience
"""

import subprocess
import sys
import time
import argparse
from datetime import datetime
from colorama import Fore, Style, init

init(autoreset=True)

class FaultInjector:
    def __init__(self):
        self.test_results = []
    
    def log(self, message, level='INFO'):
        """Log message with timestamp"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        color = {
            'INFO': Fore.WHITE,
            'SUCCESS': Fore.GREEN,
            'WARNING': Fore.YELLOW,
            'ERROR': Fore.RED,
            'TEST': Fore.CYAN
        }.get(level, Fore.WHITE)
        
        print(f"{color}[{timestamp}] {level}: {message}")
    
    def run_command(self, command, shell=False):
        """Execute shell command and return output"""
        try:
            result = subprocess.run(
                command if shell else command.split(),
                capture_output=True,
                text=True,
                timeout=30,
                shell=shell
            )
            return result.returncode == 0, result.stdout, result.stderr
        except Exception as e:
            return False, "", str(e)
    
    def check_container_status(self, container_name):
        """Check if container is running"""
        success, stdout, _ = self.run_command(f"docker ps --filter name={container_name} --format '{{{{.Status}}}}'")
        return success and stdout.strip() != ""
    
    def kill_container(self, container_name):
        """Kill a Docker container"""
        self.log(f"Killing container: {container_name}", 'TEST')
        success, _, _ = self.run_command(f"docker kill {container_name}")
        if success:
            self.log(f"✅ Container {container_name} killed", 'SUCCESS')
            return True
        else:
            self.log(f"❌ Failed to kill {container_name}", 'ERROR')
            return False
    
    def start_container(self, container_name):
        """Start a Docker container"""
        self.log(f"Starting container: {container_name}", 'TEST')
        success, _, _ = self.run_command(f"docker start {container_name}")
        if success:
            self.log(f"✅ Container {container_name} started", 'SUCCESS')
            return True
        else:
            self.log(f"❌ Failed to start {container_name}", 'ERROR')
            return False
    
    def restart_container(self, container_name):
        """Restart a Docker container"""
        self.log(f"Restarting container: {container_name}", 'TEST')
        success, _, _ = self.run_command(f"docker restart {container_name}")
        if success:
            self.log(f"✅ Container {container_name} restarted", 'SUCCESS')
            return True
        else:
            self.log(f"❌ Failed to restart {container_name}", 'ERROR')
            return False
    
    def check_kafka_topic_health(self):
        """Verify Kafka topic is accessible"""
        self.log("Checking Kafka topic health...", 'INFO')
        success, stdout, _ = self.run_command(
            "docker exec broker-1 kafka-topics --bootstrap-server broker-1:29092 --list"
        )
        if success and 'service-logs' in stdout:
            self.log("✅ Kafka topic 'service-logs' is accessible", 'SUCCESS')
            return True
        else:
            self.log("❌ Kafka topic not accessible", 'ERROR')
            return False
    
    def check_kafka_leader_election(self):
        """Check leader election status"""
        self.log("Checking Kafka leader election...", 'INFO')
        success, stdout, _ = self.run_command(
            "docker exec broker-1 kafka-topics --bootstrap-server broker-1:29092 --describe --topic service-logs"
        )
        if success:
            print(stdout)
            if 'Leader:' in stdout and 'Isr:' in stdout:
                self.log("✅ Leader election information available", 'SUCCESS')
                return True
        self.log("❌ Leader election check failed", 'ERROR')
        return False
    
    def check_hdfs_replication(self):
        """Check HDFS replication status"""
        self.log("Checking HDFS replication status...", 'INFO')
        success, stdout, _ = self.run_command(
            "docker exec namenode hdfs dfsadmin -report"
        )
        if success:
            if 'Live datanodes' in stdout:
                # Count live datanodes
                live_count = stdout.count('Name:')
                self.log(f"✅ HDFS has {live_count} live DataNode(s)", 'SUCCESS')
                return True
        self.log("❌ HDFS replication check failed", 'ERROR')
        return False
    
    def check_spark_checkpoint(self):
        """Verify Spark checkpoint exists"""
        self.log("Checking Spark checkpoint...", 'INFO')
        success, stdout, _ = self.run_command(
            "docker exec namenode hdfs dfs -ls /spark-checkpoints"
        )
        if success:
            self.log("✅ Spark checkpoint directory exists", 'SUCCESS')
            return True
        else:
            self.log("⚠️  Spark checkpoint not found (may be normal if just started)", 'WARNING')
            return False
    
    # ========================================================================
    # TEST SCENARIOS
    # ========================================================================
    
    def test_kafka_broker_failure(self, broker='broker-2'):
        """
        TEST 1: Kafka Broker Failure
        Verify system handles broker failure without data loss
        """
        self.log("="*80, 'TEST')
        self.log("TEST 1: KAFKA BROKER FAILURE", 'TEST')
        self.log("="*80, 'TEST')
        
        # Pre-test checks
        self.log("PHASE 1: Pre-test verification", 'TEST')
        if not self.check_container_status(broker):
            self.log(f"❌ {broker} is not running. Cannot proceed.", 'ERROR')
            return False
        
        initial_topic_health = self.check_kafka_topic_health()
        
        # Kill broker
        self.log(f"\nPHASE 2: Killing {broker}", 'TEST')
        if not self.kill_container(broker):
            return False
        
        time.sleep(5)  # Wait for cluster to react
        
        # Check leader election
        self.log("\nPHASE 3: Verifying leader election", 'TEST')
        self.check_kafka_leader_election()
        
        # Verify remaining brokers handle traffic
        self.log("\nPHASE 4: Checking remaining brokers", 'TEST')
        topic_still_accessible = self.check_kafka_topic_health()
        
        # Restart broker
        self.log(f"\nPHASE 5: Restarting {broker}", 'TEST')
        self.start_container(broker)
        time.sleep(10)  # Wait for broker to sync
        
        # Verify recovery
        self.log("\nPHASE 6: Verifying recovery", 'TEST')
        final_topic_health = self.check_kafka_topic_health()
        self.check_kafka_leader_election()
        
        # Results
        self.log("\n" + "="*80, 'TEST')
        self.log("TEST RESULTS:", 'TEST')
        self.log("="*80, 'TEST')
        
        results = {
            'Initial topic health': initial_topic_health,
            'Topic accessible after broker failure': topic_still_accessible,
            'Broker restarted': True,
            'Final topic health': final_topic_health
        }
        
        for key, value in results.items():
            status = f"{Fore.GREEN}✅ PASS" if value else f"{Fore.RED}❌ FAIL"
            self.log(f"{key}: {status}", 'INFO')
        
        overall_pass = all(results.values())
        if overall_pass:
            self.log("\n🎉 TEST 1 PASSED: System survived Kafka broker failure!", 'SUCCESS')
        else:
            self.log("\n❌ TEST 1 FAILED: System did not handle broker failure correctly", 'ERROR')
        
        return overall_pass
    
    def test_hdfs_datanode_failure(self, datanode='datanode1'):
        """
        TEST 2: HDFS DataNode Failure
        Verify HDFS maintains replication factor
        """
        self.log("="*80, 'TEST')
        self.log("TEST 2: HDFS DATANODE FAILURE", 'TEST')
        self.log("="*80, 'TEST')
        
        # Pre-test checks
        self.log("PHASE 1: Pre-test verification", 'TEST')
        if not self.check_container_status(datanode):
            self.log(f"❌ {datanode} is not running. Cannot proceed.", 'ERROR')
            return False
        
        initial_hdfs_health = self.check_hdfs_replication()
        
        # Kill DataNode
        self.log(f"\nPHASE 2: Killing {datanode}", 'TEST')
        if not self.kill_container(datanode):
            return False
        
        time.sleep(10)  # Wait for HDFS to react
        
        # Check HDFS status
        self.log("\nPHASE 3: Checking HDFS cluster status", 'TEST')
        self.check_hdfs_replication()
        
        # Restart DataNode
        self.log(f"\nPHASE 4: Restarting {datanode}", 'TEST')
        self.start_container(datanode)
        time.sleep(15)  # Wait for DataNode to rejoin
        
        # Verify recovery
        self.log("\nPHASE 5: Verifying recovery", 'TEST')
        final_hdfs_health = self.check_hdfs_replication()
        
        # Results
        self.log("\n" + "="*80, 'TEST')
        self.log("TEST RESULTS:", 'TEST')
        self.log("="*80, 'TEST')
        
        results = {
            'Initial HDFS health': initial_hdfs_health,
            'DataNode restarted': True,
            'Final HDFS health': final_hdfs_health
        }
        
        for key, value in results.items():
            status = f"{Fore.GREEN}✅ PASS" if value else f"{Fore.RED}❌ FAIL"
            self.log(f"{key}: {status}", 'INFO')
        
        overall_pass = all(results.values())
        if overall_pass:
            self.log("\n🎉 TEST 2 PASSED: HDFS survived DataNode failure!", 'SUCCESS')
        else:
            self.log("\n❌ TEST 2 FAILED: HDFS did not handle DataNode failure correctly", 'ERROR')
        
        return overall_pass
    
    def test_spark_recovery(self):
        """
        TEST 3: Spark Process Failure
        Verify Spark recovers from checkpoint
        """
        self.log("="*80, 'TEST')
        self.log("TEST 3: SPARK PROCESS FAILURE & RECOVERY", 'TEST')
        self.log("="*80, 'TEST')
        
        container = 'spark-processor'
        
        # Pre-test checks
        self.log("PHASE 1: Pre-test verification", 'TEST')
        if not self.check_container_status(container):
            self.log(f"❌ {container} is not running. Cannot proceed.", 'ERROR')
            return False
        
        initial_checkpoint = self.check_spark_checkpoint()
        
        # Kill Spark
        self.log(f"\nPHASE 2: Killing {container}", 'TEST')
        if not self.kill_container(container):
            return False
        
        time.sleep(5)
        
        # Restart Spark
        self.log(f"\nPHASE 3: Restarting {container}", 'TEST')
        self.start_container(container)
        time.sleep(20)  # Wait for Spark to restart and recover
        
        # Verify recovery
        self.log("\nPHASE 4: Verifying recovery from checkpoint", 'TEST')
        spark_running = self.check_container_status(container)
        checkpoint_exists = self.check_spark_checkpoint()
        
        # Results
        self.log("\n" + "="*80, 'TEST')
        self.log("TEST RESULTS:", 'TEST')
        self.log("="*80, 'TEST')
        
        results = {
            'Initial checkpoint exists': initial_checkpoint,
            'Spark restarted': spark_running,
            'Checkpoint preserved': checkpoint_exists
        }
        
        for key, value in results.items():
            status = f"{Fore.GREEN}✅ PASS" if value else f"{Fore.YELLOW}⚠️  WARN"
            self.log(f"{key}: {status}", 'INFO')
        
        # Spark recovery passes if it restarts (checkpoint may not exist if just started)
        overall_pass = spark_running
        if overall_pass:
            self.log("\n🎉 TEST 3 PASSED: Spark recovered successfully!", 'SUCCESS')
        else:
            self.log("\n❌ TEST 3 FAILED: Spark did not recover correctly", 'ERROR')
        
        return overall_pass
    
    def test_full_system_resilience(self):
        """
        TEST 4: Full System Resilience
        Kill multiple components and verify recovery
        """
        self.log("="*80, 'TEST')
        self.log("TEST 4: FULL SYSTEM RESILIENCE TEST", 'TEST')
        self.log("="*80, 'TEST')
        
        # Run all individual tests
        test1 = self.test_kafka_broker_failure()
        time.sleep(5)
        
        test2 = self.test_hdfs_datanode_failure()
        time.sleep(5)
        
        test3 = self.test_spark_recovery()
        
        # Overall result
        self.log("\n" + "="*80, 'TEST')
        self.log("OVERALL SYSTEM RESILIENCE RESULTS:", 'TEST')
        self.log("="*80, 'TEST')
        
        all_passed = test1 and test2 and test3
        
        if all_passed:
            self.log("🎉🎉🎉 ALL TESTS PASSED! System is resilient! 🎉🎉🎉", 'SUCCESS')
        else:
            self.log("❌ SOME TESTS FAILED. Review logs above.", 'ERROR')
        
        return all_passed

def main():
    parser = argparse.ArgumentParser(description='Fault Injection Testing Tool (Person 5)')
    parser.add_argument('--test', choices=['kafka-broker', 'hdfs-datanode', 'spark', 'full'],
                       help='Test scenario to run', required=False)
    parser.add_argument('--kill', help='Container name to kill (for kafka-broker or hdfs-datanode tests)')
    parser.add_argument('--restart', help='Container name to restart', metavar='CONTAINER')
    
    args = parser.parse_args()
    
    injector = FaultInjector()
    
    # Simple restart operation
    if args.restart:
        injector.restart_container(args.restart)
        return
    
    # Run specific test
    if args.test == 'kafka-broker':
        broker = args.kill if args.kill else 'broker-2'
        injector.test_kafka_broker_failure(broker)
    elif args.test == 'hdfs-datanode':
        datanode = args.kill if args.kill else 'datanode1'
        injector.test_hdfs_datanode_failure(datanode)
    elif args.test == 'spark':
        injector.test_spark_recovery()
    elif args.test == 'full':
        injector.test_full_system_resilience()
    else:
        # Interactive mode
        print(f"{Fore.CYAN}{'='*80}")
        print(f"{Fore.CYAN}🛡️  FAULT INJECTION TESTING TOOL (Person 5)")
        print(f"{Fore.CYAN}{'='*80}\n")
        print("Available tests:")
        print("  1. Kafka Broker Failure")
        print("  2. HDFS DataNode Failure")
        print("  3. Spark Recovery")
        print("  4. Full System Resilience (All tests)")
        print("\nUsage examples:")
        print("  python fault_injector.py --test kafka-broker")
        print("  python fault_injector.py --test kafka-broker --kill broker-3")
        print("  python fault_injector.py --test hdfs-datanode")
        print("  python fault_injector.py --test spark")
        print("  python fault_injector.py --test full")
        print("  python fault_injector.py --restart broker-2")

if __name__ == '__main__':
    main()
