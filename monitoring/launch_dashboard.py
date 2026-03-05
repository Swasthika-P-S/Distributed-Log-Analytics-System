#!/usr/bin/env python3
"""
Simple Dashboard Launcher
Starts all monitoring tools in separate terminals (macOS)
"""

import subprocess
import sys
import os

def launch_terminal(title, command):
    """Launch a new macOS Terminal window with a command"""
    script = f'''
    tell application "Terminal"
        do script "cd {os.getcwd()} && {command}"
        set custom title of front window to "{title}"
    end tell
    '''
    subprocess.run(['osascript', '-e', script])

def main():
    print("🛡️  Launching Person 5 Monitoring Dashboard...")
    print("=" * 60)
    
    # Change to monitoring directory
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    print("\n1. Starting Health Monitor...")
    launch_terminal("Health Monitor", "python health_monitor.py")
    
    print("2. Starting Alert System...")
    launch_terminal("Alert System", "python alert_system.py")
    
    print("3. Starting Metrics Collector...")
    launch_terminal("Metrics Collector", "python metrics_collector.py")
    
    print("\n✅ All monitoring tools launched!")
    print("=" * 60)
    print("\nMonitoring Dashboard is now active:")
    print("  - Health Monitor: Real-time component status")
    print("  - Alert System: Threshold monitoring & alerts")
    print("  - Metrics Collector: Performance data collection")
    print("\nPress Ctrl+C in each terminal to stop.")
    print("\nTo run tests:")
    print("  python fault_injector.py --test full")
    print("  bash scripts/full_system_test.sh")

if __name__ == '__main__':
    if sys.platform != 'darwin':
        print("⚠️  This launcher is designed for macOS.")
        print("On other systems, run each tool manually in separate terminals:")
        print("  python health_monitor.py")
        print("  python alert_system.py")
        print("  python metrics_collector.py")
        sys.exit(1)
    
    main()
