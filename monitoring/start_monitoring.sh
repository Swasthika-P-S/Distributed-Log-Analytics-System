#!/bin/bash

# Person 5 Monitoring System - Complete Startup and Verification Script
# This script will help you start everything and verify it's working

set -e  # Exit on error

echo "🛡️  Person 5: Fault Tolerance & Monitoring - Startup Script"
echo "================================================================"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored messages
print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

# Step 1: Check Docker
echo "Step 1: Checking Docker..."
if ! command -v docker &> /dev/null; then
    print_error "Docker is not installed!"
    echo "Please install Docker Desktop from: https://www.docker.com/products/docker-desktop"
    exit 1
fi

if ! docker info &> /dev/null; then
    print_error "Docker is not running!"
    echo ""
    print_info "Please start Docker Desktop:"
    print_info "1. Open Docker Desktop application"
    print_info "2. Wait for it to start (you'll see the whale icon in your menu bar)"
    print_info "3. Then run this script again"
    exit 1
fi

print_success "Docker is installed and running"
echo ""

# Step 2: Check if we're in the right directory
echo "Step 2: Checking project directory..."
if [ ! -f "docker-compose.yml" ]; then
    print_error "docker-compose.yml not found!"
    print_info "Please run this script from the project root directory:"
    print_info "cd /Users/priyadarshinianand/Documents/Distributed-Log-Analytics-System"
    exit 1
fi

print_success "In correct directory"
echo ""

# Step 3: Check if containers are already running
echo "Step 3: Checking existing containers..."
RUNNING_CONTAINERS=$(docker ps --filter "name=broker" --filter "name=spark" --filter "name=namenode" --format "{{.Names}}" 2>/dev/null | wc -l)

if [ "$RUNNING_CONTAINERS" -gt 5 ]; then
    print_success "System appears to be already running!"
    print_info "Current containers:"
    docker ps --format "table {{.Names}}\t{{.Status}}"
    echo ""
else
    print_warning "System not fully running. Starting containers..."
    echo ""
    
    # Step 4: Start the system
    echo "Step 4: Starting all services..."
    print_info "This may take 2-3 minutes for all containers to be healthy..."
    
    docker-compose up -d
    
    echo ""
    print_info "Waiting for containers to start..."
    sleep 10
    
    print_success "Containers started"
    echo ""
fi

# Step 5: Wait for containers to be healthy
echo "Step 5: Waiting for all containers to be healthy..."
print_info "Checking container health (this may take 1-2 minutes)..."

MAX_WAIT=120  # 2 minutes
ELAPSED=0
while [ $ELAPSED -lt $MAX_WAIT ]; do
    HEALTHY=$(docker ps --filter "health=healthy" --format "{{.Names}}" | wc -l)
    TOTAL=$(docker ps --format "{{.Names}}" | wc -l)
    
    if [ "$HEALTHY" -ge 6 ]; then  # At least Zookeeper + 3 Kafka brokers should be healthy
        print_success "Containers are healthy!"
        break
    fi
    
    echo -ne "\r   Healthy: $HEALTHY/$TOTAL containers (${ELAPSED}s elapsed)..."
    sleep 5
    ELAPSED=$((ELAPSED + 5))
done

echo ""
echo ""

# Step 6: Show container status
echo "Step 6: Current system status:"
echo "================================================================"
docker ps --format "table {{.Names}}\t{{.Status}}"
echo "================================================================"
echo ""

# Step 7: Quick health checks
echo "Step 7: Running quick health checks..."
echo ""

# Check Kafka
print_info "Checking Kafka..."
if docker exec broker-1 kafka-topics --bootstrap-server broker-1:29092 --list 2>/dev/null | grep -q "service-logs"; then
    print_success "Kafka is working - topic 'service-logs' found"
else
    print_warning "Kafka topic not found yet (may still be initializing)"
fi

# Check HDFS
print_info "Checking HDFS..."
if docker exec namenode hdfs dfs -ls / 2>/dev/null > /dev/null; then
    print_success "HDFS is working"
else
    print_warning "HDFS not responding yet"
fi

# Check Spark
print_info "Checking Spark..."
if docker ps --filter "name=spark-processor" --format "{{.Status}}" | grep -q "Up"; then
    print_success "Spark processor is running"
else
    print_warning "Spark processor not running"
fi

echo ""
echo "================================================================"
echo "🎉 SYSTEM STARTUP COMPLETE!"
echo "================================================================"
echo ""

# Step 8: Show next steps
echo "📋 NEXT STEPS:"
echo ""
echo "1️⃣  Start Health Monitor (in a NEW terminal):"
echo "   cd monitoring"
echo "   python3 health_monitor.py"
echo ""
echo "2️⃣  Start Alert System (in ANOTHER terminal):"
echo "   cd monitoring"
echo "   python3 alert_system.py"
echo ""
echo "3️⃣  Run a test (in this terminal):"
echo "   cd monitoring"
echo "   python3 fault_injector.py --test kafka-broker"
echo ""
echo "================================================================"
echo ""

# Step 9: Offer to open monitoring directory
read -p "Do you want to open a new terminal for monitoring? (y/n) " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    print_info "Opening new terminal for health monitor..."
    cd monitoring
    
    # For macOS, open a new terminal window
    if [[ "$OSTYPE" == "darwin"* ]]; then
        osascript -e "tell application \"Terminal\" to do script \"cd $(pwd) && python3 health_monitor.py\""
        print_success "New terminal opened! Health monitor should be starting..."
    else
        print_info "Please open a new terminal manually and run:"
        print_info "cd $(pwd) && python3 health_monitor.py"
    fi
fi

echo ""
print_success "Setup complete! Happy monitoring! 🛡️"
echo ""
