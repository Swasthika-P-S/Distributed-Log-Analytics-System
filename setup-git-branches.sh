#!/bin/bash

# Git Branch Setup Script (Bash version for Linux/Mac)
# Run this script to create and push all branches

echo "================================================================================"
echo "🌿 Setting up Git branches for Distributed Log Analytics Project"
echo "================================================================================"

# Check if git is initialized
if [ ! -d ".git" ]; then
    echo ""
    echo "📦 Initializing Git repository..."
    git init
    echo "✅ Git initialized"
else
    echo ""
    echo "✅ Git repository already exists"
fi

# Create main branch with README
echo ""
echo "📝 Adding README to main branch..."
git add README.md
git commit -m "Add project documentation" -q
echo "✅ Main branch setup complete"

# Branch 1: Basic Setup
echo ""
echo "🔧 Creating branch: basic-setup..."
git checkout -b basic-setup -q
git add 1-basic-setup/
git commit -m "Add basic Kafka and Zookeeper setup" -q
echo "✅ Branch 'basic-setup' created"

# Branch 2: Kafka Integration
echo ""
echo "📡 Creating branch: kafka-integration..."
git checkout main -q
git checkout -b kafka-integration -q
git add 2-kafka-producer-consumer/
git commit -m "Add Kafka producer-consumer streaming model" -q
echo "✅ Branch 'kafka-integration' created"

# Branch 3: Spark Batch
echo ""
echo "🔥 Creating branch: spark-batch..."
git checkout main -q
git checkout -b spark-batch -q
git add 3-spark-batch-processing/
git commit -m "Add Spark batch processing" -q
echo "✅ Branch 'spark-batch' created"

# Branch 4: Window Algorithm
echo ""
echo "🪟 Creating branch: window-algorithm..."
git checkout main -q
git checkout -b window-algorithm -q
git add 4-window-streaming/
git commit -m "Add window-based stream processing algorithm" -q
echo "✅ Branch 'window-algorithm' created"

# Return to main
git checkout main -q

echo ""
echo "================================================================================"
echo "🎉 All branches created successfully!"
echo "================================================================================"

echo ""
echo "📊 Branch Summary:"
git branch

echo ""
echo "💡 Next Steps:"
echo "   1. Add remote: git remote add origin <your-repo-url>"
echo "   2. Push all branches:"
echo "      git push -u origin main"
echo "      git push -u origin basic-setup"
echo "      git push -u origin kafka-integration"
echo "      git push -u origin spark-batch"
echo "      git push -u origin window-algorithm"

echo ""
echo "✅ Done!"
