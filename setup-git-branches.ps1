# Git Branch Setup Script
# Run this script to create and push all branches

Write-Host "=" * 80
Write-Host "🌿 Setting up Git branches for Distributed Log Analytics Project"
Write-Host "=" * 80

# Check if git is initialized
if (-Not (Test-Path ".git")) {
    Write-Host "`n📦 Initializing Git repository..."
    git init
    Write-Host "✅ Git initialized"
} else {
    Write-Host "`n✅ Git repository already exists"
}

# Create main branch with README
Write-Host "`n📝 Adding README to main branch..."
git add README.md
git commit -m "Add project documentation" -q
Write-Host "✅ Main branch setup complete"

# Branch 1: Basic Setup
Write-Host "`n🔧 Creating branch: basic-setup..."
git checkout -b basic-setup -q
git add 1-basic-setup/
git commit -m "Add basic Kafka and Zookeeper setup" -q
Write-Host "✅ Branch 'basic-setup' created"

# Branch 2: Kafka Integration
Write-Host "`n📡 Creating branch: kafka-integration..."
git checkout main -q
git checkout -b kafka-integration -q
git add 2-kafka-producer-consumer/
git commit -m "Add Kafka producer-consumer streaming model" -q
Write-Host "✅ Branch 'kafka-integration' created"

# Branch 3: Spark Batch
Write-Host "`n🔥 Creating branch: spark-batch..."
git checkout main -q
git checkout -b spark-batch -q
git add 3-spark-batch-processing/
git commit -m "Add Spark batch processing" -q
Write-Host "✅ Branch 'spark-batch' created"

# Branch 4: Window Algorithm
Write-Host "`n🪟 Creating branch: window-algorithm..."
git checkout main -q
git checkout -b window-algorithm -q
git add 4-window-streaming/
git commit -m "Add window-based stream processing algorithm" -q
Write-Host "✅ Branch 'window-algorithm' created"

# Return to main
git checkout main -q

Write-Host "`n" + "=" * 80
Write-Host "🎉 All branches created successfully!"
Write-Host "=" * 80

Write-Host "`n📊 Branch Summary:"
git branch

Write-Host "`n💡 Next Steps:"
Write-Host "   1. Add remote: git remote add origin <your-repo-url>"
Write-Host "   2. Push all branches:"
Write-Host "      git push -u origin main"
Write-Host "      git push -u origin basic-setup"
Write-Host "      git push -u origin kafka-integration"
Write-Host "      git push -u origin spark-batch"
Write-Host "      git push -u origin window-algorithm"

Write-Host "`n✅ Done!"
