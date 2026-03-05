#!/bin/bash

# Wait for HDFS to be ready
echo "Waiting for HDFS to be ready..."
sleep 10

# Create the logs directory and subdirectories
echo "Creating /logs/processed directory..."
hdfs dfs -mkdir -p /logs/processed

# Set permissions to 777 as requested
echo "Setting permissions for /logs..."
hdfs dfs -chmod -R 777 /logs

echo "HDFS preparation complete."
hdfs dfs -ls -R /
