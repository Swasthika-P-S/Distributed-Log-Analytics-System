# Basic Setup - Kafka & Zookeeper

This folder contains the basic infrastructure setup.

## What's included:
- Zookeeper
- Kafka broker
- Test script to verify connectivity

## How to run:
```bash
docker-compose up -d
python test-connection.py
```

## Verify:
- Kafka should be running on localhost:9092
- Zookeeper on localhost:2181
