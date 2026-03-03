# Partition Strategy: Hash-Based vs Round-Robin

_Person 2 – Kafka Cluster Manager | Distributed Log Analytics Pipeline_

---

## Overview

The `service-logs` topic has **3 partitions** spread across **3 brokers**.  
Messages arrive from Person 1's log producers, which can choose between two
partitioning strategies by simply setting (or omitting) a Kafka **message key**.

---

## Strategy A – Hash-Based Partitioning (Keyed)

### How It Works

The producer sets `key = <service_name>` for every message.  
Kafka's default partitioner applies **MurmurHash2** to the key and assigns the
message to a deterministic partition:

```
partition = murmur2(key) % num_partitions
```

Our custom `hash_partitioner` in `test_produce.py` makes this explicit.

### Partition Assignment Example (3 partitions)

| Service          | Hash mod 3 | Partition |
|------------------|-----------|-----------|
| web_server       | 2         | 2         |
| auth_service     | 1         | 1         |
| payment_service  | 0         | 0         |
| database         | 2         | 2         |
| api_gateway      | 1         | 1         |
| notification_svc | 0         | 0         |

### Advantages

| # | Benefit |
|---|---------|
| 1 | **Message ordering per service** – all `web-server` logs arrive in sequence to Partition 2; the Spark consumer can safely aggregate them in order |
| 2 | **Efficient joins** – Person 3's Spark job can join streams partition-locally without a shuffle when two keyed topics share the same key |
| 3 | **Hot-partition diagnosis** – if one service is extremely noisy, it will always show up on the same partition in `monitor_cluster.py`, making it easy to spot |
| 4 | **Idempotency** – re-runs of the producer always land on the same partition, making debugging reproducible |

### Disadvantages

| # | Drawback |
|---|---------|
| 1 | **Hot partitions** – if `web-server` generates 90 % of traffic, Partition 2 becomes a bottleneck; the other 5 partitions sit mostly idle |
| 2 | **Uneven broker load** – since each partition has exactly one leader broker, overloaded partitions translate to overloaded brokers |
| 3 | **Key space must be known** – requires agreement on the key format between Person 1's producers and this cluster layer |

---

## Strategy B – Round-Robin Partitioning (Unkeyed)

### How It Works

The producer sends messages with **no key** (`key=None`).  
Kafka's default partitioner cycles through partitions in order:

```
message 1 → partition 0
message 2 → partition 1
...
message 6 → partition 5
message 7 → partition 0  (wraps)
```

### Advantages

| # | Benefit |
|---|---------|
| 1 | **Perfect load balancing** – each partition receives roughly the same number of messages regardless of which service generated them |
| 2 | **Broker load distribution** – leader partitions spread evenly → all 3 brokers handle equal throughput |
| 3 | **No coordination required** – Person 1 needs no knowledge of service names or key schemas |
| 4 | **Better throughput at peak load** – all 3 brokers contribute equally when traffic spikes |

### Disadvantages

| # | Drawback |
|---|---------|
| 1 | **No ordering guarantee** – consecutive logs from `payment-service` can land on different partitions; Person 3's Spark job cannot rely on partition-level order |
| 2 | **Stateful aggregations require global shuffle** – if Spark needs all `payment-service` events together (e.g., error-rate per service), it must shuffle across partitions |
| 3 | **Harder debugging** – a single service's trace is scattered across all 6 partitions |

---

## Switching Between Strategies

No cluster restart needed — the strategy is controlled **at the producer**.

### Use Hash Partitioning (default)
```bash
# test producer
python scripts/test_produce.py --strategy hash

# Person 1 code: set partition key
producer.send("service-logs", key=b"web_server", value=payload)
```

### Use Round-Robin
```bash
# test producer
python scripts/test_produce.py --strategy round_robin

# Person 1 code: omit key
producer.send("service-logs", value=payload)
```

A `PARTITION_STRATEGY` environment variable can be added to Person 1's
`docker-compose.yml` service definition when this project is integrated,
allowing a runtime switch without code changes.

---

## When to Choose Which Strategy

| Scenario | Recommended Strategy |
|----------|----------------------|
| Need per-service log ordering | **Hash** |
| Services have very unequal traffic | **Round-Robin** |
| Spark does stateful joins by service | **Hash** |
| Maximum write throughput is priority | **Round-Robin** |
| Unknown service distribution (early dev) | Start **Round-Robin**, switch to Hash after profiling |

---

## Implementation Reference

| File | Role |
|------|------|
| `scripts/test_produce.py` | `--strategy hash` or `--strategy round_robin` |
| `scripts/monitor_cluster.py` | Shows per-partition message rates live |
| `scripts/show_leaders.py` | Shows which broker leads which partition |
| `docker-compose.yml` | 3 partitions configured in `KAFKA_NUM_PARTITIONS` |

---

_For integration questions, see `README.md` Integration Points section._
