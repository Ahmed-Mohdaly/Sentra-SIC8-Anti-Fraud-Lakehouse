# Amazon MSK Service Specification
## SIC — Synthetic Financial Transaction Data Platform

**Service:** Amazon Managed Streaming for Apache Kafka (Amazon MSK)  
**Cluster Name:** `sic-streaming-msk`  
**Region:** `eu-west-3` (Paris)  
**AWS Account ID:** `138257835434`  
**Status:** `ACTIVE` (Production Operation)

---

## 1. Cluster Identity & Configuration

| Parameter | Specification |
|---|---|
| **Cluster ARN** | `arn:aws:kafka:eu-west-3:138257835434:cluster/sic-streaming-msk/ff954d9e-7cba-437e-b180-83ad115d0996-2` |
| **Kafka Version** | Apache Kafka `3.6.0` |
| **Broker Instance Type** | `kafka.t3.small` (2 vCPU, 2 GiB memory per broker) |
| **Broker Count** | 2 Broker Nodes across 2 Availability Zones |
| **Storage per Broker** | 50 GiB EBS gp3 |
| **VPC & Subnets** | `vpc-01ef046927c3da33f` \| `subnet-003d3891dab36b354`, `subnet-0b18944bde732d1fd` |
| **Security Group** | `sg-0dd0933f4da2b5ea7` (Port 9092 PLAINTEXT, Port 9094 TLS) |
| **Zookeeper / Metadata Connect** | Zookeeper Connect String / KRaft enabled |

### Broker Endpoints:
- **PLAINTEXT (VPC Internal):**
  - `b-1.sicstreamingmsk.4io9um.c2.kafka.eu-west-3.amazonaws.com:9092`
  - `b-2.sicstreamingmsk.4io9um.c2.kafka.eu-west-3.amazonaws.com:9092`
- **TLS (Secure Internal):**
  - `b-1.sicstreamingmsk.4io9um.c2.kafka.eu-west-3.amazonaws.com:9094`
  - `b-2.sicstreamingmsk.4io9um.c2.kafka.eu-west-3.amazonaws.com:9094`

---

## 2. Topic Inventory & Topology

All topics are provisioned with **Replication Factor 2** (matching the 2-node cluster topology) to ensure fault tolerance across availability zones.

| Topic Name | Purpose / Data Domain | Partitions | RF | Min In-Sync Replicas | Cleanup Policy | Retention Period |
|---|---|---|---|---|---|---|
| `transactions` | High-throughput synthetic financial transaction events | 3 | 2 | 1 | `delete` | 7 days (`604800000 ms`) |
| `users` | Debezium CDC change events from RDS `public.users` | 3 | 2 | 1 | `delete` | 7 days (`604800000 ms`) |
| `cards` | Debezium CDC change events from RDS `public.cards` | 3 | 2 | 1 | `delete` | 7 days (`604800000 ms`) |
| `sic_connect_configs` | Kafka Connect distributed configuration store | 1 | 2 | 1 | `compact` | Infinite (Compacted) |
| `sic_connect_offsets` | Kafka Connect source & sink committed offsets | 25 | 2 | 1 | `compact` | Infinite (Compacted) |
| `sic_connect_status` | Kafka Connect distributed worker & task status | 5 | 2 | 1 | `compact` | Infinite (Compacted) |

---

## 3. Producer & Consumer Standards

### 3.1. Producer Configuration (`transactions`)
- **Producer Client:** `confluent-kafka-python`
- **Acks:** `1` (or `all` for strict durability)
- **Batching:** `queue.buffering.max.messages=100000`, `linger.ms=5`
- **Key Strategy:** `str(user_id).encode("utf-8")`
  - Guarantees hash-based deterministic partition routing so all events for a given user land in the same partition and preserve strict temporal ordering.

### 3.2. Consumer Configuration (Aiven S3 Sink)
- **Consumer Group:** `connect-aiven-s3-sink`
- **Session Timeout:** `45000 ms`
- **Heartbeat Interval:** `15000 ms`
- **Auto Offset Reset:** `earliest`

---

## 4. Operational Monitoring & Alerting

### Key CloudWatch Metrics:
1. `UnderReplicatedPartitions`: Must strictly equal `0`. Any value `> 0` indicates an offline broker or replication lag.
2. `BytesInPerSec` / `BytesOutPerSec`: Monitors producer and consumer throughput.
3. `CpuUser` / `CpuSystem`: Threshold alert at `80%` sustained over 5 minutes.
4. `KafkaDataLogsDiskUsed`: Disk utilization alert at `85%` to prevent broker read-only lockups.
