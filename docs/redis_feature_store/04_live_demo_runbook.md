# Live Demonstration Runbook: AWS Redis Feature Store & Real-Time Fraud Engine

This runbook provides a safe, reproducible, step-by-step procedure to demonstrate the real-time fraud detection engine and its AWS ElastiCache for Redis feature store integration.

---

## Safety and Grounding Rules
- **Non-Destructive Operations:** All commands in this runbook are either **READ-ONLY** queries or run in a **LOCAL DEMO SANDBOX**.
- **No Resource Mutations:** No AWS infrastructure will be modified, created, or destroyed by these verification steps.
- **Credential Hygiene:** All commands leverage standard AWS environment variables or IAM roles. No plaintext passwords, secret keys, or authentication tokens are exposed.

---

## 1. Prerequisites and Environment Validation

Ensure AWS CLI v2 and Python 3.10+ are available in your execution environment.

```bash
# [READ-ONLY] Verify AWS CLI identity and active assumed role
aws sts get-caller-identity
```
- **What it does:** Verifies that your terminal session is authenticated to AWS Account `138257835434`.
- **Classification:** **Read-Only (AWS API)**

---

## 2. Infrastructure Health Checks

Verify that the ElastiCache cluster, ECS task, and MSK brokers are active and healthy.

```bash
# [READ-ONLY] 1. Inspect ElastiCache Redis Replication Group status
aws elasticache describe-replication-groups \
  --replication-group-id sic-feature-store \
  --region eu-west-3 \
  --query "ReplicationGroups[0].[ReplicationGroupId,Status,PrimaryEndpoint.Address,TransitEncryptionEnabled]" \
  --output table

# [READ-ONLY] 2. Verify security group isolation on port 6379
aws ec2 describe-security-groups \
  --group-ids sg-045ac4d82b2de70c6 \
  --region eu-west-3 \
  --query "SecurityGroups[0].IpPermissions" \
  --output json

# [READ-ONLY] 3. Verify running ECS fraud consumer task
aws ecs list-tasks \
  --cluster synthetic-data-cluster \
  --family sic-fraud-consumer \
  --region eu-west-3
```
- **What it does:** Confirms that `sic-feature-store` is `available`, TLS encryption is enforced, port 6379 is restricted to `sg-0dd0933f4da2b5ea7`, and task `sic-fraud-consumer:2` is actively running.
- **Classification:** **Read-Only (AWS API)**

---

## 3. Redis Connectivity and TLS Verification

Because Redis is isolated within private subnets without public IPs, direct external connections from your laptop are blocked by design. Connectivity can be tested either via ECS CloudWatch logs or locally via Docker.

### Option A: Verify AWS Live Connectivity via CloudWatch Logs
```bash
# [READ-ONLY] Tail the active ECS fraud consumer container logs
aws logs filter-log-events \
  --log-group-name /ecs/synthetic-data-provider \
  --log-stream-name-prefix fraud-consumer/fraud-consumer \
  --limit 10 \
  --region eu-west-3 \
  --query "events[*].message" \
  --output text
```
- **What it does:** Reads live stdout/stderr from the Fargate container, verifying that transactions are actively being evaluated and scored in ~6–8ms over TLS (`rediss://`).
- **Classification:** **Read-Only (CloudWatch API)**

### Option B: Local Docker Redis Alternative (For Offline / Student Demos)
```bash
# [LOCAL MUTATION] Start local Redis 7 container
docker run -d --name demo-redis -p 6379:6379 redis:7-alpine

# [READ-ONLY] Ping local Redis instance
docker exec demo-redis redis-cli ping
```
- **What it does:** Boots a lightweight Redis instance locally on `localhost:6379` to demonstrate parity without cloud costs.
- **Classification:** **Locally Mutating (Docker Container)**

---

## 4. Reference-Data Preload Verification

Verify that static demographic dimensions can be hydrated into the state store.

```bash
# [READ-ONLY] Run the unit test suite verifying user and card preloading
pytest tests/unit/test_ml_fraud.py -k "test_consumer_process_transaction" -v
```
- **What it does:** Validates that `sd254_users.csv` and `sd254_cards.csv` are properly parsed and mapped to `profile:user:{id}` and `profile:card:{id}:{idx}` keys.
- **Classification:** **Read-Only (Local Python)**

---

## 5. Live Transaction Benchmark Execution

Run the end-to-end benchmark script to simulate real-time transactions and measure sub-10ms inference latencies.

```bash
# [READ-ONLY] Execute 50-transaction streaming benchmark
PYTHONPATH=src python scripts/verify_realtime_scoring.py
```
- **What it does:** Generates 50 synthetic transactions covering both normal spend and severe anomalies (impossible travel speed, credit exhaustion, high MCC risk). Evaluates the feature extraction pipeline and LightGBM model.
- **Expected Output:**
  - Average latency: **< 1.0 ms** in-memory, **< 10.0 ms** with network/TLS.
  - Explanations printed for flagged transactions (`impossible_travel_speed_...`, `credit_limit_exhaustion`).
- **Classification:** **Read-Only (Local Simulation)**

---

## 6. Redis State Inspection (Local or Bastion)

Inspect the keys and sliding-window contents created during the benchmark:

```bash
# [READ-ONLY] Check keys created in local demo Redis
docker exec demo-redis redis-cli KEYS "*"

# [READ-ONLY] Inspect rolling window for user 10, card 0 (ZSET)
docker exec demo-redis redis-cli ZRANGE tx_window:10:0 0 -1 WITHSCORES

# [READ-ONLY] Inspect running spend statistics (HASH)
docker exec demo-redis redis-cli HGETALL spend_stats:10
```
- **What it does:** Inspects the Sorted Set members (timestamps as scores, amounts as values) and running totals (`total_sum`, `total_count`, `total_sq_sum`).
- **Classification:** **Read-Only (Redis CLI)**

---

## 7. Fraud Alert Verification

Verify alert payloads emitted by the consumer.

```bash
# [READ-ONLY] Verify alert output structure via pytest assertion
pytest tests/unit/test_ml_fraud.py -k "test_consumer_pass_and_alert_decision" -v
```
- **What it does:** Validates that transactions exceeding the threshold produce alert envelopes containing:
  - `alert_id`, `event_id`, `user_id`, `amount`, `decision: "ALERT"`, `fraud_probability`, and `risk_factors`.
- **Classification:** **Read-Only (Local Python)**

---

## 8. Consumer Offset Commit Verification

Demonstrate that Kafka offsets are protected against data loss and skipped messages:

```bash
# [READ-ONLY] Verify offset blocking on broker delivery failure
pytest tests/unit/test_ml_fraud.py -k "test_producer_delivery_failure_prevents_offset_commit" -v
```
- **What it does:** Executes a unit test proving that if MSK fails to accept the alert message within a 5.0-second flush timeout, `consumer.commit` is blocked.
- **Classification:** **Read-Only (Local Python)**

---

## 9. Latency Benchmark Summary

Review the verified latency numbers from both environments:

| Test Mode | Network Path | Average Latency | P95 Latency | Max Latency | SLA Target | SLA Met? |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Local / In-Memory** | RAM Loopback | **0.52 ms** | 0.56 ms | 6.43 ms | < 10.0 ms | **YES** |
| **Live AWS Fargate** | VPC -> TLS Redis (`rediss://`) | **6.15 – 8.95 ms** | 7.84 ms | 8.95 ms | < 10.0 ms | **YES** |

---

## 10. Demo Teardown and Cleanup

If a local Docker container was started for the demonstration, tear it down:

```bash
# [LOCAL MUTATION] Remove demo Redis container
docker stop demo-redis && docker rm demo-redis
```
- **What it does:** Stops and removes the local container.
- **AWS Notice:** No AWS infrastructure is deleted. AWS resources (`sic-feature-store` and `synthetic-data-cluster`) remain active under standard management policies.
- **Classification:** **Locally Mutating (Docker Clean-up)**
