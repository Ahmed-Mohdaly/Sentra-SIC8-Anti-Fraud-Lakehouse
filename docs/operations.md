# SIC Platform — Operations Runbook

## 1. Running Application Components

### A. Synthetic Data Generator Service
The generator can run as a long-running streaming service or in batch generation mode.

**Streaming Mode (Continuous Generation to MSK):**
```bash
python scripts/run_generator.py --mode stream --tps 25.0
```
Or trigger via the FastAPI management API:
```bash
curl -X POST http://localhost:8000/api/v1/generator/start?target_tps=25.0
```

**Batch Generation (Synchronous & Verified Delivery):**
```bash
curl -X POST "http://localhost:8000/api/v1/generator/generate?count=50"
```
*Note: In batch mode, the service produces messages, calls `publisher.flush(10.0)`, and raises an error if broker delivery confirmation is not received for all records.*

### B. Batch Reference Extractor (MCC & Cities)
The batch extractor runs as a standalone Python process:
```bash
python -m ingestion.batch_reference_extractor \
  --db-url "$DATABASE_URL" \
  --s3-bucket "sic-pipeline-storage" \
  --region "eu-west-3"
```
Or execute manually as an ECS task:
```bash
aws ecs run-task \
  --cluster sic-streaming-cluster \
  --task-definition sic-admin-runner:1 \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[subnet-0a6237f02d0cf3fdf],securityGroups=[sg-0ffebba98877bc944],assignPublicIp=ENABLED}" \
  --overrides '{"containerOverrides": [{"name": "admin-runner", "command": ["python", "-m", "ingestion.batch_reference_extractor"]}]}' \
  --region eu-west-3
```

---

## 2. Running Tests & End-to-End Validation

### A. Unit Tests
Run the entire unit test suite locally using the project's virtualenv:
```bash
pytest tests/unit/ -v
```
All 96 unit tests run in memory without requiring external network, database, or AWS connectivity.

### B. End-to-End Live Validation
The live validation script tests the complete pipeline end to end against live AWS infrastructure (RDS PostgreSQL, MSK, Debezium CDC, Aiven S3 Sink, S3 Bronze):
```bash
python scripts/run_e2e_live_validation.py
```
Because the MSK broker endpoints and RDS database reside inside private VPC subnets, the script should be run either:
1. Within an ECS task inside the VPC (`sic-admin-runner`), or
2. From a host with VPC peering / VPN access and active AWS credentials.

The script performs the following automated verifications:
1. Inserts 52 Users & Cards into PostgreSQL -> confirms Debezium CDC captures change events to MSK topics `users` and `cards`.
2. Generates 55 Transactions directly to MSK topic `transactions`.
3. Verifies that transactions are strictly **absent** from PostgreSQL and that table counts are unchanged.
4. Executes the batch reference extractor for `mcc` and `cities`.
5. Polls S3 Bronze bucket and decompresses the generated gzip JSONL files to verify that specific test transaction IDs and CDC user IDs are present under `bronze/`.

---

## 3. Operational Troubleshooting & Diagnostics

### A. Inspecting Amazon MSK
- **Cluster Status:**
  ```bash
  aws kafka describe-cluster --cluster-arn <MSK_CLUSTER_ARN> --region eu-west-3
  ```
- **Broker Endpoints:**
  Plaintext broker endpoints reside at:
  `b-1.sicstreamingmsk.4io9um.c2.kafka.eu-west-3.amazonaws.com:9092`
  `b-2.sicstreamingmsk.4io9um.c2.kafka.eu-west-3.amazonaws.com:9092`

### B. Inspecting Kafka Connect & Connectors
Kafka Connect runs inside ECS service `sic-kafka-connect-service` on port 8083.
- **Check Connector Statuses:**
  ```bash
  curl -s http://172.31.10.236:8083/connectors?expand=status | jq .
  ```
  Both `debezium-postgres-source` and `aiven-s3-sink` should report `"state": "RUNNING"`.
- **Restart a Failed Connector Task:**
  ```bash
  curl -s -X POST http://172.31.10.236:8083/connectors/aiven-s3-sink/restart?includeTasks=true
  ```

### C. Inspecting S3 Bronze Prefixes
Verify that objects are landing in the correct Bronze partitions:
```bash
aws s3 ls s3://sic-pipeline-storage/bronze/ --recursive --human-readable --summarize
```
Expected prefixes:
- `bronze/transactions/partition=0/`
- `bronze/users/partition=0/`
- `bronze/cards/partition=0/`
- `bronze/mcc/`
- `bronze/cities/`

### D. Inspecting ECS Task Failures & Quotas
If an ECS task fails to launch or terminates unexpectedly:
1. **Describe Stopped Tasks:**
   ```bash
   aws ecs list-tasks --cluster sic-streaming-cluster --desired-status STOPPED --region eu-west-3
   aws ecs describe-tasks --cluster sic-streaming-cluster --tasks <TASK_ARN> --region eu-west-3 --query "tasks[0].stoppedReason"
   ```
2. **Check CloudWatch Logs:**
   ```bash
   aws logs tail /ecs/synthetic-data-service --follow --region eu-west-3
   aws logs tail /ecs/sic-kafka-connect --follow --region eu-west-3
   ```
3. **Verify Fargate vCPU Quota:**
   If task launches fail with `ResourceInitializationError` or quota exhaustion, check the active Fargate vCPU quota:
   ```bash
   aws service-quotas get-service-quota \
     --service-code fargate \
     --quota-code L-720C7AEB \
     --region eu-west-3
   ```
   *Note: In `eu-west-3`, the default Fargate quota is 2.0 vCPUs. The two long-running services (`synthetic-data-service` and `sic-kafka-connect-service`) utilize 1.0 vCPU each, fully occupying the 2.0 vCPU quota. To run ad-hoc tasks (such as `sic-admin-runner`), either scale down the synthetic generator to 0 tasks temporarily or request a quota increase from AWS Support.*

### E. Refreshing Expired AWS CLI Credentials
When temporary session credentials expire (manifesting as `ExpiredToken` on AWS CLI commands):
1. Re-authenticate via the organization's single sign-on or credential provider:
   ```bash
   aws sso login
   ```
   Or export refreshed temporary STS credentials in the shell session:
   ```bash
   export AWS_ACCESS_KEY_ID="<NEW_KEY_ID>"
   export AWS_SECRET_ACCESS_KEY="<NEW_SECRET_KEY>"
   export AWS_SESSION_TOKEN="<NEW_SESSION_TOKEN>"
   ```
2. Verify connectivity with a safe read-only call:
   ```bash
   aws sts get-caller-identity
   ```
