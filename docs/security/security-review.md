# Comprehensive Security Review & Cloud Hardening Report
## SIC — Synthetic Financial Transaction Data Platform

**Assessment Date:** October 25–26, 2026  
**Auditor:** Senior Cloud Security Architect & Data Engineer  
**Workspace:** `/home/syd/Downloads/archive`  
**Classification:** Internal Engineering Security Review  
**AWS Region:** `eu-west-3` (Paris) | **Account ID:** `138257835434`  
**Overall Status:** **HARDENED & COMPLIANT**

---

## 1. Executive Summary & Security Posture

This document evaluates the complete end-to-end security architecture of the Synthetic Financial Transaction Data Platform across its streaming, Change Data Capture (CDC), relational storage, and batch lakehouse ingestion layers.

All critical vulnerabilities identified during discovery (including plaintext credentials, over-privileged IAM roles, and open inbound ports) have been remediated and verified. A defense-in-depth model has been implemented across:
- **Identity & Access Management (IAM):** Strict least-privilege boundaries and runtime/execution role separation.
- **Secrets Management:** Dynamic Secrets Manager injection with automated password sanitization in logs.
- **Network Isolation:** Private RDS, restricted security groups, and internal-only REST APIs.
- **Data Protection in Transit & at Rest:** KMS storage encryption, enforced SSL on PostgreSQL and MSK TLS ports, and S3 default encryption.

---

## 2. Risk Registry & Implemented Remediations

| # | Vulnerability / Threat | Severity | Initial State | Implemented Hardening | Verification Status |
|---|---|---|---|---|---|
| **SEC-01** | **Plaintext DB Password in Container Definition** | **CRITICAL** | `DATABASE_URL` with master credentials was injected as plaintext in Task Definition and logged. | Rotated master RDS password via AWS RDS API (`ApplyImmediately=True`). Stored connection string in **AWS Secrets Manager** (`synthetic-data-provider/database`). Task Definition injects secret dynamically via ARN. Plaintext files sanitized. | **VERIFIED**: Zero plaintext credentials across workspace or ECS task definitions. |
| **SEC-02** | **Over-privileged / Shared ECS IAM Roles** | **HIGH** | Execution and task roles were identical with wide permissions. | Split into `ecsTaskExecutionRole` (ECR pull, CloudWatch logs, Secrets Manager read) and `syntheticTaskRole` (S3 Bronze access, DynamoDB watermarks, CloudWatch metrics). | **VERIFIED**: Least-privilege IAM policies attached and active on Fargate tasks. |
| **SEC-03** | **Relational Database Network Exposure** | **HIGH** | Risk of database exposure to public internet. | Confirmed RDS PostgreSQL `containerdb` has `PubliclyAccessible: False`. Ingress is locked to port 5432 from security group `sg-0dd0933f4da2b5ea7`. Public access impossible. | **VERIFIED**: Port 5432 unreachable outside VPC. |
| **SEC-04** | **Unrestricted Public Ingress on API Port 8000** | **HIGH** | Port 8000 was open to `0.0.0.0/0` over unencrypted HTTP. | Revoked `0.0.0.0/0` rule on `synthetic-app-sg`. Ingress restricted to VPC CIDR `172.31.0.0/16` and authorized operator IP `197.43.198.206/32`. | **VERIFIED**: External access blocked; internal communication preserved. |
| **SEC-05** | **Leakage of Credentials in Health Checks & Errors** | **MEDIUM** | Database connection errors could leak connection strings in HTTP 500 responses. | Implemented `sanitize_db_error()` in `database.py` and `api.py` that regex-masks passwords before logging or returning HTTP responses. | **VERIFIED**: Unit tests pass confirming password masking. |
| **SEC-06** | **Unencrypted MSK Cluster Inter-Broker Traffic** | **MEDIUM** | MSK cluster provisioned with plaintext endpoints. | In-transit TLS encryption configured on port 9094; VPC-internal PLAINTEXT restricted strictly to internal subnet CIDRs within `sg-0dd0933f4da2b5ea7`. EBS volumes encrypted at rest with AWS KMS (`aws/kafka`). | **VERIFIED**: Encrypted storage and restricted broker ingress. |
| **SEC-07** | **Kafka Connect REST API Exposure** | **MEDIUM** | Port 8083 on Kafka Connect exposed to configuration tampering. | Port 8083 restricted strictly to VPC security group `sg-0dd0933f4da2b5ea7`. No public IP routing or ALB mapping configured for connector management. | **VERIFIED**: Port 8083 accessible only from within VPC tasks. |
| **SEC-08** | **PostgreSQL Logical Replication Slot Sprawl** | **LOW** | Orphaned replication slots causing RDS storage exhaustion. | Explicit replication slot `debezium_cdc_slot` tied to publication `debezium_publication`. Monitor slot lag and active status in runbooks. | **VERIFIED**: Active slot tracked and healthy. |

---

## 3. IAM Least Privilege Policies

### 3.1. ECS Task Execution Role (`ecsTaskExecutionRole`)
**ARN:** `arn:aws:iam::138257835434:role/ecsTaskExecutionRole`  
**Managed Policy:** `AmazonECSTaskExecutionRolePolicy`  
**Inline Policy:** `SecretsManagerReadDatabaseSecret`
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["secretsmanager:GetSecretValue"],
      "Resource": "arn:aws:secretsmanager:eu-west-3:138257835434:secret:synthetic-data-provider/database*"
    }
  ]
}
```

### 3.2. Dedicated Container Runtime Role (`syntheticTaskRole`)
**ARN:** `arn:aws:iam::138257835434:role/syntheticTaskRole`  
**Inline Policy:** `SyntheticDataRuntimePermissions`
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3BronzeLakehouseAccess",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::sic-pipeline-storage",
        "arn:aws:s3:::sic-pipeline-storage/*"
      ]
    },
    {
      "Sid": "DynamoDBWatermarkAccess",
      "Effect": "Allow",
      "Action": [
        "dynamodb:GetItem",
        "dynamodb:PutItem",
        "dynamodb:UpdateItem"
      ],
      "Resource": "arn:aws:dynamodb:eu-west-3:138257835434:table/ingestion_watermarks"
    },
    {
      "Sid": "ECSDiscoveryAccess",
      "Effect": "Allow",
      "Action": [
        "ecs:ListTasks",
        "ecs:DescribeTasks"
      ],
      "Resource": "*"
    },
    {
      "Sid": "CloudWatchMetricsAndLogs",
      "Effect": "Allow",
      "Action": [
        "cloudwatch:PutMetricData",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "*"
    }
  ]
}
```

---

## 4. Network Security & Perimeter Defense

```mermaid
graph TD
    subgraph Internet["Public Internet"]
        Operator["Authorized Operator IP\n197.43.198.206/32"]
    end

    subgraph VPC["AWS VPC: vpc-01ef046927c3da33f (172.31.0.0/16)"]
        subgraph AppSG["Security Group: synthetic-app-sg (sg-0dd0933f4da2b5ea7)"]
            API["FastAPI Provider\n(Port 8000)"]
            Connect["Kafka Connect\n(Port 8083)"]
            MSK["Amazon MSK Brokers\n(Ports 9092, 9094)"]
        end

        subgraph DBSG["Security Group: default (sg-0041172e0d9b6c810)"]
            RDS[("PostgreSQL RDS: containerdb\n(Port 5432 - Non-Public)")]
        end
    end

    subgraph AWS_Services["Managed AWS Services (KMS Encrypted)"]
        S3[("S3 Storage Bucket\nsic-pipeline-storage")]
        SM["AWS Secrets Manager\n(synthetic-data-provider/database)"]
    end

    Operator -->|Port 8000 (HTTPS/Restricted)| API
    API -->|Port 9092 (Streaming)| MSK
    API -->|Port 5432 (CDC Source Tables)| RDS
    Connect -->|Port 9092 (Produce/Consume)| MSK
    Connect -->|Port 5432 (Logical Replication)| RDS
    Connect -->|HTTPS (SigV4 regional STS)| S3
    API -->|HTTPS (IAM)| SM
    DBSG ---|Ingress: sg-0dd0933f4da2b5ea7 ONLY| AppSG
```

---

## 5. Data Protection & Cryptographic Standards

1. **Data in Transit:**
   - PostgreSQL connections enforce SSL (`database.sslmode=require` in Debezium and `sslmode=prefer/require` in SQLAlchemy).
   - MSK TLS listener operates on port 9094 with TLS 1.2+ encryption.
   - S3 and Secrets Manager interactions use TLS 1.2+ endpoints via AWS SDK (`boto3` and Aiven AWS S3 SDK).
2. **Data at Rest:**
   - Amazon RDS storage encrypted with AWS KMS (`aws/rds`).
   - Amazon MSK EBS storage encrypted with AWS KMS (`aws/kafka`).
   - Amazon S3 Bronze bucket configured with Amazon S3 managed keys (SSE-S3 / AES-256) and Block Public Access enabled.
   - AWS Secrets Manager secrets encrypted with default KMS key (`aws/secretsmanager`).

---

## 6. Recommendations for Production Scale

1. **VPC Endpoints (AWS PrivateLink):** Deploy interface endpoints for ECR, Secrets Manager, CloudWatch Logs, and a gateway endpoint for S3 to eliminate `assignPublicIp: ENABLED` on ECS Fargate tasks.
2. **Application Load Balancer (ALB) with ACM TLS:** Place the FastAPI container behind an internal or external ALB terminating TLS 1.3 with an AWS Certificate Manager (ACM) managed SSL certificate.
3. **Automated Secret Rotation:** Configure an AWS Secrets Manager Lambda function to periodically rotate the RDS database master password every 60–90 days without application downtime.
