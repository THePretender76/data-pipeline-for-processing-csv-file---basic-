# Enterprise-Grade Serverless Data Platform with CI/CD, Multi-Stage Environments, and Proactive Security Governance

## Project Summary

Building on v3's event-driven pipeline, v4 introduces four major enterprise capabilities:

- **CI/CD automation** via AWS CodePipeline + CodeBuild — every git push automatically runs tests and deploys to the correct environment, eliminating all manual `cdk deploy` commands
- **Multi-stage environments** (dev / staging / prod) — fully isolated AWS environments with separate S3 buckets, KMS keys, Glue jobs and IAM roles per stage, controlled through CDK environment-aware stacks
- **Streaming ingestion** via Amazon Kinesis Data Streams — replaces the S3 file upload trigger with real-time event processing, moving from batch file-by-file ingestion to sub-second latency data delivery
- **Data Lakehouse** via Apache Iceberg tables — replaces plain Parquet output with ACID-compliant Iceberg tables enabling schema evolution, time travel queries and partition pruning for analytics at scale
- **Proactive security governance** via AWS Lake Formation, Amazon Macie, VPC endpoints and AWS Config — centralizes data access control and adds automated sensitive data detection and compliance monitoring

---

## Architecture Evolution from v3

```
v3 Architecture (replaced/retained)
  S3 (raw CSV upload) ← replaced by Kinesis Data Streams
    └── EventBridge → Step Functions → Lambda → Glue (Parquet) ← replaced by Iceberg
          └── Athena

v4 Full Architecture
  ├── Kinesis Data Streams (real-time ingestion)
  │     └── Lambda consumer → validates + cleans → S3 processed
  │
  ├── EventBridge → Step Functions (orchestration retained)
  │     ├── Lambda          → validate + clean → S3 (processed)
  │     ├── Glue Crawler 1  → scan processed bucket → Glue Data Catalog
  │     ├── Glue ETL Job    → transform → S3 (final, Iceberg format)
  │     ├── Glue Crawler 2  → scan final bucket → Glue Data Catalog
  │     └── CATCH           → SNS alert + SQS DLQ
  │
  ├── CodePipeline (CI/CD)
  │     ├── Source stage    → CodeCommit / GitHub
  │     ├── Build stage     → CodeBuild (cdk synth + pytest)
  │     ├── Deploy Dev      → cdk deploy (dev environment)
  │     ├── Manual Approval → SNS notification to team
  │     └── Deploy Prod     → cdk deploy (prod environment)
  │
  ├── Lake Formation (Governance)
  │     ├── Data Lake Admin → centralized permission management
  │     ├── Column-level    → hide sensitive columns (customer_email)
  │     ├── Row-level       → filter rows by country per team
  │     ├── Data lineage    → track data flow across pipeline
  │     └── LF-TBAC         → tag-based access control (sensitive=true)
  │
  └── Security Layer
        ├── Amazon Macie    → sensitive data detection in S3
        ├── VPC Endpoints   → private communication between S3 and Glue
        └── AWS Config      → compliance monitoring and rule enforcement
```

---

## Tech Stack

| Layer | Service | What's New |
|---|---|---|
| CI/CD | AWS CodePipeline + CodeBuild | ✅ New |
| Source Control | AWS CodeCommit / GitHub | ✅ New |
| Multi-env | CDK Environments (dev/staging/prod) | ✅ New |
| Streaming Ingestion | Amazon Kinesis Data Streams | ✅ New |
| Data Lakehouse | Apache Iceberg (via Glue 4.0) | ✅ New |
| Governance | AWS Lake Formation | ✅ New |
| Data Access Control | Lake Formation (column + row level + LF-TBAC) | ✅ New |
| Sensitive Data Detection | Amazon Macie | ✅ New |
| Private Networking | VPC Endpoints (S3, Glue) | ✅ New |
| Compliance | AWS Config Rules | ✅ New |
| Trigger | Amazon EventBridge | Retained |
| Orchestration | AWS Step Functions | Retained |
| Validation & Cleaning | AWS Lambda (Python 3.12) | Retained |
| ETL & Transformation | AWS Glue (PySpark, Glue 4.0) | Retained |
| Schema Discovery | AWS Glue Crawler + Data Catalog | Retained |
| Storage | Amazon S3 (raw / processed / final) | Retained |
| Analytics | Amazon Athena | Retained |
| Encryption | AWS KMS | Retained |
| Alerting | Amazon SNS + SQS Dead-Letter Queue | Retained |
| Observability | AWS CloudWatch + AWS X-Ray | Retained |
| Infrastructure as Code | AWS CDK (Python) | Retained |

---

## WAF Pillars Addressed

| Pillar | Feature |
|---|---|
| Operational Excellence | CI/CD pipeline, automated deployments, multi-stage environment promotion |
| Performance Efficiency | Kinesis real-time streaming ingestion, Iceberg partition pruning and time travel |
| Security | Lake Formation column/row-level security, Macie sensitive data detection, VPC endpoints, AWS Config |
| Reliability | Manual approval gate before prod, automated rollback on failure, ACID transactions via Iceberg |
| Cost Optimization | Iceberg partition pruning reduces Athena scan costs, dev environment uses smaller Glue worker types (G.025X) |

---

## What Lake Formation Adds Over v3

In v3, data access is controlled through scattered IAM roles and KMS key grants per service (Lambda, Glue, Crawler). As the number of consumers grows (more Athena users, more Glue jobs, more teams), this becomes unmanageable. Lake Formation centralizes all of that:

- **Column-level security** — restrict access to specific columns (e.g. hide `customer_email`) without touching any S3 bucket policy
- **Row-level filtering** — restrict which rows a user can see (e.g. a team only sees their country's data)
- **Centralized permissions** — one place to manage who can access which database, table or column in the Glue Data Catalog
- **Data lineage** — track where data comes from and how it flows through the pipeline
- **Tag-based access control (LF-TBAC)** — tag tables/columns as `sensitive=true` and apply policies at the tag level

---

## What Iceberg Adds Over Plain Parquet (v3)

In v3, the Glue ETL job writes plain Parquet files to S3. Iceberg replaces this with a full table format:

- **ACID transactions** — no partial writes or corrupt reads during concurrent Glue job runs
- **Schema evolution** — add, rename or drop columns without rewriting the entire dataset
- **Time travel queries** — query the state of the data at any point in the past directly from Athena
- **Partition pruning** — Iceberg's hidden partitioning significantly reduces Athena scan costs at scale

---

## Environment Strategy

| Setting | Dev | Staging | Prod |
|---|---|---|---|
| Suffix | `nak76700-dev` | `nak76700-stg` | `nak76700-prod` |
| Glue workers | 2 x G.025X | 2 x G.025X | 2 x G.1X |
| Removal policy | DESTROY | DESTROY | RETAIN |
| Manual approval | No | No | Yes |
| Lake Formation | Enabled | Enabled | Enabled |
| Macie | Disabled | Enabled | Enabled |

---

## Deploy

```bash
cd "v4-Enterprise-Grade Serverless Data Platform with CI-CD, Multi-Stage Environments, and Proactive Security Governance"
py -m pip install -r requirements.txt
cdk bootstrap aws://ACCOUNT_ID/us-east-1

# CI/CD pipeline stack (deploys itself and then manages all other stacks)
cdk deploy PipelineCICDStack
```

After the CI/CD stack is deployed, all future deployments are triggered automatically by pushing to the main branch.

---

## Tear Down

```bash
cdk destroy PipelineCICDStack

# Then manually delete retained prod S3 buckets
aws s3 rm s3://raw-file-pipeline-nak76700-prod --recursive && aws s3 rb s3://raw-file-pipeline-nak76700-prod
aws s3 rm s3://processed-file-pipeline-nak76700-prod --recursive && aws s3 rb s3://processed-file-pipeline-nak76700-prod
aws s3 rm s3://final-file-pipeline-nak76700-prod --recursive && aws s3 rb s3://final-file-pipeline-nak76700-prod
```
