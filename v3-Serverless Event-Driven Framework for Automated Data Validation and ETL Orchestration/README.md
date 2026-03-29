# Serverless Event-Driven Framework for Automated Data Validation and ETL Orchestration

## Project Summary

- **Architected a fully automated, event-driven data pipeline** on AWS, where every raw CSV upload to S3 automatically triggers an end-to-end orchestration workflow via Amazon EventBridge and AWS Step Functions — eliminating all manual intervention from ingestion to analytics-ready output.

- **Built a multi-stage data quality and transformation layer** using AWS Lambda for preliminary validation and cleansing (date normalization, email formatting, schema enforcement) and AWS Glue (PySpark) for complex ETL logic — with failures automatically routed to an SQS Dead-Letter Queue and SNS alerts sent to the team, ensuring full observability and zero silent data loss.

- **Delivered production-grade business intelligence outputs** by converting cleaned CSV data into partitioned Parquet format stored in S3, enabling cost-efficient SQL analysis via Amazon Athena — surfacing VIP customer segments ranked by total spend and regional market performance ranked by revenue.

---

## Architecture

<img width="1007" height="1096" alt="image" src="https://github.com/user-attachments/assets/60949992-a361-4f58-81a4-6d34ea79a9aa" />





## Tech Stack

| Layer | Service |
|---|---|
| Trigger | Amazon EventBridge |
| Orchestration | AWS Step Functions |
| Validation & Cleaning | AWS Lambda (Python 3.12) |
| ETL & Transformation | AWS Glue (PySpark, Glue 4.0) |
| Schema Discovery | AWS Glue Crawler + Data Catalog |
| Storage | Amazon S3 (raw / processed / final) |
| Analytics | Amazon Athena |
| Encryption | AWS KMS (one key per bucket) |
| Alerting | Amazon SNS + SQS Dead-Letter Queue |
| Observability | AWS CloudWatch + AWS X-Ray |
| Infrastructure as Code | AWS CDK (Python) |

---

## Security

- Least privilege IAM roles per service — Lambda, Glue Crawler, Glue Job, Step Functions and EventBridge each have their own scoped role
- All S3 buckets are private, SSL-enforced, and encrypted with dedicated KMS keys
- SQS DLQ and SNS topic encrypted with a shared KMS key
- Athena query results encrypted with KMS and stored in a dedicated results bucket

---

## Deploy

```bash
cd "v3-Serverless Event-Driven Framework for Automated Data Validation and ETL Orchestration"
py -m pip install -r requirements.txt
cdk bootstrap   # first time only
cdk deploy

# Upload the Glue ETL script after deploy
aws s3 cp lambda_fn\etl_job.py s3://glue-scripts-nak76700/scripts/etl_job.py
```

Confirm the SNS subscription email sent to your inbox after deploy.

## Tear Down

```bash
cdk destroy

# Then manually delete the retained S3 buckets
aws s3 rm s3://raw-file-pipeline-nak76700 --recursive && aws s3 rb s3://raw-file-pipeline-nak76700
aws s3 rm s3://processed-file-pipeline-nak76700 --recursive && aws s3 rb s3://processed-file-pipeline-nak76700
aws s3 rm s3://final-file-pipeline-nak76700 --recursive && aws s3 rb s3://final-file-pipeline-nak76700
```
