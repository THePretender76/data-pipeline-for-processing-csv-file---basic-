# Automated CSV Data Pipeline with AWS CDK — Infrastructure as Code

## Project Summary

- **Provisioned a fully automated, serverless data pipeline as Infrastructure as Code using AWS CDK (Python)**, replacing all manual AWS Console configuration with a single `cdk deploy` command — covering S3 ingestion, Lambda-triggered processing, Glue schema discovery, ETL transformation, and QuickSight-ready output across three isolated S3 buckets.

- **Implemented a production-grade data quality layer using AWS Lambda (Python 3.12)** that automatically triggers on every CSV upload, performing date normalization, email formatting, country standardization, and numeric validation — with invalid files automatically routed to an SQS Dead-Letter Queue and CloudWatch alarms sending SNS email alerts to the team, ensuring no data loss goes unnoticed.

- **Delivered structured business intelligence outputs** by running a Glue PySpark ETL job that aggregates cleaned order data into two analytical datasets — VIP customers ranked by total spend and regional market performance ranked by revenue — written as CSV to a final S3 bucket and visualized in Amazon QuickSight.

---

## Architecture

```
S3 (raw CSV upload)
  └── S3 Event Notification
        └── Lambda (validate + clean CSV) ──FAIL──→ SQS DLQ → CloudWatch Alarm → SNS email
              └── S3 (processed bucket, orders/)
                    └── Glue Crawler → Glue Data Catalog
                          └── Glue ETL Job (PySpark)
                                └── S3 (final bucket)
                                      └── Amazon QuickSight
```

---

## Tech Stack

| Layer | Service |
|---|---|
| Trigger | S3 Event Notification |
| Validation & Cleaning | AWS Lambda (Python 3.12) |
| ETL & Transformation | AWS Glue (PySpark, Glue 4.0) |
| Schema Discovery | AWS Glue Crawler + Data Catalog |
| Storage | Amazon S3 (raw / processed / final / scripts) |
| Visualization | Amazon QuickSight |
| Encryption | AWS KMS (one key per bucket) |
| Alerting | Amazon SNS + SQS Dead-Letter Queue + CloudWatch Alarm |
| Observability | AWS CloudWatch Logs + AWS X-Ray (structured JSON logging) |
| Infrastructure as Code | AWS CDK (Python) |

---

## Security

- Least privilege IAM roles per service — Lambda, Glue Crawler, and Glue Job each have their own scoped role with only the permissions they need
- All S3 buckets are private, SSL-enforced, and encrypted with dedicated KMS keys with automatic key rotation
- SQS DLQ and SNS topic encrypted with a shared KMS key
- Every `s3.put_object` call explicitly enforces `SSEKMSKeyId` — no unencrypted writes possible

---

## Observability

- Structured JSON logs at every step of the Lambda execution (file read, row-level skips with reasons, write confirmation)
- `request_id` threaded through all log entries and DLQ messages for end-to-end correlation in CloudWatch Logs Insights
- AWS X-Ray active tracing with named subsegments (`lambda_handler`, `process_csv`) for flame graph visualisation
- CloudWatch alarm fires within 5 minutes of any file landing in the DLQ

---

## Deploy

```bash
cd v2-infrastructure-as-code
py -m pip install -r requirements.txt
cdk bootstrap   # first time only
cdk deploy

# Upload the Glue ETL script after deploy
aws s3 cp lambda_fn\etl_job.py s3://glue-scripts-nak76700/scripts/etl_job.py

# Run the Glue Crawler manually after uploading a CSV
aws glue start-crawler --name pipeline-crawler-nak76700

# Once the crawler finishes, run the ETL job
aws glue start-job-run --job-name pipeline-etl-job-nak76700
```

Confirm the SNS subscription email sent to your inbox after deploy.

---

## Tear Down

```bash
cdk destroy

# Then manually delete the retained S3 buckets
aws s3 rm s3://raw-file-pipeline-nak76700 --recursive && aws s3 rb s3://raw-file-pipeline-nak76700
aws s3 rm s3://processed-file-pipeline-nak76700 --recursive && aws s3 rb s3://processed-file-pipeline-nak76700
aws s3 rm s3://final-file-pipeline-nak76700 --recursive && aws s3 rb s3://final-file-pipeline-nak76700
```
