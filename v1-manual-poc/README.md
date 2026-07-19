# Manual Data Pipeline Proof of Concept — S3, Lambda, Glue and QuickSight

## Project Summary

- **Designed and validated an end-to-end serverless data pipeline proof of concept entirely through the AWS Console**, covering raw CSV ingestion via S3, automated processing with Lambda, schema discovery with Glue Crawler, ETL transformation with a Glue PySpark job, and business intelligence visualization with Amazon QuickSight — establishing the functional blueprint for the infrastructure-as-code evolutions that followed.

- **Demonstrated automated data flow from raw upload to structured output** by configuring an S3 event notification to trigger a Lambda function on every CSV upload, which cleans and reformats the data before storing it in a processed S3 bucket — proving the core validation logic before it was hardened into production code in v2.

- **Validated the full analytics layer end-to-end** by running a Glue Crawler to infer the schema into the Glue Data Catalog, executing a Glue ETL job to produce the final transformed dataset, and connecting Amazon QuickSight to the output bucket to confirm the data was correctly structured for dashboarding and reporting.

---

## Architecture

<img width="778" height="715" alt="image" src="https://github.com/user-attachments/assets/bb90adf1-c3bb-461c-a8e0-636469314beb" />


## Tech Stack

| Layer | Service |
|---|---|
| Trigger | S3 Event Notification |
| Validation & Cleaning | AWS Lambda (Python) |
| ETL & Transformation | AWS Glue (PySpark) |
| Schema Discovery | AWS Glue Crawler + Data Catalog |
| Storage | Amazon S3 (raw / processed / final) |
| Visualization | Amazon QuickSight |
| Configuration | AWS Console (fully manual) |

---

## Purpose

This folder is a **proof of concept only** — all resources were created and configured manually through the AWS Console. There is no Infrastructure as Code here by design; the goal was to validate the architecture and data flow before investing in automation.

The lessons learned from this POC directly informed:
- **v2** — full CDK automation, KMS encryption, SQS DLQ, SNS alerting, X-Ray tracing and structured logging
- **v3** — event-driven orchestration with EventBridge and Step Functions, Parquet output, date partitioning, and Athena for SQL analysis
