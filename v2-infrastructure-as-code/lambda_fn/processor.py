import csv
import io
import json
import logging
import os
from datetime import datetime, timezone

import boto3
from aws_xray_sdk.core import patch_all, xray_recorder

patch_all()  # auto-instrument all boto3 calls with X-Ray

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
sqs = boto3.client("sqs")

PROCESSED_BUCKET = os.environ["PROCESSED_BUCKET"]
PROCESSED_PREFIX = os.environ.get("PROCESSED_PREFIX", "orders/")
DLQ_URL = os.environ["DLQ_URL"]
PROCESSED_KMS_KEY_ARN = os.environ["PROCESSED_KMS_KEY_ARN"]


def normalize_date(date_str):
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(date_str.strip(), fmt).date()
        except ValueError:
            continue
    return None


@xray_recorder.capture("lambda_handler")
def lambda_handler(event, context):
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]

    logger.info(json.dumps({
        "event": "invocation_start",
        "source_bucket": bucket,
        "source_key": key,
        "request_id": context.aws_request_id,
    }))

    try:
        rows_written, output_key = _process(bucket, key)
        logger.info(json.dumps({
            "event": "processing_success",
            "source_key": key,
            "output_key": output_key,
            "rows_written": rows_written,
            "request_id": context.aws_request_id,
        }))
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "File processed successfully",
                "output_key": output_key,
                "rows_written": rows_written,
            }),
        }
    except Exception as exc:
        logger.error(json.dumps({
            "event": "processing_failed",
            "source_bucket": bucket,
            "source_key": key,
            "error": str(exc),
            "request_id": context.aws_request_id,
        }))
        sqs.send_message(
            QueueUrl=DLQ_URL,
            MessageBody=json.dumps({
                "bucket": bucket,
                "key": key,
                "error": str(exc),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "request_id": context.aws_request_id,
            }),
        )
        raise  # re-raise so Lambda marks the invocation as failed


@xray_recorder.capture("process_csv")
def _process(bucket: str, key: str) -> tuple[int, str]:
    # 1️⃣ Read raw CSV from S3
    logger.info(json.dumps({"event": "s3_read_start", "bucket": bucket, "key": key}))
    response = s3.get_object(Bucket=bucket, Key=key)
    raw_content = response["Body"].read().decode("utf-8")
    logger.info(json.dumps({"event": "s3_read_done", "key": key, "bytes": len(raw_content)}))

    reader = csv.DictReader(io.StringIO(raw_content))
    if not reader.fieldnames:
        raise ValueError("Empty or missing CSV headers")

    cleaned_rows = []
    skipped = 0

    for i, row in enumerate(reader, start=1):
        # Normalize date
        order_date = normalize_date(row["OrderDate"])
        if not order_date:
            logger.warning(json.dumps({"event": "row_skipped", "reason": "invalid_date", "row": i, "value": row["OrderDate"]}))
            skipped += 1
            continue

        # Normalize email
        email = row["CustomerEmail"].strip().lower() or "unknown@unknown.com"

        # Normalize country
        country = row["Country"].strip().upper()

        # Quantity validation
        try:
            quantity = int(row["Quantity"])
            if quantity <= 0:
                raise ValueError("non-positive quantity")
        except ValueError as e:
            logger.warning(json.dumps({"event": "row_skipped", "reason": "invalid_quantity", "row": i, "detail": str(e)}))
            skipped += 1
            continue

        # Unit price
        try:
            unit_price = float(row["UnitPrice"])
        except ValueError:
            logger.warning(json.dumps({"event": "row_skipped", "reason": "invalid_unit_price", "row": i, "value": row["UnitPrice"]}))
            skipped += 1
            continue

        # Total amount (fallback to computed value)
        try:
            total_amount = float(row["TotalAmount"])
        except (ValueError, TypeError):
            total_amount = quantity * unit_price
            logger.warning(json.dumps({"event": "total_amount_computed", "row": i, "computed": total_amount}))

        cleaned_rows.append({
            "order_id": int(row["OrderID"]),
            "order_date": order_date.isoformat(),
            "customer_email": email,
            "country": country,
            "product": row["Product"].strip(),
            "quantity": quantity,
            "unit_price": unit_price,
            "total_amount": total_amount,
            "year": order_date.year,
            "month": f"{order_date.month:02d}",
            "day": f"{order_date.day:02d}",
        })

    logger.info(json.dumps({
        "event": "cleaning_done",
        "rows_accepted": len(cleaned_rows),
        "rows_skipped": skipped,
    }))

    if not cleaned_rows:
        raise ValueError("No valid rows found after cleaning")

    # 2️⃣ Write cleaned CSV to processed bucket (always KMS-encrypted)
    output_buffer = io.StringIO()
    writer = csv.DictWriter(output_buffer, fieldnames=cleaned_rows[0].keys())
    writer.writeheader()
    writer.writerows(cleaned_rows)

    output_key = f"{PROCESSED_PREFIX}orders_processed_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}.csv"

    logger.info(json.dumps({"event": "s3_write_start", "bucket": PROCESSED_BUCKET, "key": output_key}))
    s3.put_object(
        Bucket=PROCESSED_BUCKET,
        Key=output_key,
        Body=output_buffer.getvalue(),
        ContentType="text/csv",
        ServerSideEncryption="aws:kms",
        SSEKMSKeyId=PROCESSED_KMS_KEY_ARN,
    )
    logger.info(json.dumps({"event": "s3_write_done", "key": output_key, "rows": len(cleaned_rows)}))

    return len(cleaned_rows), output_key
