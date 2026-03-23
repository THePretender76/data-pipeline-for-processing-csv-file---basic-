from aws_cdk import (
    Stack, RemovalPolicy, Duration,
    aws_s3 as s3,
    aws_kms as kms,
    aws_lambda as _lambda,
    aws_s3_notifications as s3n,
    aws_sqs as sqs,
    aws_glue as glue,
    aws_iam as iam,
    aws_sns as sns,
    aws_sns_subscriptions as subscriptions,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cw_actions,
)
from constructs import Construct

SUFFIX = "nak76700"
ALERT_EMAIL = "nak76700.awsproject@outlook.com"


class DataPipelineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ── KMS keys (one per bucket + one shared for SQS/SNS) ──────────────
        raw_key = kms.Key(self, "RawKey", enable_key_rotation=True, removal_policy=RemovalPolicy.DESTROY)
        processed_key = kms.Key(self, "ProcessedKey", enable_key_rotation=True, removal_policy=RemovalPolicy.DESTROY)
        final_key = kms.Key(self, "FinalKey", enable_key_rotation=True, removal_policy=RemovalPolicy.DESTROY)
        shared_key = kms.Key(self, "SharedKey", enable_key_rotation=True, removal_policy=RemovalPolicy.DESTROY)

        # ── S3 buckets ───────────────────────────────────────────────────────
        raw_bucket = s3.Bucket(
            self, "RawBucket",
            bucket_name=f"raw-file-pipeline-{SUFFIX}",
            encryption=s3.BucketEncryption.KMS,
            encryption_key=raw_key,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.RETAIN,
        )

        processed_bucket = s3.Bucket(
            self, "ProcessedBucket",
            bucket_name=f"processed-file-pipeline-{SUFFIX}",
            encryption=s3.BucketEncryption.KMS,
            encryption_key=processed_key,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.RETAIN,
        )

        final_bucket = s3.Bucket(
            self, "FinalBucket",
            bucket_name=f"final-file-pipeline-{SUFFIX}",
            encryption=s3.BucketEncryption.KMS,
            encryption_key=final_key,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # ── SQS Dead-Letter Queue ────────────────────────────────────────────
        dlq = sqs.Queue(
            self, f"DLQ{SUFFIX}",
            queue_name=f"pipeline-dlq-{SUFFIX}",
            encryption=sqs.QueueEncryption.KMS,
            encryption_master_key=shared_key,
            retention_period=Duration.days(14),
        )

        # ── SNS topic + email subscription ──────────────────────────────────
        alert_topic = sns.Topic(
            self, f"AlertTopic{SUFFIX}",
            topic_name=f"pipeline-alerts-{SUFFIX}",
            master_key=shared_key,
        )
        alert_topic.add_subscription(subscriptions.EmailSubscription(ALERT_EMAIL))

        # ── CloudWatch alarm: DLQ has messages → SNS ────────────────────────
        dlq_alarm = cloudwatch.Alarm(
            self, f"DLQAlarm{SUFFIX}",
            alarm_name=f"pipeline-dlq-not-empty-{SUFFIX}",
            metric=dlq.metric_approximate_number_of_messages_visible(
                period=Duration.minutes(5)
            ),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            alarm_description="One or more bad files landed in the pipeline DLQ",
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )
        dlq_alarm.add_alarm_action(cw_actions.SnsAction(alert_topic))

        # ── Lambda IAM role (least privilege) ────────────────────────────────
        lambda_role = iam.Role(
            self, f"LambdaRole{SUFFIX}",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ],
        )
        raw_key.grant_decrypt(lambda_role)
        processed_key.grant_encrypt(lambda_role)
        shared_key.grant_encrypt_decrypt(lambda_role)
        raw_bucket.grant_read(lambda_role)
        processed_bucket.grant_write(lambda_role)
        dlq.grant_send_messages(lambda_role)
        # X-Ray: allow Lambda to emit trace segments
        lambda_role.add_to_policy(iam.PolicyStatement(
            actions=["xray:PutTraceSegments", "xray:PutTelemetryRecords"],
            resources=["*"],
        ))

        # ── Lambda layer: aws-xray-sdk ────────────────────────────────────────
        xray_layer = _lambda.LayerVersion(
            self, f"XRayLayer{SUFFIX}",
            layer_version_name=f"aws-xray-sdk-{SUFFIX}",
            code=_lambda.Code.from_asset(
                "lambda_fn",
                bundling={
                    "image": _lambda.Runtime.PYTHON_3_12.bundling_image,
                    "command": [
                        "bash", "-c",
                        "pip install aws-xray-sdk -t /asset-output/python && echo done",
                    ],
                },
            ),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_12],
        )

        # ── Lambda function ──────────────────────────────────────────────────
        processor_fn = _lambda.Function(
            self, f"ProcessorFn{SUFFIX}",
            function_name=f"csv-processor-{SUFFIX}",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="processor.lambda_handler",
            code=_lambda.Code.from_asset("lambda_fn"),
            layers=[xray_layer],
            role=lambda_role,
            timeout=Duration.minutes(5),
            tracing=_lambda.Tracing.ACTIVE,
            environment={
                "PROCESSED_BUCKET": processed_bucket.bucket_name,
                "PROCESSED_PREFIX": "orders/",
                "DLQ_URL": dlq.queue_url,
                "PROCESSED_KMS_KEY_ARN": processed_key.key_arn,
            },
        )

        # Trigger Lambda on every S3 PUT in the raw bucket
        raw_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(processor_fn),
        )

        # ── Glue database ────────────────────────────────────────────────────
        glue_db = glue.CfnDatabase(
            self, f"GlueDB{SUFFIX}",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=f"pipeline_catalog_{SUFFIX}",
                description="Data Catalog for the CSV pipeline",
            ),
        )

        # ── Glue Crawler IAM role ────────────────────────────────────────────
        crawler_role = iam.Role(
            self, f"CrawlerRole{SUFFIX}",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
            ],
        )
        processed_key.grant_decrypt(crawler_role)
        processed_bucket.grant_read(crawler_role)

        # ── Glue Crawler ─────────────────────────────────────────────────────
        glue.CfnCrawler(
            self, f"Crawler{SUFFIX}",
            name=f"pipeline-crawler-{SUFFIX}",
            role=crawler_role.role_arn,
            database_name=f"pipeline_catalog_{SUFFIX}",
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[glue.CfnCrawler.S3TargetProperty(
                    path=f"s3://{processed_bucket.bucket_name}/"
                )]
            ),
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior="UPDATE_IN_DATABASE",
                delete_behavior="LOG",
            ),
        )

        # ── Glue ETL job IAM role ────────────────────────────────────────────
        glue_job_role = iam.Role(
            self, f"GlueJobRole{SUFFIX}",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
            ],
        )
        processed_key.grant_decrypt(glue_job_role)
        final_key.grant_encrypt(glue_job_role)
        processed_bucket.grant_read(glue_job_role)
        final_bucket.grant_write(glue_job_role)

        # Glue job script bucket (reuse final key for simplicity)
        scripts_bucket = s3.Bucket(
            self, "ScriptsBucket",
            bucket_name=f"glue-scripts-{SUFFIX}",
            encryption=s3.BucketEncryption.KMS,
            encryption_key=final_key,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )
        final_key.grant_encrypt_decrypt(glue_job_role)
        scripts_bucket.grant_read(glue_job_role)

        # ── Glue ETL job ─────────────────────────────────────────────────────
        glue.CfnJob(
            self, f"GlueJob{SUFFIX}",
            name=f"pipeline-etl-job-{SUFFIX}",
            role=glue_job_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{scripts_bucket.bucket_name}/scripts/etl_job.py",
            ),
            default_arguments={
                "--SOURCE_DATABASE": f"pipeline_catalog_{SUFFIX}",
                "--DESTINATION_BUCKET": final_bucket.bucket_name,
                "--job-bookmark-option": "job-bookmark-enable",
                "--enable-metrics": "true",
            },
            glue_version="4.0",
            number_of_workers=2,
            worker_type="G.1X",
        )
