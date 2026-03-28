from aws_cdk import (
    Stack, RemovalPolicy, Duration,
    aws_s3 as s3,
    aws_kms as kms,
    aws_lambda as _lambda,
    aws_sqs as sqs,
    aws_glue as glue,
    aws_iam as iam,
    aws_sns as sns,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cw_actions,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_events as events,
    aws_events_targets as targets,
    aws_athena as athena,
    custom_resources as cr,
)
from constructs import Construct

SUFFIX = "nak76700"
ALERT_EMAIL = "thenewpretender@gmail.com"
DATABASE_NAME = f"pipeline_catalog_{SUFFIX}"


class DataPipelineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ── KMS keys ─────────────────────────────────────────────────────────
        raw_key       = kms.Key(self, "RawKey",       enable_key_rotation=True, removal_policy=RemovalPolicy.DESTROY)
        processed_key = kms.Key(self, "ProcessedKey", enable_key_rotation=True, removal_policy=RemovalPolicy.DESTROY)
        final_key     = kms.Key(self, "FinalKey",     enable_key_rotation=True, removal_policy=RemovalPolicy.DESTROY)
        shared_key    = kms.Key(self, "SharedKey",    enable_key_rotation=True, removal_policy=RemovalPolicy.DESTROY)

        # ── S3 buckets ───────────────────────────────────────────────────────
        raw_bucket = s3.Bucket(
            self, "RawBucket",
            bucket_name=f"raw-file-pipeline-{SUFFIX}",
            encryption=s3.BucketEncryption.KMS,
            encryption_key=raw_key,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            event_bridge_enabled=True,  # required for EventBridge S3 rules
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

        athena_results_bucket = s3.Bucket(
            self, "AthenaResultsBucket",
            bucket_name=f"athena-results-{SUFFIX}",
            encryption=s3.BucketEncryption.KMS,
            encryption_key=shared_key,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # ── SQS Dead-Letter Queue ────────────────────────────────────────────
        dlq = sqs.Queue(
            self, f"DLQ{SUFFIX}",
            queue_name=f"pipeline-dlq-{SUFFIX}",
            encryption=sqs.QueueEncryption.KMS,
            encryption_master_key=shared_key,
            retention_period=Duration.days(14),
        )

        # ── SNS alert topic ──────────────────────────────────────────────────
        alert_topic = sns.Topic(
            self, f"AlertTopic{SUFFIX}",
            topic_name=f"pipeline-alerts-{SUFFIX}",
            master_key=shared_key,
        )
        # Subscription created once via custom resource — never recreated on redeploy
        cr.AwsCustomResource(
            self, f"SNSSubscription{SUFFIX}",
            on_create=cr.AwsSdkCall(
                service="SNS",
                action="subscribe",
                parameters={
                    "TopicArn": alert_topic.topic_arn,
                    "Protocol": "email",
                    "Endpoint": ALERT_EMAIL,
                },
                physical_resource_id=cr.PhysicalResourceId.of(f"sns-sub-{SUFFIX}"),
                output_paths=["SubscriptionArn"],
            ),
            on_delete=cr.AwsSdkCall(
                service="SNS",
                action="unsubscribe",
                parameters={"SubscriptionArn": cr.PhysicalResourceIdReference()},
            ),
            policy=cr.AwsCustomResourcePolicy.from_statements([
                iam.PolicyStatement(
                    actions=["sns:Subscribe", "sns:Unsubscribe"],
                    resources=[alert_topic.topic_arn],
                )
            ]),
        )

        # ── CloudWatch alarm: DLQ not empty → SNS ───────────────────────────
        dlq_alarm = cloudwatch.Alarm(
            self, f"DLQAlarm{SUFFIX}",
            alarm_name=f"pipeline-dlq-not-empty-{SUFFIX}",
            metric=dlq.metric_approximate_number_of_messages_visible(period=Duration.minutes(5)),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )
        dlq_alarm.add_alarm_action(cw_actions.SnsAction(alert_topic))

        # ── Glue database ────────────────────────────────────────────────────
        glue.CfnDatabase(
            self, f"GlueDB{SUFFIX}",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=DATABASE_NAME,
                description="Data Catalog for the CSV pipeline v3",
            ),
        )

        # ── Shared Glue crawler IAM role ─────────────────────────────────────
        crawler_role = iam.Role(
            self, f"CrawlerRole{SUFFIX}",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
            ],
        )
        processed_key.grant_decrypt(crawler_role)
        final_key.grant_decrypt(crawler_role)
        processed_bucket.grant_read(crawler_role)
        final_bucket.grant_read(crawler_role)

        # ── Glue Crawler 1: scans processed bucket after Lambda ──────────────
        processed_crawler = glue.CfnCrawler(
            self, f"ProcessedCrawler{SUFFIX}",
            name=f"pipeline-processed-crawler-{SUFFIX}",
            role=crawler_role.role_arn,
            database_name=DATABASE_NAME,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[glue.CfnCrawler.S3TargetProperty(
                    path=f"s3://{processed_bucket.bucket_name}/orders/"
                )]
            ),
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior="UPDATE_IN_DATABASE",
                delete_behavior="LOG",
            ),
        )

        # ── Glue Crawler 2: scans final bucket after ETL job ─────────────────
        final_crawler = glue.CfnCrawler(
            self, f"FinalCrawler{SUFFIX}",
            name=f"pipeline-final-crawler-{SUFFIX}",
            role=crawler_role.role_arn,
            database_name=DATABASE_NAME,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[glue.CfnCrawler.S3TargetProperty(
                    path=f"s3://{final_bucket.bucket_name}/final_output/"
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
        final_key.grant_encrypt_decrypt(glue_job_role)
        processed_bucket.grant_read(glue_job_role)
        final_bucket.grant_write(glue_job_role)
        scripts_bucket.grant_read(glue_job_role)

        # ── Glue ETL job ─────────────────────────────────────────────────────
        glue_job = glue.CfnJob(
            self, f"GlueJob{SUFFIX}",
            name=f"pipeline-etl-job-{SUFFIX}",
            role=glue_job_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{scripts_bucket.bucket_name}/scripts/etl_job.py",
            ),
            default_arguments={
                "--SOURCE_DATABASE": DATABASE_NAME,
                "--DESTINATION_BUCKET": final_bucket.bucket_name,
                "--job-bookmark-option": "job-bookmark-enable",
                "--enable-metrics": "true",
            },
            glue_version="4.0",
            number_of_workers=2,
            worker_type="G.1X",
        )

        # ── Lambda IAM role ──────────────────────────────────────────────────
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
        lambda_role.add_to_policy(iam.PolicyStatement(
            actions=["xray:PutTraceSegments", "xray:PutTelemetryRecords"],
            resources=["*"],
        ))

        # ── Lambda X-Ray layer ───────────────────────────────────────────────
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

        # ── Step Functions IAM role ──────────────────────────────────────────
        sfn_role = iam.Role(
            self, f"SFnRole{SUFFIX}",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
        )
        processor_fn.grant_invoke(sfn_role)
        alert_topic.grant_publish(sfn_role)
        shared_key.grant_encrypt_decrypt(sfn_role)
        sfn_role.add_to_policy(iam.PolicyStatement(
            actions=["glue:StartCrawler", "glue:GetCrawler", "glue:StartJobRun", "glue:GetJobRun"],
            resources=["*"],
        ))

        # ── Step Functions tasks ─────────────────────────────────────────────

        # Task 1: Lambda validation + cleaning
        validate_task = tasks.LambdaInvoke(
            self, "ValidateAndClean",
            lambda_function=processor_fn,
            payload=sfn.TaskInput.from_object({
                "Records": [{
                    "s3": {
                        "bucket": {"name": sfn.JsonPath.string_at("$.detail.bucket.name")},
                        "object": {"key":  sfn.JsonPath.string_at("$.detail.object.key")},
                    }
                }]
            }),
            result_path="$.lambdaResult",
        )

        # Task 2: Start processed crawler
        start_processed_crawler = tasks.CallAwsService(
            self, "StartProcessedCrawler",
            service="glue",
            action="startCrawler",
            parameters={"Name": processed_crawler.name},
            iam_resources=["*"],
            result_path=sfn.JsonPath.DISCARD,
        )

        # Task 3: Wait for processed crawler
        wait_processed_crawler = tasks.CallAwsService(
            self, "WaitProcessedCrawler",
            service="glue",
            action="getCrawler",
            parameters={"Name": processed_crawler.name},
            iam_resources=["*"],
            result_path="$.crawlerState",
        )
        crawler_running = sfn.Choice(self, "ProcessedCrawlerDone?")
        crawler_wait = sfn.Wait(self, "WaitProcessedCrawlerRetry",
            time=sfn.WaitTime.duration(Duration.seconds(15))
        )

        # Task 4: Start Glue ETL job
        start_etl_job = tasks.CallAwsService(
            self, "StartETLJob",
            service="glue",
            action="startJobRun",
            parameters={"JobName": glue_job.name},
            iam_resources=["*"],
            result_path="$.jobRun",
        )

        # Task 5: Wait for ETL job
        wait_etl_job = tasks.CallAwsService(
            self, "WaitETLJob",
            service="glue",
            action="getJobRun",
            parameters={
                "JobName": glue_job.name,
                "RunId": sfn.JsonPath.string_at("$.jobRun.JobRunId"),
            },
            iam_resources=["*"],
            result_path="$.jobRunState",
        )
        etl_done = sfn.Choice(self, "ETLJobDone?")
        etl_wait = sfn.Wait(self, "WaitETLJobRetry",
            time=sfn.WaitTime.duration(Duration.seconds(30))
        )

        # Task 6: Start final crawler
        start_final_crawler = tasks.CallAwsService(
            self, "StartFinalCrawler",
            service="glue",
            action="startCrawler",
            parameters={"Name": final_crawler.name},
            iam_resources=["*"],
            result_path=sfn.JsonPath.DISCARD,
        )

        # Task 7: Wait for final crawler
        wait_final_crawler = tasks.CallAwsService(
            self, "WaitFinalCrawler",
            service="glue",
            action="getCrawler",
            parameters={"Name": final_crawler.name},
            iam_resources=["*"],
            result_path="$.finalCrawlerState",
        )
        final_crawler_done = sfn.Choice(self, "FinalCrawlerDone?")
        final_crawler_wait = sfn.Wait(self, "WaitFinalCrawlerRetry",
            time=sfn.WaitTime.duration(Duration.seconds(15))
        )

        # Success + failure terminal states
        pipeline_success = sfn.Succeed(self, "PipelineSucceeded")
        pipeline_failure = sfn.Fail(self, "PipelineFailed",
            error="PipelineError",
            cause="See execution details for error",
        )

        # SNS alert on failure
        notify_failure = tasks.SnsPublish(
            self, "NotifyFailure",
            topic=alert_topic,
            message=sfn.TaskInput.from_object({
                "error": sfn.JsonPath.string_at("$.Error"),
                "cause": sfn.JsonPath.string_at("$.Cause"),
                "bucket": sfn.JsonPath.string_at("$.detail.bucket.name"),
                "key": sfn.JsonPath.string_at("$.detail.object.key"),
            }),
            result_path=sfn.JsonPath.DISCARD,
        ).next(pipeline_failure)

        # ── Step Functions workflow definition ───────────────────────────────
        definition = (
            validate_task
            .add_catch(notify_failure, errors=["States.ALL"], result_path="$")
            .next(start_processed_crawler)
            .next(crawler_wait.next(wait_processed_crawler))
        )

        crawler_running.when(
            sfn.Condition.string_equals("$.crawlerState.Crawler.State", "RUNNING"),
            crawler_wait,
        ).when(
            sfn.Condition.string_equals("$.crawlerState.Crawler.State", "STOPPING"),
            crawler_wait,
        ).otherwise(
            start_etl_job
            .add_catch(notify_failure, errors=["States.ALL"], result_path="$")
            .next(etl_wait.next(wait_etl_job))
        )

        etl_done.when(
            sfn.Condition.string_equals("$.jobRunState.JobRun.JobRunState", "RUNNING"),
            etl_wait,
        ).when(
            sfn.Condition.string_equals("$.jobRunState.JobRun.JobRunState", "STARTING"),
            etl_wait,
        ).when(
            sfn.Condition.string_equals("$.jobRunState.JobRun.JobRunState", "FAILED"),
            notify_failure,
        ).otherwise(
            start_final_crawler
            .next(final_crawler_wait.next(wait_final_crawler))
        )

        final_crawler_done.when(
            sfn.Condition.string_equals("$.finalCrawlerState.Crawler.State", "RUNNING"),
            final_crawler_wait,
        ).when(
            sfn.Condition.string_equals("$.finalCrawlerState.Crawler.State", "STOPPING"),
            final_crawler_wait,
        ).otherwise(pipeline_success)

        wait_processed_crawler.next(crawler_running)
        wait_etl_job.next(etl_done)
        wait_final_crawler.next(final_crawler_done)

        # ── State machine ────────────────────────────────────────────────────
        state_machine = sfn.StateMachine(
            self, f"PipelineSFn{SUFFIX}",
            state_machine_name=f"pipeline-orchestrator-{SUFFIX}",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            role=sfn_role,
            tracing_enabled=True,
        )

        # ── EventBridge rule: S3 upload → Step Functions ─────────────────────
        eb_role = iam.Role(
            self, f"EventBridgeRole{SUFFIX}",
            assumed_by=iam.ServicePrincipal("events.amazonaws.com"),
        )
        state_machine.grant_start_execution(eb_role)

        events.Rule(
            self, f"S3UploadRule{SUFFIX}",
            rule_name=f"pipeline-s3-upload-{SUFFIX}",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                    "bucket": {"name": [raw_bucket.bucket_name]},
                    "object": {"key": [{"suffix": ".csv"}]},
                },
            ),
            targets=[targets.SfnStateMachine(
                state_machine,
                role=eb_role,
            )],
        )

        # ── Athena workgroup ─────────────────────────────────────────────────
        athena.CfnWorkGroup(
            self, f"AthenaWorkgroup{SUFFIX}",
            name=f"pipeline-workgroup-{SUFFIX}",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{athena_results_bucket.bucket_name}/results/",
                    encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(
                        encryption_option="SSE_KMS",
                        kms_key=shared_key.key_arn,
                    ),
                ),
                enforce_work_group_configuration=True,
            ),
        )
