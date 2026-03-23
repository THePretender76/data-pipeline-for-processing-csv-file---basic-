import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME", "SOURCE_DATABASE", "DESTINATION_BUCKET"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read all tables from the catalog database
dyf = glueContext.create_dynamic_frame.from_catalog(
    database=args["SOURCE_DATABASE"],
    table_name="processed",  # table name created by the crawler
)

# Write to the final S3 bucket in Parquet format
glueContext.write_dynamic_frame.from_options(
    frame=dyf,
    connection_type="s3",
    connection_options={"path": f"s3://{args['DESTINATION_BUCKET']}/final/"},
    format="parquet",
)

job.commit()
