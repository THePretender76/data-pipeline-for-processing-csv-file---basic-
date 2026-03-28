import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "SOURCE_DATABASE", "DESTINATION_BUCKET"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

dest = f"s3://{args['DESTINATION_BUCKET']}/final_output"

# ── 1. Read the table created by the Crawler in the Data Catalog ─────────────
datasource = glueContext.create_dynamic_frame.from_catalog(
    database=args["SOURCE_DATABASE"],
    table_name="orders",
)
df = datasource.toDF()

# ── 2. VIP customers: total spend per customer, ranked highest first ─────────
vip_customers = (
    df.groupBy("customer_email")
    .agg(
        F.round(F.sum("total_amount"), 2).alias("total_spent"),
        F.count("order_id").alias("order_count"),
    )
    .orderBy(F.col("total_spent").desc())
)

# ── 3. Regional market performance: total revenue and orders per country ──────
market_performance = (
    df.groupBy("country")
    .agg(
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.count("order_id").alias("order_count"),
    )
    .orderBy(F.col("total_revenue").desc())
)

# ── 4. Write both outputs as CSV ─────────────────────────────────────────────
def write_csv(df, path):
    glueContext.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(df, glueContext, "output"),
        connection_type="s3",
        connection_options={"path": path},
        format="csv",
        format_options={"writeHeader": True, "separator": ","},
    )

write_csv(vip_customers, f"{dest}/vip_customers/")
write_csv(market_performance, f"{dest}/market_performance/")

job.commit()
