import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "SOURCE_DATABASE", "DESTINATION_BUCKET"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

dest = f"s3://{args['DESTINATION_BUCKET']}/final_output"

# ── 1. Read the table created by the processed Crawler ───────────────────────
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
    .coalesce(1)
)

# ── 3. Regional market performance: total revenue and orders per country ──────
market_performance = (
    df.groupBy("country")
    .agg(
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.count("order_id").alias("order_count"),
    )
    .orderBy(F.col("total_revenue").desc())
    .coalesce(1)
)

# ── 4. Product performance: total revenue and units sold per product ──────────
product_performance = (
    df.groupBy("product")
    .agg(
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.sum("quantity").alias("total_units_sold"),
        F.count("order_id").alias("order_count"),
    )
    .orderBy(F.col("total_revenue").desc())
    .coalesce(1)
)

# ── 5. Write outputs as Parquet partitioned by year/month/day ────────────────
def write_parquet_partitioned(dataframe, path):
    glueContext.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(dataframe, glueContext, "output"),
        connection_type="s3",
        connection_options={
            "path": path,
            "partitionKeys": ["year", "month", "day"],
        },
        format="parquet",
    )

write_parquet_partitioned(vip_customers.withColumn("year",  F.lit("all"))
                                       .withColumn("month", F.lit("all"))
                                       .withColumn("day",   F.lit("all")),
                          f"{dest}/vip_customers/")

write_parquet_partitioned(market_performance.withColumn("year",  F.lit("all"))
                                            .withColumn("month", F.lit("all"))
                                            .withColumn("day",   F.lit("all")),
                          f"{dest}/market_performance/")

write_parquet_partitioned(product_performance.withColumn("year",  F.lit("all"))
                                              .withColumn("month", F.lit("all"))
                                              .withColumn("day",   F.lit("all")),
                          f"{dest}/product_performance/")

# ── 6. Write full orders dataset partitioned by order date ───────────────────
write_parquet_partitioned(df, f"{dest}/orders/")

job.commit()
