"""
Created for Snowflake Iceberg Lakehouse demo
Created By: Parag Jain

Description:
PySpark Script for AWS Glue - Top 10 Highest Selling Products Report 
This script reads from the Iceberg table created from gzipped sales data 
and generates a report of the top 10 highest selling products by product ID. using Snowflake notebook.
"""

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col
from awsglue.utils import getResolvedOptions
import sys

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# AWS Glue parameters
args = getResolvedOptions(sys.argv, ["dataBucket", "glueDatabase"])
bucket_path = args["dataBucket"]
database = args["glueDatabase"]

# catalog and table names
catalog = "glue_catalog"
table = "top_10_products_report_snow"

# Create Iceberg table (if not exists)
spark.sql(f"""
    CREATE OR REPLACE TABLE  {catalog}.{database}.{table} (
        sales_amount_rank INT,
        product_id INT,
        total_sales_amount DOUBLE,
        total_quantity_sold BIGINT,
        unique_customers BIGINT,
        total_transactions BIGINT,
        avg_transaction_amount DOUBLE,
        avg_quantity_per_transaction DOUBLE,
        avg_price_per_unit DOUBLE,
        max_transaction_amount DOUBLE,
        min_transaction_amount DOUBLE,
        max_quantity_per_transaction BIGINT,
        first_sale_date DATE,
        last_sale_date DATE,
        report_date DATE,
        source_table STRING,
        job_name STRING
    )
    USING ICEBERG
    PARTITIONED BY (report_date)
    TBLPROPERTIES ('format-version' = '2')
""")

# Write to Iceberg
#df.writeTo(f"{catalog}.{database}.{table}").append()
