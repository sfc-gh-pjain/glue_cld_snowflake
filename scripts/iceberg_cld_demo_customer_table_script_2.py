"""
Created for Snowflake Iceberg Lakehouse demo
Created By: Parag Jain

Description: 
Simple AWS Glue Script for Customer Data to Iceberg Table
Minimal code to load customers.json into Iceberg table
"""

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col
from awsglue.utils import getResolvedOptions
import sys

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# AWS Glue parameters - using simple camelCase names
args = getResolvedOptions(sys.argv, ["dataBucket", "glueDatabase"])
bucket_path = args["dataBucket"]
database = args["glueDatabase"]
# Paths
data_path = f"s3://{bucket_path}/raw_data/customers.json"

# catalog and table names
catalog = "glue_catalog"
table = "customers_info"

# Read JSON
df = spark.read.option("multiline", "true").json(data_path)

# Cast columns
df = df.withColumn("customer_id", col("customer_id").cast("int")) \
       .withColumn("customer_name", col("name").cast("string")) \
       .withColumn("age", col("age").cast("int"))

# Select final columns
df = df.select("customer_id", "customer_name", "age")

# Create Iceberg table (if not exists)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{database}.{table} (
  customer_id INT,
  customer_name STRING,
  age INT
)
USING ICEBERG
""")

# Write to Iceberg
spark.sql(f"TRUNCATE TABLE {catalog}.{database}.{table}")
#df.writeTo(f"{catalog}.{database}.{table}").append()
df.write.insertInto(f"{catalog}.{database}.{table}")
