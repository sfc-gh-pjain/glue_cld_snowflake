"""
Created for Snowflake Iceberg Lakehouse demo
Created By: Parag Jain

Description:
PySpark Script for AWS Glue to load sales data into iceberg table

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
# Paths
data_path = f"s3://{bucket_path}/raw_data/sales_data_1m.json.gz"

# catalog and table names
catalog = "glue_catalog"
table = "raw_sales_data"


from pyspark.sql.functions import col, struct, to_date

# Read JSON with nested structure
df = spark.read.option("multiline", "true").json(data_path)

# Individually cast subfields
df = df.withColumn("prodid", col("purchases.prodid").cast("int")) \
       .withColumn("purchase_amount", col("purchases.purchase_amount").cast("double")) \
       .withColumn("quantity", col("purchases.quantity").cast("int")) \
       .withColumn("purchase_date", to_date(col("purchases.purchase_date"), "yyyy-MM-dd"))

# Rebuild 'purchases' as a struct
df = df.withColumn("purchases", struct(
    col("prodid"),
    col("purchase_amount"),
    col("quantity"),
    col("purchase_date")
))

# Drop temp columns (optional, for clarity)
df = df.select("customer_id", "customer_name", "purchases")

# Optional sanity check
df.printSchema()
df.show(truncate=False)

# Create Iceberg table (if not exists)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{database}.{table} (
  customer_id INT,
  customer_name STRING,
  purchases STRUCT<
    prodid: INT,
    purchase_amount: DOUBLE,
    quantity: INT,
    purchase_date: DATE
  >
)
USING ICEBERG
""")

# Write to Iceberg
df.writeTo(f"{catalog}.{database}.{table}").append()
