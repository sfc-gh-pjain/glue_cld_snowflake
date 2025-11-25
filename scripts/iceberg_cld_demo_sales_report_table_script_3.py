"""
Created for Snowflake Iceberg Lakehouse demo
Created By: Parag Jain

Description:
PySpark Script for AWS Glue - Top 10 Highest Selling Products Report 
This script reads from the Iceberg table created from gzipped sales data 
and generates a report of the top 10 highest selling products by product ID.
"""

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import sys

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# AWS Glue parameters
args = getResolvedOptions(sys.argv, ["dataBucket", "glueDatabase"])
bucket_path = args["dataBucket"]
database = args["glueDatabase"]


# catalog and table names
catalog = "glue_catalog"
source_sales_table = "raw_sales_data"  
report_table = "top_10_products_report_spark"

# For better performance with large datasets (1M records)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")

# Create Iceberg table for the report if it doesn't exist
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{database}.{report_table} (
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
""")

print("=====Code Below is Spark dataframe, that can run as is on Sowflake using Snowpark Connect==========")

from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, max as spark_max, 
    min as spark_min, desc, asc, round as spark_round,
    to_date, when, isnan, isnull, current_date, countDistinct
)
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

job_name = "AWS Glue Top 10 Product Sales Report"

print("=== Starting Top 10 Highest Selling Products Report from Iceberg Table ===")

# Step 1: Read from existing Iceberg table (created from gzipped data)
print(f"\n1. Reading sales data from Iceberg table: {catalog}.{database}.{source_sales_table}")
try:
    # Read from the existing Iceberg table created by glue_script_gzip.py
    sales_iceberg_df = spark.table(f"{catalog}.{database}.{source_sales_table}")
    record_count = sales_iceberg_df.count()
    print(f"Sales data loaded successfully from Iceberg table. Total records: {record_count:,}")
    
    print("Iceberg table schema:")
    sales_iceberg_df.printSchema()    
    
    print("Sample data from Iceberg table:")
    sales_iceberg_df.show(3, truncate=False)
    
except Exception as e:
    print(f"Error reading from Iceberg table: {e}")
    print("Make sure the Iceberg table exists and has been populated by running glue_script_gzip.py first")
    raise

# Step 2: Extract and flatten the nested purchase data from Iceberg table
print("\n2. Extracting and flattening purchase data from Iceberg table...")
try:
    # Extract nested purchase data from the struct column
    flattened_sales = sales_iceberg_df.select(
        col("customer_id").alias("customer_id"),
        col("customer_name").alias("customer_name"),
        col("purchases.prodid").alias("product_id"),
        col("purchases.purchase_amount").alias("purchase_amount"),
        col("purchases.quantity").alias("quantity"),
        col("purchases.purchase_date").alias("purchase_date")
    )
    
    # Remove records with null or invalid product IDs
    clean_sales = flattened_sales.filter(
        col("product_id").isNotNull() & 
        (col("product_id") > 0) & 
        col("purchase_amount").isNotNull() & 
        (col("purchase_amount") > 0) &
        col("quantity").isNotNull() & 
        (col("quantity") > 0)
    )
    
    print(f"Cleaned sales data. Records after cleaning: {clean_sales.count():,}")
    print("Sample cleaned data:")
    clean_sales.show(5, truncate=False)
    
except Exception as e:
    print(f"Error flattening Iceberg table data: {e}")
    raise

# Step 3: Aggregate sales data by product ID
print("\n3. Aggregating sales data by product ID...")
try:
    product_sales_summary = clean_sales.groupBy("product_id").agg(
        spark_sum("purchase_amount").alias("total_sales_amount"),
        spark_sum("quantity").alias("total_quantity_sold"),
        count("*").alias("total_transactions"),
        avg("purchase_amount").alias("avg_transaction_amount"),
        spark_max("purchase_amount").alias("max_transaction_amount"),
        spark_min("purchase_amount").alias("min_transaction_amount"),
        avg("quantity").alias("avg_quantity_per_transaction"),
        spark_max("quantity").alias("max_quantity_per_transaction"),
        countDistinct("customer_id").alias("unique_customers"),
        spark_min("purchase_date").alias("first_sale_date"),
        spark_max("purchase_date").alias("last_sale_date")
    ).withColumn(
        "avg_price_per_unit", 
        spark_round(col("total_sales_amount") / col("total_quantity_sold"), 2)
    ).withColumn(
        "avg_transaction_amount", 
        spark_round(col("avg_transaction_amount"), 2)
    ).withColumn(
        "avg_quantity_per_transaction", 
        spark_round(col("avg_quantity_per_transaction"), 2)
    )
    
    print(f"Product aggregation completed. Unique products: {product_sales_summary.count():,}")
    print("Top 5 products by sales amount:")
    product_sales_summary.orderBy(desc("total_sales_amount")).show(5, truncate=False)
    
except Exception as e:
    print(f"Error aggregating sales data: {e}")
    raise

# Step 4: Generate Top 10 Highest Selling Products Report
print("\n4. Generating Top 10 Highest Selling Products Report...")
try:
    # Rank by total sales amount
    top_10_products = product_sales_summary.orderBy(desc("total_sales_amount")).limit(10)
    
    print("\n=== TOP 10 HIGHEST SELLING PRODUCTS BY SALES AMOUNT ===")
    top_10_products.show(10, truncate=False)
    
    # Also show top 10 by quantity for comparison
    top_10_by_quantity = product_sales_summary.orderBy(desc("total_quantity_sold")).limit(10)
    
    print("\n=== TOP 10 HIGHEST SELLING PRODUCTS BY QUANTITY SOLD ===")
    top_10_by_quantity.show(10, truncate=False)
    
    # Top 10 by customer reach
    top_10_by_reach = product_sales_summary.orderBy(desc("unique_customers")).limit(10)
    
    print("\n=== TOP 10 PRODUCTS BY CUSTOMER REACH ===")
    top_10_by_reach.show(10, truncate=False)
    
except Exception as e:
    print(f"Error generating top products report: {e}")
    raise

# Step 5: Create detailed report with rankings
print("\n5. Creating detailed report with rankings...")
try:
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, rank, dense_rank, lit
    
    # Create window specification for ranking
    sales_window = Window.partitionBy(lit(1)).orderBy(desc("total_sales_amount"))
    quantity_window = Window.partitionBy(lit(1)).orderBy(desc("total_quantity_sold"))  
    customer_window = Window.partitionBy(lit(1)).orderBy(desc("unique_customers"))
    
    detailed_report = product_sales_summary.withColumn(
        "sales_amount_rank", row_number().over(sales_window)
    ).withColumn(
        "quantity_rank", row_number().over(quantity_window)
    ).withColumn(
        "customer_reach_rank", row_number().over(customer_window)
    ).filter(col("sales_amount_rank") <= 10).orderBy("sales_amount_rank")
    
    print("\n=== DETAILED TOP 10 PRODUCTS REPORT (1M Records Analysis) ===")
    detailed_report.select(
        "sales_amount_rank",
        "product_id",
        "total_sales_amount",
        "total_quantity_sold",
        "unique_customers",
        "total_transactions",
        "avg_price_per_unit",
        "avg_transaction_amount",
        "first_sale_date",
        "last_sale_date"
    ).show(10, truncate=False)
    
except Exception as e:
    print(f"Error creating detailed report: {e}")
    raise

# Step 6: Save report to Iceberg table
print("\n6. Saving report to Iceberg table...")
try:
  
    # Add report metadata and save
    from pyspark.sql.functions import current_date, lit
    
    final_report = detailed_report.withColumn("report_date", current_date()) \
                                 .withColumn("source_table", lit(source_sales_table)) \
                                 .withColumn("job_name", lit(job_name))
    
    # Write to Iceberg table (overwrite mode for latest report)
    # final_report.writeTo(f"{catalog}.{database}.{report_table}").overwritePartitions()
    
    # Get the target table's column order
    target_df = spark.read.table(f"{catalog}.{database}.{report_table}")

    spark.sql(f"TRUNCATE TABLE {catalog}.{database}.{report_table}")
    final_report.select(*target_df.columns).write.insertInto(f"{catalog}.{database}.{report_table}")    
    
    print(f"Report saved successfully to {catalog}.{database}.{report_table}")
    target_df.show(10, truncate=False)
except Exception as e:
    print(f"Error saving report to Iceberg: {e}")


print(f" Source: Iceberg table {catalog}.{database}.{source_sales_table}")
print(f" Report saved to: {catalog}.{database}.{report_table}")
print(f" Top 10 products analysis completed")

print(f"\n Sales report generation completed successfully!")
