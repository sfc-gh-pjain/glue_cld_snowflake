# Implementation Guide: AWS Glue + Iceberg + Snowflake Interoperable Lakehouse

This guide provides step-by-step instructions to deploy and run the interoperable lakehouse architecture demonstrating code portability between AWS Glue and Snowflake using Apache Iceberg tables.

## Table of Contents
1. [Prerequisites](#prerequisites)
2. [Architecture Overview](#architecture-overview)
3. [Step-by-Step Implementation](#step-by-step-implementation)
4. [Validation and Testing](#validation-and-testing)
5. [Cost Considerations](#cost-considerations)
6. [Cleanup](#cleanup)

---

## Prerequisites

### AWS Requirements
- **AWS Account** with administrative access
- **AWS CLI** installed and configured
- **Permissions** to create:
  - S3 buckets
  - IAM roles and policies
  - AWS Glue databases and jobs
  - CloudFormation stacks

### Snowflake Requirements
- **Snowflake Account** (Enterprise Edition or higher recommended)
- **Account Admin** or equivalent privileges
- **Snowflake Notebook** feature enabled (requires Snowflake Runtime 2.0+)
- **Snowpark Connect** package support

### Tools & Software
- Web browser for AWS Console and Snowflake UI
- Text editor for updating configuration parameters
- Basic knowledge of:
  - AWS services (S3, Glue, IAM, CloudFormation)
  - Snowflake (databases, warehouses, notebooks)
  - SQL and Python/PySpark

### Files from This Repository
```
glue_cld_snowflake/
├── cloudformation/
│   └── iceberg_glue_stack_cloudformation_template.yaml
├── data/
│   ├── customers.json
│   └── sales_data_1m.json.gz
├── scripts/
│   ├── iceberg_cld_demo_customer_table_script_2.py
│   ├── iceberg_cld_demo_sales_table_script_1.py
│   ├── iceberg_cld_demo_sales_report_table_script_3.py
│   └── iceberg_cld_demo_snow_report_table_script_4.py
└── snowflake_notebook/
    └── 01_GLUE_LH_SNOWPARK_CONNECT_DEMO.ipynb
```

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                    Implementation Flow                   │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  1. Setup S3 Bucket + Upload Data & Scripts             │
│  2. Configure Snowflake Notebook                        │
│  3. Create Snowflake External Volume                    │
│  4. Get Snowflake IAM User ARN & External ID            │
│  5. Deploy CloudFormation Stack                         │
│  6. Run Glue ETL Jobs (Load Data)                       │
│  7. Verify Iceberg Tables in Glue                       │
│  8. Generate Report in Glue                             │
│  9. Create Snowflake Catalog Integration                │
│ 10. Run Same Analytics Code in Snowflake               │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

---

## Step-by-Step Implementation

### Step 1: Create S3 Bucket and Upload Data

**1.1 Create S3 Bucket**

Navigate to AWS S3 Console and create a new bucket:

```bash
# Option 1: Using AWS Console
# - Go to S3 Console
# - Click "Create bucket"
# - Choose a unique name (e.g., my-iceberg-lakehouse-demo-<your-account-id>)
# - Select the same region as your Snowflake account (to avoid egress charges)
# - Keep default settings for encryption and versioning
# - create two folders in this bucket namely raw_data and scripts
# - upload all the scripts to the scripts folder and data to the raw_data folder from your local machine
```
![content in scripts folder](diagrams/scripts.jpeg)

![raw_data folder](diagrams/raw.jpeg)

```bash
# Option 2: Using AWS CLI
export DATA_BUCKET=my-iceberg-lakehouse-demo-<your-account-id>
export AWS_REGION=us-west-2  # Match with your Snowflake region

aws s3 mb s3://$DATA_BUCKET --region $AWS_REGION
```

**1.2 Create Folder Structure**

```bash
# Create the required folder structure
aws s3api put-object --bucket $DATA_BUCKET --key raw_data/
aws s3api put-object --bucket $DATA_BUCKET --key scripts/
aws s3api put-object --bucket $DATA_BUCKET --key iceberg-warehouse/
```

**1.3 Upload Data Files**

```bash
# Navigate to your project directory
cd glue_cld_snowflake

# Upload data files to raw_data folder
aws s3 cp data/customers.json s3://$DATA_BUCKET/raw_data/customers.json
aws s3 cp data/sales_data_1m.json.gz s3://$DATA_BUCKET/raw_data/sales_data_1m.json.gz

# Verify upload
aws s3 ls s3://$DATA_BUCKET/raw_data/
```

**1.4 Upload Glue Scripts**

```bash
# Upload all Glue ETL scripts
aws s3 sync scripts/ s3://$DATA_BUCKET/scripts/ --exclude "*" --include "*.py"

# Verify scripts upload
aws s3 ls s3://$DATA_BUCKET/scripts/
```

**Expected Output:**
```
iceberg_cld_demo_customer_table_script_2.py
iceberg_cld_demo_sales_table_script_1.py
iceberg_cld_demo_sales_report_table_script_3.py
iceberg_cld_demo_snow_report_table_script_4.py
```

---

### Step 2: Open Snowflake Notebook UI and Create New Notebook

**2.1 Access Snowflake Notebooks**

1. Log into your Snowflake account
2. Navigate to **Projects** → **Notebooks** in the left sidebar
3. Click **+ Notebook** button to create a new notebook

**2.2 Import the Notebook File**

1. Select **Import from file** option
2. Upload the file: `snowflake_notebook/01_GLUE_LH_SNOWPARK_CONNECT_DEMO.ipynb`
3. Give your notebook a meaningful name (e.g., "AWS Glue Iceberg Integration Demo")

---

### Step 3: Configure Notebook Runtime and Packages

**3.1 Set Notebook Runtime**

1. In the notebook, click on **Notebook settings** (gear icon)
2. Select **Snowflake Runtime**: Choose **2.0** or later
3. Select **Warehouse**: Choose any warehouse for compute (e.g., `COMPUTE_WH`)
   - Recommend: Small or Medium warehouse for this demo
4. Click **Apply**

**3.2 Add Snowpark Connect Package**

1. Click on the **Packages** tab in the notebook
2. Search for `snowpark-connect`
3. Select the **latest version** of Snowpark Connect
4. Click **Add** to install the package
![Snwoflake notebook packages](diagrams/packages.jpeg)
**Note:** The notebook will initialize with Snowpark Connect capabilities after adding this package.

---

### Step 4: Update Snowflake Notebook Parameters (Initial Setup)

Before running any cells, update the parameters in **Cell 2** to match your AWS environment.

**4.1 Locate Cell 2 in the Notebook**

Look for the cell that creates the External Volume:

```sql
CREATE OR REPLACE EXTERNAL VOLUME extvol_iceberg_demo
STORAGE_LOCATIONS =
      (
         (
            NAME = 'Iceberg-Table-Demo'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://<Your dataBucket name>/'
            STORAGE_AWS_ROLE_ARN = '<AWS ROLE ARN>'
            STORAGE_AWS_EXTERNAL_ID = '<Any secret word you want>'
         )
      );
```

**4.2 Update These Parameters**

Replace the following placeholders:

| Placeholder | Replace With | Example |
|-------------|--------------|---------|
| `<Your dataBucket name>` | Your S3 bucket name from Step 1 | `my-iceberg-lakehouse-demo-123456789012` |
| `<AWS ROLE ARN>` | Leave as placeholder for now | Will be updated after CloudFormation |
| `<Any secret word you want>` | Choose a secret external ID | `my_secret_iceberg_ext_id_2024` |

**Important:** 
- **Save the External ID** you choose - you'll need it for CloudFormation deployment (Step 7)
- The AWS Role ARN will be created by CloudFormation, so leave it as a placeholder for now
- After CloudFormation completes, you'll come back and update the Role ARN

---

### Step 5: Create Snowflake Database and External Volume

**5.1 Run Initial Notebook Cells**

Run the first **3 cells** of the Snowflake notebook:

**Cell 0:** Create Database and Schema
```sql
CREATE OR REPLACE DATABASE ICEBERG_LAKE;
CREATE OR REPLACE SCHEMA ICEBERG_LAKE.DEMO;
USE SCHEMA ICEBERG_LAKE.DEMO;
```

**Cell 1:** Informational text about catalog integration

**Cell 2:** Create External Volume (with your updated parameters)
```sql
CREATE OR REPLACE EXTERNAL VOLUME extvol_iceberg_demo
STORAGE_LOCATIONS = ( ... );
```

**5.2 Run Cell 3 to Get Snowflake Credentials**

Cell 3 retrieves the Snowflake-generated IAM User ARN and External ID:

```sql
DESC EXTERNAL VOLUME extvol_iceberg_demo;

SELECT b.KEY, b.VALUE 
FROM 
    TABLE(RESULT_SCAN(LAST_QUERY_ID())) a, 
    TABLE(FLATTEN(INPUT => PARSE_JSON(a."property_value"))) b 
WHERE 
    a."parent_property" = 'STORAGE_LOCATIONS'
    AND a."property" = 'STORAGE_LOCATION_1'
    AND (b.KEY='STORAGE_AWS_EXTERNAL_ID' OR b.KEY ='STORAGE_AWS_IAM_USER_ARN');
```

**5.3 Save These Values**

The output will show two critical values:

```
KEY                          | VALUE
-----------------------------+----------------------------------------
STORAGE_AWS_IAM_USER_ARN     | arn:aws:iam::123456789012:user/abc...
STORAGE_AWS_EXTERNAL_ID      | XYZ12345_SFCRole=1_AbCdEfGh...
```

**📝 IMPORTANT: Copy and save these values!**
- `STORAGE_AWS_IAM_USER_ARN` → Use as **SnowflakeUserArn** in CloudFormation
- `STORAGE_AWS_EXTERNAL_ID` → Use as **SnowflakeExternalId** in CloudFormation

---

### Step 6: Deploy AWS CloudFormation Stack

**6.1 Prepare CloudFormation Parameters**

Before deploying, gather these parameters:

| Parameter Name | Description | Example Value |
|----------------|-------------|---------------|
| **GlueDatabaseName** | Name for Glue database | `snowcldtest` |
| **GlueRoleName** | Name for IAM role | `snowcldtest` |
| **DataBucketName** | S3 bucket from Step 1 | `snowcldtestbucket` |
| **S3ScriptLocation** | Path to any script (for generic job) | `s3://snowcldtestbucket/scripts/iceberg_cld_demo_sales_table_script_1.py` |
| **SnowflakeUserArn** | From Step 5.3 | `arn:aws:iam::123456789012:user/abc...` |
| **SnowflakeExternalId** | From Step 5.3 | `XYZ12345_SFCRole=1_...` |

![cloud formation template sample run](diagrams/cloudform.jpeg)

**6.2 Deploy via AWS Console**

1. Navigate to **AWS CloudFormation Console**
2. Click **Create stack** → **With new resources**
3. Choose **Upload a template file**
4. Upload: `cloudformation/iceberg_glue_stack_cloudformation_template.yaml`
5. Click **Next**

**6.3 Enter Stack Parameters**

- **Stack name**: `iceberg-glue-snowflake-demo`
- Fill in all parameters from the table above
- Click **Next**

**6.4 Configure Stack Options**

- **Tags** (optional): Add tags for cost tracking
- **Permissions**: Leave as default (uses your current IAM role)
- **Stack failure options**: Select **Delete newly created resources during rollback**
- Click **Next**

**6.5 Review and Deploy**

1. Review all parameters
2. Check the box: **"I acknowledge that AWS CloudFormation might create IAM resources with custom names"**
3. Click **Submit**

**6.6 Monitor Stack Creation**

- Wait for stack status to change to **CREATE_COMPLETE** (typically 2-5 minutes)
- Monitor the **Events** tab for progress
- If any errors occur, check the **Events** tab for details

**6.7 Option: Deploy via AWS CLI**

```bash
# Set variables
export GLUE_DB_NAME=iceberg_demo_db
export GLUE_ROLE_NAME=GlueIcebergRole
export DATA_BUCKET=my-iceberg-lakehouse-demo-123456789012
export SNOWFLAKE_USER_ARN="arn:aws:iam::123456789012:user/abc..."
export SNOWFLAKE_EXT_ID="XYZ12345_SFCRole=1_..."

# Deploy stack
aws cloudformation create-stack \
  --stack-name iceberg-glue-snowflake-demo \
  --template-body file://cloudformation/iceberg_glue_stack_cloudformation_template.yaml \
  --capabilities CAPABILITY_NAMED_IAM \
  --on-failure DELETE \
  --parameters \
      ParameterKey=GlueDatabaseName,ParameterValue=$GLUE_DB_NAME \
      ParameterKey=GlueRoleName,ParameterValue=$GLUE_ROLE_NAME \
      ParameterKey=DataBucketName,ParameterValue=$DATA_BUCKET \
      ParameterKey=S3ScriptLocation,ParameterValue=s3://$DATA_BUCKET/scripts/iceberg_cld_demo_sales_table_script_1.py \
      ParameterKey=SnowflakeUserArn,ParameterValue="$SNOWFLAKE_USER_ARN" \
      ParameterKey=SnowflakeExternalId,ParameterValue="$SNOWFLAKE_EXT_ID"

# Check stack status
aws cloudformation describe-stacks \
  --stack-name iceberg-glue-snowflake-demo \
  --query 'Stacks[0].StackStatus'
```

---

### Step 7: Get CloudFormation Outputs (IAM Role ARN)

**7.1 Get the Glue IAM Role ARN**

After CloudFormation completes, retrieve the IAM Role ARN:

```bash
# Using AWS CLI
aws cloudformation describe-stacks \
  --stack-name iceberg-glue-snowflake-demo \
  --query 'Stacks[0].Outputs[?OutputKey==`GlueRole`].OutputValue' \
  --output text

# Using AWS Console
# Go to CloudFormation → Stacks → iceberg-glue-snowflake-demo → Outputs tab
# Look for "GlueRole" key → Copy the ARN value
```

**7.2 Construct Full Role ARN**

The output shows the role name. Construct the full ARN:

```
arn:aws:iam::<YOUR-AWS-ACCOUNT-ID>:role/<GlueRoleName>
```

Example:
```
arn:aws:iam::123456789012:role/GlueIcebergRole
```

**📝 Save this ARN - you'll need it to update the Snowflake notebook!**

---

### Step 8: Verify AWS Glue Resources

**8.1 Check Glue Database**

```bash
# Using AWS CLI
aws glue get-database --name $GLUE_DB_NAME

# Using AWS Console
# Navigate to: AWS Glue → Data Catalog → Databases
# Verify your database (e.g., iceberg_demo_db) exists
```

**8.2 Check Glue ETL Jobs**

Navigate to **AWS Glue Console** → **ETL Jobs**

You should see **5 Glue jobs** created:

| Job Name | Purpose |
|----------|---------|
| `iceberg-create-<db-name>` | Generic job for running any script |
| `iceberg-sales-table-<db-name>` | Load 1M sales records to Iceberg |
| `iceberg-customer-table-<db-name>` | Load customer data to Iceberg |
| `iceberg-sales-report-<db-name>` | Generate Top 10 products report (Spark) |
| `iceberg-snow-report-<db-name>` | Create placeholder table for Snowflake |

![Glue ETL jobs sample](diagrams/glue.jpeg)

**8.3 Verify Scripts Location**

Each job should have:
- **Script location**: `s3://<your-bucket>/scripts/iceberg_cld_demo_*.py`
- **IAM role**: The role created by CloudFormation
- **Glue version**: 4.0
- **Worker type**: G.1X
- **Number of workers**: 2

---

### Step 9: Run Glue ETL Jobs - Load Data Tables

**9.1 Run Customer Table Load Job**

```bash
# Using AWS CLI
aws glue start-job-run --job-name iceberg-customer-table-$GLUE_DB_NAME

# Save the Job Run ID
export CUSTOMER_JOB_RUN_ID=<output-from-above>

# Check job status
aws glue get-job-run \
  --job-name iceberg-customer-table-$GLUE_DB_NAME \
  --run-id $CUSTOMER_JOB_RUN_ID \
  --query 'JobRun.JobRunState'
```

**Using AWS Console:**
1. Go to **AWS Glue** → **ETL Jobs**
2. Select `iceberg-customer-table-<db-name>`
3. Click **Run** button
4. Monitor the **Runs** tab for status

**Expected Duration:** 2-3 minutes

**9.2 Run Sales Table Load Job**

```bash
# Using AWS CLI
aws glue start-job-run --job-name iceberg-sales-table-$GLUE_DB_NAME

# Check job status (wait for SUCCEEDED)
aws glue get-job-run \
  --job-name iceberg-sales-table-$GLUE_DB_NAME \
  --run-id <job-run-id> \
  --query 'JobRun.JobRunState'
```

**Using AWS Console:**
1. Select `iceberg-sales-table-<db-name>`
2. Click **Run**
3. Monitor progress (will take longer due to 1M records)

**Expected Duration:** 3-5 minutes (processing 1M gzipped records)

**9.3 Wait for Both Jobs to Complete**

Both jobs must show **SUCCEEDED** status before proceeding.

---

### Step 10: Verify Iceberg Tables in Glue Data Catalog

**10.1 Check Tables in Glue Catalog**

```bash
# List tables in the Glue database
aws glue get-tables --database-name $GLUE_DB_NAME \
  --query 'TableList[*].[Name,StorageDescriptor.Location]' \
  --output table
```

**Expected Output:**
```
-----------------------------------------------------------
|                       GetTables                         |
+--------------------+-----------------------------------+
|  customers_info    |  s3://my-bucket/iceberg-warehouse/...|
|  raw_sales_data    |  s3://my-bucket/iceberg-warehouse/...|
-----------------------------------------------------------
```

**10.2 Verify Table Schemas**

```bash
# Get customer table schema
aws glue get-table --database-name $GLUE_DB_NAME --name customers_info \
  --query 'Table.StorageDescriptor.Columns'

# Get sales table schema
aws glue get-table --database-name $GLUE_DB_NAME --name raw_sales_data \
  --query 'Table.StorageDescriptor.Columns'
```

**10.3 Check Iceberg Data in S3**

```bash
# Verify Iceberg warehouse structure
aws s3 ls s3://$DATA_BUCKET/iceberg-warehouse/ --recursive
```

You should see Iceberg table structure:
```
iceberg-warehouse/<db-name>/customers_info/
    metadata/
    data/
iceberg-warehouse/<db-name>/raw_sales_data/
    metadata/
    data/
```

---

### Step 11: Run Glue ETL Job - Generate Sales Report

**11.1 Run Sales Report Job (Spark)**

This job reads from the `raw_sales_data` Iceberg table and generates the Top 10 products report.

```bash
# Using AWS CLI
aws glue start-job-run --job-name iceberg-sales-report-$GLUE_DB_NAME

# Monitor job
aws glue get-job-run \
  --job-name iceberg-sales-report-$GLUE_DB_NAME \
  --run-id <job-run-id> \
  --query 'JobRun.JobRunState'
```

**Using AWS Console:**
1. Select `iceberg-sales-report-<db-name>` job
2. Click **Run**
3. Monitor the run (check logs for detailed output)

**Expected Duration:** 3-5 minutes

**11.2 View Job Logs**

1. In the job run details, click **Logs** → **Output logs**
2. Look for output showing:
   - Data loading from Iceberg table
   - Flattening nested structures
   - Aggregation by product_id
   - Top 10 products report

**11.3 Verify Report Table Created**

```bash
# Check for the new report table
aws glue get-table --database-name $GLUE_DB_NAME --name top_10_products_report_spark

# Verify it's an Iceberg table
aws glue get-table --database-name $GLUE_DB_NAME --name top_10_products_report_spark \
  --query 'Table.Parameters'
```

---

### Step 12: Create Snowflake Report Table (Placeholder)

**12.1 Run Snowflake Report Table Creation Job**

This creates the table schema for the report that will be generated by Snowflake.

```bash
# Using AWS CLI
aws glue start-job-run --job-name iceberg-snow-report-$GLUE_DB_NAME
```

**Using AWS Console:**
1. Select `iceberg-snow-report-<db-name>` job
2. Click **Run**

**Expected Duration:** 1-2 minutes

**12.2 Verify Both Report Tables Exist**

```bash
aws glue get-tables --database-name $GLUE_DB_NAME \
  --query 'TableList[?starts_with(Name, `top_10`)].Name'
```

**Expected Output:**
```
[
    "top_10_products_report_spark",
    "top_10_products_report_snow"
]
```

---

### Step 13: Complete Snowflake Notebook Configuration

Now that CloudFormation has created all AWS resources, update the Snowflake notebook with the actual AWS values.

**13.1 Update Notebook Parameters**

Go back to your Snowflake notebook and update the following parameters across **multiple cells** (Cell 2 through Cell 8):

| Placeholder | Where to Find | Cells to Update |
|-------------|---------------|-----------------|
| `<Glue database Name>` | Your CloudFormation parameter | Cells 4, 6, 8, 10 |
| `<AWS REGION>` | Your AWS region | Cell 4 |
| `<AWS ACCOUNT NUMBER>` | Your AWS account ID | Cell 4 |
| `<AWS ROLE ARN>` | From Step 7 output | Cells 2, 4 |
| `<External ID used in cloud formation>` | Your chosen secret from Step 4 | Cell 4 |
| `<Your dataBucket name>` | Your S3 bucket name | Cell 2 |

**13.2 Cells to Update**

**Cell 2 - External Volume (Update if not done already)**
```sql
CREATE OR REPLACE EXTERNAL VOLUME extvol_iceberg_demo1
STORAGE_LOCATIONS = (
    (
        NAME = 'Iceberg-Table-Demo'
        STORAGE_PROVIDER = 'S3'
        STORAGE_BASE_URL = 's3://<Your dataBucket name>/'           -- UPDATE
        STORAGE_AWS_ROLE_ARN = '<AWS ROLE ARN>'                     -- UPDATE
        STORAGE_AWS_EXTERNAL_ID = '<Any secret word you want>'      -- UPDATE (same as CloudFormation)
    )
);
```

**Cell 4 - Catalog Integration**
```sql
CREATE OR REPLACE CATALOG INTEGRATION glue_rest_cat_int_demo
  CATALOG_SOURCE = ICEBERG_REST
  TABLE_FORMAT = ICEBERG
  CATALOG_NAMESPACE = '<Glue database Name>'                        -- UPDATE
  REST_CONFIG = (
    CATALOG_URI = 'https://glue.<AWS REGION>.amazonaws.com/iceberg' -- UPDATE
    CATALOG_API_TYPE = AWS_GLUE
    CATALOG_NAME = '<AWS ACCOUNT NUMBER>'                           -- UPDATE
  )
  REST_AUTHENTICATION = (
    TYPE = SIGV4
    SIGV4_IAM_ROLE = '<AWS ROLE ARN>'                               -- UPDATE
    SIGV4_SIGNING_REGION = '<AWS REGION like US-WEST-2>'            -- UPDATE
    SIGV4_EXTERNAL_ID = '<External ID used in cloud formation>'     -- UPDATE
  )
  ENABLED = TRUE;
```

**Cell 6 - Create Catalog-Linked Database**
```sql
CREATE DATABASE IF NOT EXISTS glue_lake_int_db
  LINKED_CATALOG = (
    CATALOG = 'glue_rest_cat_int_demo',
    ALLOWED_NAMESPACES = ('<Glue database name>'),                  -- UPDATE
    NAMESPACE_MODE = FLATTEN_NESTED_NAMESPACE,
    NAMESPACE_FLATTEN_DELIMITER = '-'
    SYNC_INTERVAL_SECONDS = 60
  )
  EXTERNAL_VOLUME = 'extvol_iceberg_demo';
```

**Cell 8 - Use Correct Schema**
```sql
use database glue_lake_int_db;
use schema "<glue database name>";                                   -- UPDATE
show schemas;
```

**Cell 10 - Set Catalog Variables**
```python
# Catalog configuration
catalog = "GLUE_LAKE_INT_DB"
database = "<Glue database Name>"                                    -- UPDATE
source_sales_table = "raw_sales_data"  
report_table = "top_10_products_report_snow"
```

---

### Step 14: Run Snowflake Notebook (Code Interoperability Demo)

**14.1 Understanding the Notebook Flow**

Cells 1-8: Snowflake setup and integration configuration
- Cell 0: Create database
- Cells 2-5: Configure external volume and catalog
- Cells 6-8: Create catalog-linked database
- **Cell 10 onwards: SAME CODE AS GLUE ETL JOB** ✨

**14.2 Run Setup Cells (if not already done)**

Run Cells 0 through 8 to complete the Snowflake integration setup.

**Cell 7 - Check Catalog Link Status**
```sql
SELECT SYSTEM$CATALOG_LINK_STATUS('glue_lake_int_db');
```

Expected output should show successful sync status.

**Cell 8 - Verify Tables are Discovered**
```sql
show tables;
```

You should see all 4 Iceberg tables that were created by Glue:
- `customers_info`
- `raw_sales_data`
- `top_10_products_report_spark`
- `top_10_products_report_snow`

**14.3 Initialize Snowpark Connect (Cell 13)**

```python
from snowflake import snowpark_connect
import traceback

spark = snowpark_connect.server.init_spark_session()
spark = snowpark_connect.get_session()

# Use lowercase table identifiers (Glue default)
spark.conf.set("snowpark.connect.sql.identifiers.auto-uppercase", "none")

# Test query - read from Glue-created table
query = f"select customer_name, purchases from {catalog}.{database}.{source_sales_table}"
spark.sql(query).show(5, truncate = False)
```

This cell verifies that Snowflake can read the Iceberg tables created by Glue.

**14.4 Run the Sales Report Analysis (Cells 16 onwards)**

**The Key Innovation:** Starting from Cell 16, the code is **IDENTICAL** to the Glue ETL script!

**Cell 16 - Read Sales Data** (Same as Glue script)
```python
sales_iceberg_df = spark.table(f"{catalog}.{database}.{source_sales_table}")
record_count = sales_iceberg_df.count()
print(f"Sales data loaded successfully from Iceberg table. Total records: {record_count:,}")
sales_iceberg_df.show(3, truncate=False)
```

**Cell 18 - Flatten and Clean Data** (Same as Glue script)
```python
flattened_sales = sales_iceberg_df.select(
    col("customer_id").alias("customer_id"),
    col("customer_name").alias("customer_name"),
    col("purchases.prodid").alias("product_id"),
    col("purchases.purchase_amount").alias("purchase_amount"),
    col("purchases.quantity").alias("quantity"),
    col("purchases.purchase_date").alias("purchase_date")
)

clean_sales = flattened_sales.filter(
    col("product_id").isNotNull() & 
    (col("product_id") > 0) & 
    col("purchase_amount").isNotNull() & 
    (col("purchase_amount") > 0) &
    col("quantity").isNotNull() & 
    (col("quantity") > 0)
)
```

**Cell 20 - Aggregate by Product** (Same as Glue script)
```python
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
)
```

**Cell 22 - Generate Top 10 Report** (Same as Glue script)
```python
top_10_products = product_sales_summary.orderBy(desc("total_sales_amount")).limit(10)
print("\n=== TOP 10 HIGHEST SELLING PRODUCTS BY SALES AMOUNT ===")
top_10_products.show(10, truncate=False)
```

**Cell 24 - Create Rankings with Window Functions** (Same as Glue script)
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

sales_window = Window.partitionBy(lit(1)).orderBy(desc("total_sales_amount"))

detailed_report = product_sales_summary.withColumn(
    "sales_amount_rank", row_number().over(sales_window)
).filter(col("sales_amount_rank") <= 10).orderBy("sales_amount_rank")
```

**Cell 26 - Save to Iceberg Table** (Same as Glue script)
```python
final_report = detailed_report.withColumn("report_date", current_date()) \
                             .withColumn("source_table", lit(source_sales_table)) \
                             .withColumn("job_name", lit(job_name))

# Get target table columns
target_df = spark.read.table(f"{catalog}.{database}.{report_table}")

# Truncate and insert
spark.sql(f"TRUNCATE TABLE {catalog}.{database}.{report_table}")
final_report.select(*target_df.columns).write.insertInto(f"{catalog}.{database}.{report_table}")
```

**14.5 Run All Analysis Cells**

Execute cells 16 through 26 sequentially. You should see:
1. Data loading and counts
2. Flattened data samples
3. Aggregated product summaries
4. Top 10 reports by various metrics
5. Final report saved to Iceberg

---

### Step 15: Verify Code Interoperability

**15.1 Compare Results**

The beauty of this architecture is that both platforms generate identical results!

**Query Glue-Generated Report:**
```sql
-- In Snowflake notebook (Cell 28)
spark.sql(f"select * from {catalog}.{database}.top_10_products_report_spark").show(10, truncate=False)
```

**Query Snowflake-Generated Report:**
```sql
spark.sql(f"select * from {catalog}.{database}.{report_table}").show(10, truncate=False)
```

**15.2 Verify in AWS Glue Console**

Go to **AWS Glue Console** → **Data Catalog** → **Tables**

Check the `top_10_products_report_snow` table:
- It should now have data (populated by Snowflake)
- Metadata automatically synced back to Glue catalog

**15.3 Key Observations**

✅ **Same Code**: 95%+ of the Spark code is identical between platforms  
✅ **Same Results**: Both reports show identical Top 10 products  
✅ **Same Data**: Both platforms query the same Iceberg files in S3  
✅ **Bi-directional**: Both can read and write to the shared catalog  
✅ **Zero Data Movement**: No ETL between platforms required

---

## Validation and Testing

### Validate End-to-End Flow

**1. Check All Tables in Glue Catalog**
```bash
aws glue get-tables --database-name $GLUE_DB_NAME \
  --query 'TableList[*].Name' --output table
```

Expected: 4 tables
- customers_info
- raw_sales_data
- top_10_products_report_spark
- top_10_products_report_snow

**2. Verify Data Counts**

In Snowflake notebook:
```python
# Check sales data count
spark.sql(f"SELECT COUNT(*) as total_records FROM {catalog}.{database}.raw_sales_data").show()
# Expected: ~1,000,000 records

# Check customer count
spark.sql(f"SELECT COUNT(*) as total_customers FROM {catalog}.{database}.customers_info").show()

# Check report data
spark.sql(f"SELECT COUNT(*) as report_rows FROM {catalog}.{database}.{report_table}").show()
# Expected: 10 rows (Top 10 products)
```

**3. Compare Report Outputs**

```python
# Get Glue-generated report
glue_report = spark.sql(f"SELECT * FROM {catalog}.{database}.top_10_products_report_spark ORDER BY sales_amount_rank").collect()

# Get Snowflake-generated report
snow_report = spark.sql(f"SELECT * FROM {catalog}.{database}.{report_table} ORDER BY sales_amount_rank").collect()

# Both should have same product_id rankings
```

**4. Test Catalog Sync**

Create a test table in Glue:
```bash
# Create a simple test table via Glue
aws glue create-table --database-name $GLUE_DB_NAME \
  --table-input '{
    "Name": "test_sync_table",
    "StorageDescriptor": {
      "Columns": [{"Name": "id", "Type": "int"}],
      "Location": "s3://'$DATA_BUCKET'/test/"
    }
  }'
```

Wait 60 seconds (sync interval), then check in Snowflake:
```sql
SHOW TABLES LIKE 'test_sync_table';
```

The table should appear automatically!

---



## Cost Considerations

### AWS Costs

| Service | Component | Estimated Cost (per run) |
|---------|-----------|--------------------------|
| **S3** | Storage (1M records + Iceberg metadata) | ~$0.01/month |
| **S3** | GET/PUT requests | ~$0.01 per execution |
| **AWS Glue** | ETL jobs (4 jobs × 3-5 min × 2 workers) | ~$0.88 per full run |
| **Glue Data Catalog** | Table storage (4 tables) | Free (first 1M objects) |
| **CloudFormation** | Stack management | Free |

**Estimated AWS cost per complete run:** ~$0.90

**To minimize costs:**
- Delete Glue tables when not in use (catalog charges after 1M objects)
- Use S3 Lifecycle policies to archive old Iceberg snapshots
- Stop/delete CloudFormation stack when demo is complete

### Snowflake Costs

| Service | Component | Estimated Cost |
|---------|-----------|----------------|
| **Compute** | Warehouse usage for notebook | Based on warehouse size and runtime |
| **Storage** | None (data stays in S3) | $0 |
| **Catalog Sync** | Automatic table discovery | Minimal cloud services credits |

**To minimize costs:**
- Use Small or X-Small warehouse for demos
- Suspend warehouse when not in use (auto-suspend recommended)
- Note: Starting December 2025, catalog-linked databases will incur charges

---

## Cleanup

### Complete Cleanup Process

**Step 1: Delete Snowflake Resources**

```sql
-- Drop catalog-linked database (also unlinks from Glue catalog)
DROP DATABASE IF EXISTS glue_lake_int_db;

-- Drop catalog integration
DROP CATALOG INTEGRATION IF EXISTS glue_rest_cat_int_demo;

-- Drop external volume
DROP EXTERNAL VOLUME IF EXISTS extvol_iceberg_demo1;

-- Drop demo database
DROP DATABASE IF EXISTS ICEBERG_LAKE;
```

**Step 2: Delete AWS CloudFormation Stack**

```bash
# Delete stack (will remove all Glue jobs, database, IAM role)
aws cloudformation delete-stack --stack-name iceberg-glue-snowflake-demo

# Wait for deletion to complete
aws cloudformation wait stack-delete-complete --stack-name iceberg-glue-snowflake-demo

# Verify deletion
aws cloudformation describe-stacks --stack-name iceberg-glue-snowflake-demo
# Should return error: "Stack does not exist"
```

**Step 3: Delete S3 Data**

```bash
# Delete all Iceberg data
aws s3 rm s3://$DATA_BUCKET/iceberg-warehouse/ --recursive

# Delete scripts and raw data
aws s3 rm s3://$DATA_BUCKET/scripts/ --recursive
aws s3 rm s3://$DATA_BUCKET/raw_data/ --recursive

# Optional: Delete the bucket itself
aws s3 rb s3://$DATA_BUCKET --force
```

**Step 4: Verify Cleanup**

```bash
# Check Glue database is gone
aws glue get-database --database-name $GLUE_DB_NAME
# Should return error

# Check Glue jobs are gone
aws glue list-jobs --query 'JobNames[?contains(@, `iceberg`)]'
# Should return empty list

# Check S3 bucket
aws s3 ls s3://$DATA_BUCKET
# Should be empty or not exist
```

---

## Summary

Congratulations! You've successfully:

✅ Deployed AWS Glue infrastructure with CloudFormation  
✅ Created Iceberg tables using Glue ETL jobs  
✅ Integrated Snowflake with AWS Glue Data Catalog  
✅ Ran identical Spark code on both Glue and Snowflake  
✅ Demonstrated true lakehouse interoperability  

### Key Takeaways

1. **Open Standards Work**: Apache Iceberg enables true multi-platform data access
2. **Code Portability is Real**: 95%+ of Spark code runs unchanged on Snowflake
3. **Zero Data Movement**: Both platforms query the same S3 data
4. **Catalog-Linked Databases**: Automatic table discovery eliminates manual setup
5. **Snowpark Connect**: Brings Spark API compatibility to Snowflake

### Next Steps

- **Experiment**: Try modifying the analytics logic and see it work on both platforms
- **Scale**: Test with larger datasets (10M+ records)
- **Integrate**: Connect BI tools (Tableau, PowerBI) to both Glue and Snowflake
- **Optimize**: Tune Iceberg partitioning and file sizes for performance
- **Extend**: Add more data sources and tables to the lakehouse

---

## Additional Resources

### Documentation Links
- [Snowflake Catalog-Linked Database](https://docs.snowflake.com/en/user-guide/tables-iceberg-catalog-linked-database)
- [Snowpark Connect Overview](https://docs.snowflake.com/en/developer-guide/snowpark-connect/snowpark-connect-overview)
- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [AWS Glue Developer Guide](https://docs.aws.amazon.com/glue/)
- [AWS Glue Iceberg Support](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html)

### Support
For issues or questions:
- AWS Glue: AWS Support Console
- Snowflake: Snowflake Support Portal
- Apache Iceberg: GitHub Issues

---

**Author:** Parag Jain  
**Project:** Snowflake Iceberg Lakehouse Demo  
**Last Updated:** November 2025  
**Version:** 1.0

