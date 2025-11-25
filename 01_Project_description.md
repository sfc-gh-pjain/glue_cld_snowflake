# Project Description: Interoperable Lakehouse Architecture with AWS Glue, Apache Iceberg, and Snowflake

## Overview

This project demonstrates a modern, interoperable lakehouse architecture that seamlessly integrates **AWS Glue**, **Apache Iceberg**, and **Snowflake** to enable data processing workloads across multiple compute engines while maintaining a single source of truth for data storage. The solution showcases how organizations can leverage best-of-breed tools without vendor lock-in, allowing Spark code written for AWS Glue to run unmodified on Snowflake using Snowpark Connect.

## Business Problem

Modern data architectures often face challenges with:
- **Vendor Lock-in**: Data and compute are tightly coupled, making it difficult to switch between platforms
- **Code Portability**: Analytics code needs to be rewritten when moving between different processing engines
- **Data Duplication**: Moving data between systems creates multiple copies, increasing storage costs and data consistency issues
- **Limited Interoperability**: Different teams using different tools can't easily share data and insights

## Solution Architecture

This project solves these challenges by implementing an **open lakehouse architecture** that separates storage from compute and uses open standards (Apache Iceberg) to enable seamless interoperability.
![Lakehouse using Snowflake and AWS](diagrams/arch.jpeg)
### Key Components

#### 1. **AWS Glue with Apache Iceberg**
- **Infrastructure as Code**: CloudFormation template deploys complete Glue infrastructure including:
  - Glue Database for Iceberg table metadata
  - IAM roles with appropriate S3 permissions
  - 4 pre-configured Glue ETL jobs (Glue 4.0)
  - Generic job for ad-hoc script execution
- **Iceberg Table Format**: Uses Apache Iceberg for ACID transactions, schema evolution, and time travel
- **Data Processing**: Processes 1M+ sales records with nested JSON structures
- **Analytics**: Generates Top 10 product sales reports with comprehensive metrics

#### 2. **Snowflake Catalog-Linked Database**
The project leverages [Snowflake's Catalog-Linked Database](https://docs.snowflake.com/en/user-guide/tables-iceberg-catalog-linked-database) feature to create true interoperability:

**What is a Catalog-Linked Database?**
- A Snowflake database connected to an external Iceberg REST catalog (AWS Glue Data Catalog in this case)
- Automatically syncs with the external catalog to detect namespaces and Iceberg tables
- No need to create individual externally managed tables - Snowflake discovers them automatically
- Supports both **read and write operations** to the remote catalog

**Benefits for This Project:**
- **Zero Data Movement**: Snowflake queries the same Iceberg tables stored in S3 that Glue created
- **Automatic Discovery**: Tables created by Glue jobs automatically appear in Snowflake (sync interval: 30-60 seconds)
- **Unified Catalog**: Single source of truth for metadata in AWS Glue Data Catalog
- **Bi-directional Write Support**: Both Glue and Snowflake can create and modify tables
- **No Data Duplication**: Data stays in S3, accessed by both platforms

**How It Works:**
1. **Catalog Integration**: Snowflake connects to AWS Glue Data Catalog via Iceberg REST API
2. **External Volume**: Configures S3 access with IAM role and external ID for security
3. **Automatic Sync**: Snowflake polls the Glue catalog (configurable interval) and registers discovered tables
4. **Namespace Mapping**: Glue database schemas appear as Snowflake schemas, tables are automatically mapped

#### 3. **Snowpark Connect for Code Portability**
The project uses [Snowpark Connect](https://docs.snowflake.com/en/developer-guide/snowpark-connect/snowpark-connect-overview) to enable Apache Spark code to run on Snowflake without modification:

**What is Snowpark Connect?**
- An implementation of Apache Spark's DataFrame API that runs on Snowflake's compute
- Allows existing Spark applications to run on Snowflake with minimal or no code changes
- Provides Spark-compatible API while leveraging Snowflake's optimized query engine

**Code Portability Demonstration:**
The project includes identical analytics code that runs on both platforms:

| **AWS Glue Script** | **Snowflake Notebook** | **Result** |
|---------------------|------------------------|------------|
| `iceberg_cld_demo_sales_report_table_script_3.py` | `01_GLUE_LH_SNOWPARK_CONNECT_DEMO.ipynb` | Same Top 10 sales report |

**Key Features:**
- **Same DataFrame Operations**: `groupBy`, `agg`, `withColumn`, `orderBy`, etc.
- **Same Functions**: `col()`, `sum()`, `count()`, `avg()`, `desc()`, window functions
- **Same Business Logic**: Product aggregation, ranking, and report generation
- **Zero Code Changes**: Only configuration changes (catalog name, session initialization)

**The Magic:**
```python
# This exact code runs on BOTH AWS Glue and Snowflake
product_sales_summary = clean_sales.groupBy("product_id").agg(
    spark_sum("purchase_amount").alias("total_sales_amount"),
    spark_sum("quantity").alias("total_quantity_sold"),
    count("*").alias("total_transactions"),
    avg("purchase_amount").alias("avg_transaction_amount"),
    countDistinct("customer_id").alias("unique_customers")
)

top_10_products = product_sales_summary.orderBy(desc("total_sales_amount")).limit(10)
```

## Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                     S3 Data Bucket                              │
│  • raw_data/sales_data_1m.json.gz (1M records)                 │
│  • raw_data/customers.json                                      │
│  • iceberg-warehouse/ (Iceberg table storage)                   │
└────────────┬──────────────────────────────────┬─────────────────┘
             │                                  │
             ▼                                  ▼
┌─────────────────────────┐        ┌──────────────────────────────┐
│   AWS Glue ETL Jobs     │        │  AWS Glue Data Catalog       │
│  (Spark on AWS)         │◄──────►│  (Iceberg REST Catalog)      │
│                         │        │  • Metadata for all tables   │
│  Script 1: Load Sales   │        │  • Schema definitions        │
│  Script 2: Load Customers│       │  • Partition info            │
│  Script 3: Sales Report │        └──────────┬───────────────────┘
│  Script 4: Placeholder  |                   |
|  for Snowflake          │                   │
└─────────────────────────┘                   │ Iceberg REST API
                                              │ (Catalog Integration)
                                              │
                                              ▼
                                   ┌──────────────────────────────┐
                                   │ Snowflake Catalog-Linked DB  │
                                   │  (Snowpark Connect)          │
                                   │                              │
                                   │  • Auto-discovers tables     │
                                   │  • Notebook with Spark code  │
                                   │  • Generates same report     │
                                   │  • Zero data movement        │
                                   └──────────────────────────────┘
```

## Use Case: Sales Analytics

### Dataset
- **Sales Data**: 1M transaction records (gzipped JSON)
  - Customer ID, Name
  - Nested purchase structure (product ID, amount, quantity, date)
- **Customer Data**: Customer profiles with demographics

### ETL Pipeline (AWS Glue)

**Job 1 - Load Sales Data** (`iceberg_cld_demo_sales_table_script_1.py`)
- Reads 1M records from gzipped JSON
- Handles nested purchase data with struct types
- Loads into Iceberg table: `raw_sales_data`
- Schema: customer_id, customer_name, purchases (struct)

**Job 2 - Load Customer Data** (`iceberg_cld_demo_customer_table_script_2.py`)
- Reads customer JSON data
- Type casting and column selection
- Loads into Iceberg table: `customers_info`
- Schema: customer_id, customer_name, age

**Job 3 - Sales Report** (`iceberg_cld_demo_sales_report_table_script_3.py`)
- Reads from `raw_sales_data` Iceberg table
- Flattens nested purchase structure
- Aggregates by product_id:
  - Total sales amount
  - Total quantity sold
  - Unique customers
  - Transaction counts
  - Average transaction metrics
- Generates Top 10 products by sales amount
- Saves to Iceberg table: `top_10_products_report_spark`

**Job 4 - Create Snowflake Report Table** (`iceberg_cld_demo_snow_report_table_script_4.py`)
- Creates report table schema for Snowflake writes
- Table: `top_10_products_report_snow`

### Analytics Notebook (Snowflake)

**Snowflake Notebook** (`01_GLUE_LH_SNOWPARK_CONNECT_DEMO.ipynb`)
1. **Setup Snowflake Integration**:
   - Create External Volume for S3 access
   - Create Catalog Integration for AWS Glue catalog
   - Create Catalog-Linked Database
   - Verify sync status

2. **Initialize Snowpark Connect**:
   ```python
   from snowflake import snowpark_connect
   spark = snowpark_connect.get_session()
   ```

3. **Run Identical Spark Code**:
   - Same DataFrame operations as Glue Script 3
   - Same aggregations and transformations
   - Same window functions for ranking
   - Generates identical Top 10 products report

4. **Write Results**:
   - Saves to `top_10_products_report_snow` Iceberg table
   - Results immediately available in Glue catalog
   - Can be queried by Glue jobs

## Key Innovation: True Interoperability

### What Makes This Special?

1. **Open Standards**: Apache Iceberg provides vendor-neutral table format
2. **Unified Catalog**: AWS Glue Data Catalog serves both platforms
3. **Automatic Sync**: Changes appear in both systems within 30-60 seconds
4. **Code Portability**: 95%+ code reuse between Glue and Snowflake
5. **No Data Movement**: Both platforms query the same S3 data
6. **Bi-directional**: Both platforms can create and modify tables

### Real-World Benefits

**For Data Engineers:**
- Write Spark code once, run anywhere
- Use the best tool for each job
- No vendor lock-in

**For Organizations:**
- Reduce infrastructure costs (no data duplication)
- Improve data consistency (single source of truth)
- Enable multi-cloud strategies
- Faster time to value (no data migration needed)

**For Analytics Teams:**
- Access all data regardless of processing tool
- Consistent results across platforms
- Flexible compute choices based on workload

## Technical Highlights

### Iceberg Features Used
- **ACID Transactions**: Reliable writes from multiple engines
- **Schema Evolution**: Add columns without breaking existing queries
- **Partition Evolution**: Change partitioning strategy without rewriting data
- **Time Travel**: Query historical data states
- **Hidden Partitioning**: Automatic partition management

### AWS Services
- **AWS Glue 4.0**: Latest Spark 3.5 with Iceberg 1.5+
- **AWS Glue Data Catalog**: Central metadata store
- **Amazon S3**: Scalable data lake storage
- **AWS IAM**: Secure access with external IDs
- **CloudFormation**: Infrastructure as Code

### Snowflake Features
- **Catalog-Linked Database**: Automatic table discovery and sync
- **Snowpark Connect**: Spark API compatibility
- **External Volumes**: Secure S3 access with credential vending
- **Catalog Integration**: Iceberg REST API connectivity
- **Notebooks**: Interactive development environment

## Deployment Model

### Infrastructure Automation
- Single CloudFormation template deploys entire stack
- Parameterized for different environments
- Optional Snowflake integration configuration
- Rollback protection for failed deployments

### Security
- IAM roles with least privilege access
- External IDs for cross-account access
- S3 bucket policies for data protection
- Glue Data Catalog encryption
- Snowflake credential vending

### Scalability
- Handles 1M+ records efficiently
- Configurable Glue worker counts
- Snowflake elastic compute
- Partitioned Iceberg tables for performance
- Optimized with Adaptive Query Execution

## Future Enhancements

- **Multi-Region**: Replicate Iceberg tables across regions
- **Data Governance**: Add Snowflake tags and masking policies
- **Streaming**: Real-time data ingestion with Kinesis/Kafka
- **Delta Sharing**: Share datasets with external organizations
- **ML Workloads**: Run Spark ML models on both platforms
- **Cost Optimization**: Automated cost analysis and optimization

## Conclusion

This project demonstrates that organizations no longer need to choose between platforms or maintain multiple copies of data. By leveraging open standards like Apache Iceberg and modern integration features like Snowflake's Catalog-Linked Databases and Snowpark Connect, teams can build truly interoperable lakehouse architectures that provide:

- **Freedom of Choice**: Use the best tool for each job
- **Cost Efficiency**: No data duplication, optimized compute
- **Code Portability**: Write once, run anywhere
- **Future Proof**: Built on open standards, not proprietary formats

The same Spark code that processes data in AWS Glue can run unchanged in Snowflake, querying the same Iceberg tables in S3, demonstrating the power of modern open lakehouse architecture.

## References

- [Snowflake Catalog-Linked Database Documentation](https://docs.snowflake.com/en/user-guide/tables-iceberg-catalog-linked-database)
- [Snowpark Connect Overview](https://docs.snowflake.com/en/developer-guide/snowpark-connect/snowpark-connect-overview)
- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)

---

**Project Author**: Parag Jain  
**Purpose**: Snowflake Iceberg Lakehouse Demo  
**Last Updated**: November 2025

