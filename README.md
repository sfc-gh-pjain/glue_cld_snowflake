# sc_vhol

This is the working directory for the new project.

Add files and subfolders here; I will treat `sc_vhol` as the working root going forward.

Deployment
----------

Template: `cloudformation/iceberg_glue_stack_cloudformation_template.yaml`

Parameters (required):
- `GlueDatabaseName`: Glue Data Catalog database for Iceberg tables
- `GlueRoleName`: IAM role name for Glue jobs
- `DataBucketName`: Existing S3 bucket used for data and the Iceberg warehouse
- `S3ScriptLocation`: S3 URI to a Glue ETL script to run with the generic job (e.g., one of the scripts below)

What the stack creates:
- Glue IAM Role with S3 access to `DataBucketName`
- Glue Database (in Data Catalog)
- Glue 4.0 Jobs for each script under `s3://${DataBucketName}/scripts/`:
  - `iceberg-sales-table-${GlueDatabaseName}` → `scripts/iceberg_cld_demo_sales_table_script_1.py`
  - `iceberg-customer-table-${GlueDatabaseName}` → `scripts/iceberg_cld_demo_customer_table_script_2.py`
  - `iceberg-sales-report-${GlueDatabaseName}` → `scripts/iceberg_cld_demo_sales_report_table_script_3.py`
  - `iceberg-snow-report-${GlueDatabaseName}` → `scripts/iceberg_cld_demo_snow_report_table_script_4.py`
- A generic Glue job `iceberg-create-${GlueDatabaseName}` that runs `S3ScriptLocation`

How scripts receive configuration:
- Each job passes DefaultArguments:
  - `--data-bucket-name` = `DataBucketName`
  - `--glue-database-name` = `GlueDatabaseName`
- Scripts read these with `getResolvedOptions` and use them for S3 paths and database.
- Iceberg Spark conf (extensions/catalog/warehouse) is set via job DefaultArguments.

Upload scripts to S3 (place under scripts/):
```bash
export DATA_BUCKET=your-data-bucket
aws s3 sync scripts s3://$DATA_BUCKET/scripts/ --exclude "*" --include "*.py"
```

Create the stack (delete-on-failure enabled):
```bash
aws cloudformation create-stack \
  --stack-name sc-vhol-iceberg \
  --template-body file://cloudformation/iceberg_glue_stack_cloudformation_template.yaml \
  --capabilities CAPABILITY_NAMED_IAM \
  --on-failure DELETE \
  --parameters \
      ParameterKey=GlueDatabaseName,ParameterValue=your_db \
      ParameterKey=GlueRoleName,ParameterValue=your_glue_role \
      ParameterKey=DataBucketName,ParameterValue=$DATA_BUCKET \
      ParameterKey=S3ScriptLocation,ParameterValue=s3://$DATA_BUCKET/scripts/iceberg_cld_demo_sales_table_script_1.py
```

Run Glue jobs:
```bash
# Replace <db> with your GlueDatabaseName
aws glue start-job-run --job-name iceberg-sales-table-<db>
aws glue start-job-run --job-name iceberg-customer-table-<db>
aws glue start-job-run --job-name iceberg-sales-report-<db>
aws glue start-job-run --job-name iceberg-snow-report-<db>

# Or run the generic job with any script via S3ScriptLocation
aws glue start-job-run --job-name iceberg-create-<db>
```

Data locations expected by scripts (under `s3://$DATA_BUCKET/`):
- `iceberg/raw_data/customers.json`
- `iceberg/raw_data/sales_data.json` (multiline JSON)
- Warehouse path: `iceberg/spark_warehouse`

Enable delete newly created resources during rollback
-----------------------------------------------------

- Console (Create/Update stack): On the options step, enable “Delete newly created resources during rollback.” This ensures any resources created in the failed operation are deleted.
- CLI (Create stack): The equivalent is `--on-failure DELETE` as shown above.
- CLI (Update stack): This setting isn’t currently exposed as a dedicated CLI flag; use the console for this behavior during updates.

Notes
-----

- The stack assumes `DataBucketName` already exists (no bucket is created by this template).
- Glue jobs configure Iceberg via DefaultArguments (Spark conf). In-script catalog config can be removed.
- Ensure the Glue IAM role has access if your data is outside `DataBucketName`.

