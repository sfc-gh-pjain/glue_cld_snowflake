## Troubleshooting

### Common Issues and Solutions

#### Issue 1: CloudFormation Stack Creation Failed

**Error:** IAM permissions denied

**Solution:**
- Ensure your AWS user has permissions to create IAM roles
- Add `iam:CreateRole`, `iam:PutRolePolicy`, `iam:AttachRolePolicy` permissions
- Or use an admin user for initial deployment

**Error:** S3 bucket not found

**Solution:**
- Verify the bucket exists: `aws s3 ls s3://$DATA_BUCKET`
- Ensure bucket name in parameters matches exactly
- Check bucket is in the same region as CloudFormation stack

---

#### Issue 2: Glue Job Fails

**Error:** "Access Denied" to S3 bucket

**Solution:**
```bash
# Check IAM role permissions
aws iam get-role-policy --role-name $GLUE_ROLE_NAME --policy-name GlueIcebergS3Access

# Ensure the role has s3:GetObject, s3:PutObject, s3:ListBucket
```

**Error:** "No JSON object could be decoded"

**Solution:**
- Verify data files uploaded correctly to S3
- Check file paths in the script match S3 structure:
  - `s3://<bucket>/raw_data/customers.json`
  - `s3://<bucket>/raw_data/sales_data_1m.json.gz`

**Error:** "Table already exists"

**Solution:**
- If re-running, delete old tables first:
```bash
aws glue delete-table --database-name $GLUE_DB_NAME --name customers_info
aws glue delete-table --database-name $GLUE_DB_NAME --name raw_sales_data
```

---

#### Issue 3: Snowflake External Volume Creation Failed

**Error:** "External volume could not be created"

**Solution:**
- Ensure you have `ACCOUNTADMIN` or `CREATE EXTERNAL VOLUME` privilege
- Check S3 bucket name is correct and accessible
- Verify IAM role ARN format: `arn:aws:iam::<account-id>:role/<role-name>`

---

#### Issue 4: Catalog Integration Fails

**Error:** "Unable to connect to catalog"

**Solution:**
- Verify Glue database name matches exactly (case-sensitive)
- Check AWS region is correct in CATALOG_URI
- Ensure IAM role has Glue permissions:
```json
{
  "Effect": "Allow",
  "Action": [
    "glue:GetDatabase",
    "glue:GetTable",
    "glue:GetTables",
    "glue:CreateTable",
    "glue:UpdateTable"
  ],
  "Resource": "*"
}
```

**Error:** "External ID mismatch"

**Solution:**
- The external ID in Cell 4 (Catalog Integration) must match:
  - The External ID from Cell 3 output (STORAGE_AWS_EXTERNAL_ID)
  - The SnowflakeExternalId parameter in CloudFormation
- All three must be identical

---

#### Issue 5: Tables Not Appearing in Snowflake

**Error:** `SHOW TABLES` returns empty in catalog-linked database

**Solution:**
1. Check catalog link status:
```sql
SELECT SYSTEM$CATALOG_LINK_STATUS('glue_lake_int_db');
```

2. Verify sync interval hasn't expired (default 60 seconds)

3. Check ALLOWED_NAMESPACES includes your Glue database:
```sql
SHOW DATABASES LIKE 'glue_lake_int_db';
-- Check ALLOWED_NAMESPACES parameter
```

4. Verify tables exist in Glue:
```bash
aws glue get-tables --database-name $GLUE_DB_NAME
```

5. If tables still don't appear, drop and recreate catalog-linked database

---

#### Issue 6: Snowpark Connect Not Working

**Error:** "Module 'snowpark_connect' not found"

**Solution:**
- Ensure Snowpark Connect package is added in Packages tab
- Restart notebook kernel
- Check Snowflake Runtime is 2.0 or later

**Error:** "Spark session could not be initialized"

**Solution:**
- Verify warehouse is running and accessible
- Check you have USAGE privilege on the warehouse
- Try restarting the notebook

---

#### Issue 7: Query Results Don't Match

**Error:** Different row counts or values between Glue and Snowflake

**Solution:**
1. Check if data is being appended multiple times:
```python
# Verify total records
spark.sql(f"SELECT COUNT(*) FROM {catalog}.{database}.raw_sales_data").show()
```

2. Ensure scripts use TRUNCATE before INSERT:
```python
spark.sql(f"TRUNCATE TABLE {catalog}.{database}.{table}")
```

3. Check Iceberg metadata refresh:
```sql
ALTER ICEBERG TABLE <table_name> REFRESH;
```

---

#### Issue 8: Permission Errors in Snowflake

**Error:** "Insufficient privileges to operate on database"

**Solution:**
```sql
-- Grant necessary privileges
GRANT USAGE ON DATABASE glue_lake_int_db TO ROLE <your_role>;
GRANT USAGE ON ALL SCHEMAS IN DATABASE glue_lake_int_db TO ROLE <your_role>;
GRANT SELECT ON ALL TABLES IN DATABASE glue_lake_int_db TO ROLE <your_role>;
```

---

### Debug Commands

**Check Glue Job Logs:**
```bash
# Get recent job run
RUN_ID=$(aws glue get-job-runs --job-name iceberg-sales-table-$GLUE_DB_NAME \
  --max-results 1 --query 'JobRuns[0].Id' --output text)

# Get log stream
aws glue get-job-run --job-name iceberg-sales-table-$GLUE_DB_NAME \
  --run-id $RUN_ID --query 'JobRun.LogGroupName'
```

**Check S3 Iceberg Structure:**
```bash
# List Iceberg metadata
aws s3 ls s3://$DATA_BUCKET/iceberg-warehouse/$GLUE_DB_NAME/raw_sales_data/metadata/

# Check data files
aws s3 ls s3://$DATA_BUCKET/iceberg-warehouse/$GLUE_DB_NAME/raw_sales_data/data/ --recursive
```

**Test IAM Role Trust Policy:**
```bash
# Get role trust policy
aws iam get-role --role-name $GLUE_ROLE_NAME \
  --query 'Role.AssumeRolePolicyDocument'

# Should show both Glue service and Snowflake user ARN as trusted principals
```

---