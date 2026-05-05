import sys
import json
import boto3
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# -----------------------------
# STEP 0: Read configs
# -----------------------------
required_args = ['TARGET_PATH']
optional_args = ['WORKFLOW_NAME', 'WORKFLOW_RUN_ID']

args = getResolvedOptions(sys.argv, required_args)

for opt in optional_args:
    if f'--{opt}' in sys.argv:
        args.update(getResolvedOptions(sys.argv, [opt]))

TARGET_PATH = args['TARGET_PATH']
WORKFLOW_NAME = args.get('WORKFLOW_NAME')
WORKFLOW_RUN_ID = args.get('WORKFLOW_RUN_ID')

spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Enable schema evolution
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# --datalake-formats delta
s3 = boto3.client('s3')
glue = boto3.client('glue')

# -----------------------------
# STEP 1: Get metadata_path
# -----------------------------
if WORKFLOW_NAME and WORKFLOW_RUN_ID:
    try:
        response = glue.get_workflow_run_properties(
            Name=WORKFLOW_NAME,
            RunId=WORKFLOW_RUN_ID
        )
        run_props = response['RunProperties']
        metadata_path = run_props.get('metadata_path')

        if not metadata_path:
            raise Exception("metadata_path not found in workflow properties")

    except Exception as e:
        raise Exception(f"Failed to fetch metadata from workflow: {str(e)}")
else:
    raise Exception("This job requires metadata_path via workflow")

print("Metadata path:", metadata_path)

# -----------------------------
# STEP 2: Read metadata
# -----------------------------
bucket = metadata_path.split("/")[2]
key = "/".join(metadata_path.split("/")[3:])

response = s3.get_object(Bucket=bucket, Key=key)
metadata = json.loads(response['Body'].read())

file_details = metadata.get('files', [])
file_list = [f['file_path'] for f in file_details]

batch_id = metadata['batch_id']
process_date = metadata['process_date']

print(f"Files to process: {len(file_list)}")

# -----------------------------
# STEP 3: Exit if no files
# -----------------------------
if not file_list:
    print("No new files. Skipping processing.")

    metadata['status'] = "NO_FILES"
    metadata['job_end_time'] = datetime.utcnow().isoformat()

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(metadata, indent=2)
    )

    sys.exit(0)

# -----------------------------
# STEP 4: Process data (UPDATED)
# -----------------------------
try:
    df = spark.read.option("header", True).csv(file_list)

    # Normalize column names
    df = df.toDF(*[c.lower() for c in df.columns])

    # -----------------------------------
    # Dynamic Column Mapping (Alias-based)
    # -----------------------------------
    COLUMN_ALIASES = {
        "customer_id": [
            "custid", "customerid", "cust_id", "customer", "cust"
        ],
        
        "transaction_amount": [
            "txnamt", "amount", "txn_amount", "transactionamt", "amt"
        ],
        
        "transaction_timestamp": [
            "transaction_timestamp", "txn_time", "timestamp", "txn_timestamp", "time"
        ],
        
        "quantity": [
            "qty", "quantity", "no_of_items", "count"
        ],
        
        "transaction_id": [
            "transaction_id", "txn_id", "txnid", "id"
        ],
        
        "product_id": [
            "product_id", "productid", "prod_id", "pid"
        ],
        
        "country": [
            "country", "country_code", "location"
        ],
        
        "status": [
            "status", "txn_status", "state"
        ]
    }

    for standard_col, possible_cols in COLUMN_ALIASES.items():
        matches = [c for c in possible_cols if c in df.columns]

        if len(matches) > 1:
            raise Exception(f"Multiple columns found for {standard_col}: {matches}")

        if len(matches) == 1:
            df = df.withColumnRenamed(matches[0], standard_col)

    # -----------------------------------
    # Expected Schema
    # -----------------------------------
    EXPECTED_SCHEMA = {
        "transaction_id": "string",
        "customer_id": "string",
        "transaction_amount": "double",
        "transaction_timestamp": "timestamp",
        "country": "string",
        "product_id": "string",
        "quantity": "int",
        "status": "string"
    }

    # Add missing columns
    for col_name in EXPECTED_SCHEMA:
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None))

    # Safe type casting
    for col_name, dtype in EXPECTED_SCHEMA.items():
        df = df.withColumn(col_name, col(col_name).cast(dtype))

    # Add metadata columns (same as original)
    df = df.withColumn("batch_id", lit(batch_id)) \
           .withColumn("process_date", lit(process_date))

    # -----------------------------------
    # Data Quality Handling (NEW)
    # -----------------------------------
    bad_df = df.filter(col("customer_id").isNull())
    good_df = df.filter(col("customer_id").isNotNull())

    bad_count = bad_df.count()
    if bad_count > 0:
        print(f"⚠️ Bad records found: {bad_count}")
        bad_df.write.mode("append").parquet(f"{TARGET_PATH}/bad_records/")

    # -----------------------------------
    # Write output (Delta - same logic enhanced)
    # -----------------------------------
    good_df.write \
        .format("delta") \
        .option("mergeSchema", "true") \
        .mode("append") \
        .partitionBy("process_date") \
        .save(TARGET_PATH)

    print(f"Data written to {TARGET_PATH}")

    # Cache before count (optimization)
    good_df.cache()
    metadata['records_processed'] = good_df.count()

    # -----------------------------
    # STEP 5: Update metadata SUCCESS
    # -----------------------------
    metadata['status'] = "SUCCESS"
    metadata['job_end_time'] = datetime.utcnow().isoformat()

except Exception as e:
    print("Processing failed:", str(e))

    metadata['status'] = "FAILED"
    metadata['error_message'] = str(e)
    metadata['job_end_time'] = datetime.utcnow().isoformat()

    raise e

# -----------------------------
# STEP 6: Save updated metadata
# -----------------------------
s3.put_object(
    Bucket=bucket,
    Key=key,
    Body=json.dumps(metadata, indent=2)
)

print("Metadata updated with final status")
