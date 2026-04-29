import sys
import json
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# -----------------------------
# STEP 0: Read configs
# -----------------------------
args = getResolvedOptions(sys.argv, [
    'TARGET_PATH',
    'WORKFLOW_NAME',
    'WORKFLOW_RUN_ID'
])
TARGET_PATH = args['TARGET_PATH']
WORKFLOW_NAME = args['WORKFLOW_NAME']
WORKFLOW_RUN_ID = args['WORKFLOW_RUN_ID']

spark = SparkSession.builder.getOrCreate()
s3 = boto3.client('s3')
glue = boto3.client('glue')
# -----------------------------
# STEP 1: Get metadata_path from workflow
# -----------------------------
response = glue.get_workflow_run_properties(
    Name=WORKFLOW_NAME,
    RunId=WORKFLOW_RUN_ID
)

run_props = response['RunProperties']

if 'metadata_path' not in run_props:
    raise Exception("metadata_path not found in workflow properties")

metadata_path = run_props['metadata_path']

print("Received metadata_path:", metadata_path)

# -----------------------------
# STEP 1: Read metadata
# -----------------------------
bucket = metadata_path.split("/")[2]
key = "/".join(metadata_path.split("/")[3:])

response = s3.get_object(Bucket=bucket, Key=key)
metadata = json.loads(response['Body'].read())

file_list = metadata['files']
batch_id = metadata['batch_id']
process_date = metadata['process_date']

print(f"Files to process: {len(file_list)}")

# -----------------------------
# STEP 2: Exit if no files
# -----------------------------

if not file_list:
    print("No new files. Skipping processing.")
else:
    # df = spark.read.option("header", True).csv(file_list)
    # rest of logic
# -----------------------------
# STEP 3: Read data
# -----------------------------
    df = spark.read.option("header", True).csv(file_list)

    # -----------------------------
    # STEP 4: Standardize schema
    # -----------------------------
    df = df.toDF(*[c.lower() for c in df.columns])

    df = df.withColumnRenamed("custid", "customer_id") \
        .withColumnRenamed("txnamt", "transaction_amount")

    # -----------------------------
    # STEP 5: Data type conversion
    # -----------------------------
    df = df.withColumn("transaction_amount", col("transaction_amount").cast("double")) \
        .withColumn("quantity", col("quantity").cast("int")) \
        .withColumn("transaction_timestamp", col("transaction_timestamp").cast("timestamp"))

    # -----------------------------
    # STEP 6: Add metadata columns
    # -----------------------------
    df = df.withColumn("batch_id", lit(batch_id)) \
        .withColumn("process_date", lit(process_date))

    # -----------------------------
    # STEP 7: Write output
    # -----------------------------
    df.write.mode("append") \
    .partitionBy("process_date") \
    .parquet(TARGET_PATH)

    print(f"Data written to {TARGET_PATH}")