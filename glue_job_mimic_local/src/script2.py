import sys
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# -------------------------
# Load .env (local)
# -------------------------
try:
    from dotenv import load_dotenv
    load_dotenv()
except:
    pass

# -------------------------
# Optional AWS imports
# -------------------------
try:
    import boto3
    from awsglue.utils import getResolvedOptions
except:
    boto3 = None


# =========================
# CORE TRANSFORMATION (TESTABLE)
# =========================
def transform(df, batch_id, process_date):
    df = df.toDF(*[c.lower() for c in df.columns])

    df = df.withColumnRenamed("custid", "customer_id") \
        .withColumnRenamed("txnamt", "transaction_amount")

    df = df.withColumn("transaction_amount", col("transaction_amount").cast("double")) \
        .withColumn("quantity", col("quantity").cast("int")) \
        .withColumn("transaction_timestamp", col("transaction_timestamp").cast("timestamp"))

    df = df.withColumn("batch_id", lit(batch_id)) \
        .withColumn("process_date", lit(process_date))

    return df


# =========================
# LOCAL METADATA READER
# =========================
def read_local_metadata(path):
    with open(path, "r") as f:
        return json.load(f)


# =========================
# S3 METADATA READER
# =========================
def read_s3_metadata(s3, metadata_path):
    bucket = metadata_path.split("/")[2]
    key = "/".join(metadata_path.split("/")[3:])

    response = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(response['Body'].read())


# =========================
# MAIN RUNNER
# =========================
def run(mode="local"):
    spark = SparkSession.builder.appName("job").getOrCreate()

    # -----------------------
    # LOCAL MODE
    # -----------------------
    if mode == "local":
        metadata_path = os.getenv("METADATA_PATH")
        target_path = os.getenv("TARGET_PATH")

        metadata = read_local_metadata(metadata_path)

    # -----------------------
    # PROD MODE (GLUE)
    # -----------------------
    elif mode == "prod":
        args = getResolvedOptions(sys.argv, [
            'TARGET_PATH',
            'WORKFLOW_NAME',
            'WORKFLOW_RUN_ID'
        ])

        target_path = args['TARGET_PATH']
        workflow_name = args['WORKFLOW_NAME']
        run_id = args['WORKFLOW_RUN_ID']

        s3 = boto3.client("s3")
        glue = boto3.client("glue")

        response = glue.get_workflow_run_properties(
            Name=workflow_name,
            RunId=run_id
        )

        run_props = response['RunProperties']

        if 'metadata_path' not in run_props:
            raise Exception("metadata_path not found")

        metadata_path = run_props['metadata_path']

        metadata = read_s3_metadata(s3, metadata_path)

    else:
        raise ValueError("Invalid mode")

    # -----------------------
    # COMMON LOGIC
    # -----------------------
    file_list = metadata['files']
    batch_id = metadata['batch_id']
    process_date = metadata['process_date']

    print(f"Files to process: {len(file_list)}")

    if not file_list:
        print("No new files. Skipping.")
        return

    # -----------------------
    # READ DATA
    # -----------------------
    df = spark.read.option("header", True).csv(file_list)

    # -----------------------
    # TRANSFORM
    # -----------------------
    df = transform(df, batch_id, process_date)

    # -----------------------
    # WRITE
    # -----------------------
    df.write.mode("append") \
        .partitionBy("process_date") \
        .parquet(target_path)

    print(f"Data written to {target_path}")


# =========================
# ENTRY POINT
# =========================
def main():
    # Try Glue args first
    try:
        args = getResolvedOptions(sys.argv, ['ENV'])
        ENV = args['ENV']
    except:
        ENV = os.getenv("ENV", "local")

    run(mode=ENV)


if __name__ == "__main__":
    main()