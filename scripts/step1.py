import sys
import json
import boto3
from datetime import datetime
from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError

# -----------------------------
# STEP 0: Read configs
# -----------------------------
args = getResolvedOptions(sys.argv, [
    'LANDING_BUCKET',
    'LANDING_PREFIX',
    'METADATA_BUCKET',
    'WATERMARK_KEY',
    'BATCH_PREFIX',
    'WORKFLOW_NAME',
    'WORKFLOW_RUN_ID'
])

LANDING_BUCKET = args['LANDING_BUCKET']
LANDING_PREFIX = args['LANDING_PREFIX']
METADATA_BUCKET = args['METADATA_BUCKET']
WATERMARK_KEY = args['WATERMARK_KEY']
BATCH_PREFIX = args['BATCH_PREFIX']
WORKFLOW_NAME = args['WORKFLOW_NAME']
WORKFLOW_RUN_ID = args['WORKFLOW_RUN_ID']

s3 = boto3.client('s3')
glue = boto3.client('glue')

# -----------------------------
# STEP 1: Capture cutoff time (CRITICAL)
# -----------------------------
job_start_time = datetime.utcnow().isoformat()
print(f"Job start cutoff: {job_start_time}")

# -----------------------------
# STEP 2: Read watermark
# -----------------------------
try:
    response = s3.get_object(Bucket=METADATA_BUCKET, Key=WATERMARK_KEY)
    watermark_data = json.loads(response['Body'].read())
    last_processed = watermark_data['last_processed_time']
except ClientError as e:
    if e.response['Error']['Code'] == 'NoSuchKey':
        print("First run: no watermark found")
        last_processed = "1970-01-01T00:00:00Z"
    else:
        raise e

print(f"Last watermark: {last_processed}")

# -----------------------------
# STEP 3: List files
# -----------------------------
response = s3.list_objects_v2(
    Bucket=LANDING_BUCKET,
    Prefix=LANDING_PREFIX
)

new_files = []

# -----------------------------
# STEP 4: Detect new files safely
# -----------------------------
for obj in response.get('Contents', []):
    file_time = obj['LastModified'].isoformat()

    # CRITICAL CONDITION
    if last_processed < file_time <= job_start_time:
        file_path = f"s3://{LANDING_BUCKET}/{obj['Key']}"
        new_files.append(file_path)

# -----------------------------
# STEP 5: Prepare metadata
# -----------------------------
batch_id = f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
process_date = datetime.utcnow().strftime('%Y-%m-%d')

metadata = {
    "batch_id": batch_id,
    "process_date": process_date,
    "watermark_ts": job_start_time,
    "files": new_files
}

has_files = "true" if new_files else "false"

print(f"New files count: {len(new_files)}")

# -----------------------------
# STEP 6: Save metadata
# -----------------------------
metadata_key = f"{BATCH_PREFIX}/{batch_id}.json"

s3.put_object(
    Bucket=METADATA_BUCKET,
    Key=metadata_key,
    Body=json.dumps(metadata)
)

print(f"Metadata saved: s3://{METADATA_BUCKET}/{metadata_key}")

# -----------------------------
# STEP 7: Update watermark to cutoff
# -----------------------------
s3.put_object(
    Bucket=METADATA_BUCKET,
    Key=WATERMARK_KEY,
    Body=json.dumps({"last_processed_time": job_start_time})
)

print("Watermark updated safely")

# -----------------------------
# STEP 8: Pass parameters
# -----------------------------

metadata_path = f"s3://{METADATA_BUCKET}/{metadata_key}"
print("Metadata path:", metadata_path)
print(f"--has_files={has_files}")
glue.put_workflow_run_properties(
    Name=WORKFLOW_NAME,
    RunId=WORKFLOW_RUN_ID,
    RunProperties={
        "metadata_path": metadata_path
    }
)
