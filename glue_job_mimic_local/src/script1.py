import sys
import json
import os
from datetime import datetime

# -------------------------
# Load .env (local only)
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
    from botocore.exceptions import ClientError
except:
    boto3 = None


# =========================
# CORE LOGIC (UNIT TESTABLE)
# =========================
def get_new_files(files, last_processed, job_start_time):
    new_files = []

    for f in files:
        if last_processed < f["last_modified"] <= job_start_time:
            new_files.append(f["path"])

    return new_files


# =========================
# LOCAL STORAGE
# =========================
def list_local_files(folder):
    files = []

    if not os.path.exists(folder):
        return files

    for file in os.listdir(folder):
        full_path = os.path.join(folder, file)

        files.append({
            "path": full_path,
            "last_modified": datetime.utcfromtimestamp(
                os.path.getmtime(full_path)
            ).isoformat()
        })

    return files


def read_local_json(path):
    if not os.path.exists(path):
        return None

    with open(path, "r") as f:
        return json.load(f)


def write_local_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    with open(path, "w") as f:
        json.dump(data, f, indent=2)


# =========================
# S3 STORAGE
# =========================
def list_s3_files(s3, bucket, prefix):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    files = []
    for obj in response.get("Contents", []):
        files.append({
            "path": f"s3://{bucket}/{obj['Key']}",
            "last_modified": obj["LastModified"].isoformat()
        })

    return files


def read_s3_json(s3, bucket, key):
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(response["Body"].read())
    except:
        return None


def write_s3_json(s3, bucket, key, data):
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(data))


# =========================
# MAIN RUNNER
# =========================
def run(mode="local", config=None):
    job_start_time = datetime.utcnow().isoformat()

    # -----------------------
    # LOCAL MODE
    # -----------------------
    if mode == "local":
        landing = config["landing"]
        watermark_file = config["watermark"]
        batch_folder = config["batch"]

        watermark_data = read_local_json(watermark_file)

        last_processed = (
            watermark_data["last_processed_time"]
            if watermark_data else "1970-01-01T00:00:00"
        )

        files = list_local_files(landing)

        new_files = get_new_files(files, last_processed, job_start_time)

        batch_id = f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

        metadata = {
            "batch_id": batch_id,
            "files": new_files,
            "watermark_ts": job_start_time
        }

        metadata_path = f"{batch_folder}/{batch_id}.json"

        write_local_json(metadata_path, metadata)
        write_local_json(
            watermark_file,
            {"last_processed_time": job_start_time}
        )

        print("LOCAL RUN SUCCESS ✅")
        print("New files:", new_files)

    # -----------------------
    # AWS MODE
    # -----------------------
    elif mode == "aws":
        args = getResolvedOptions(sys.argv, [
            'LANDING_BUCKET',
            'LANDING_PREFIX',
            'METADATA_BUCKET',
            'WATERMARK_KEY',
            'BATCH_PREFIX'
        ])

        s3 = boto3.client("s3")

        watermark_data = read_s3_json(
            s3,
            args["METADATA_BUCKET"],
            args["WATERMARK_KEY"]
        )

        last_processed = (
            watermark_data["last_processed_time"]
            if watermark_data else "1970-01-01T00:00:00"
        )

        files = list_s3_files(
            s3,
            args["LANDING_BUCKET"],
            args["LANDING_PREFIX"]
        )

        new_files = get_new_files(files, last_processed, job_start_time)

        batch_id = f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

        metadata = {
            "batch_id": batch_id,
            "files": new_files,
            "watermark_ts": job_start_time
        }

        metadata_key = f"{args['BATCH_PREFIX']}/{batch_id}.json"

        write_s3_json(
            s3,
            args["METADATA_BUCKET"],
            metadata_key,
            metadata
        )

        write_s3_json(
            s3,
            args["METADATA_BUCKET"],
            args["WATERMARK_KEY"],
            {"last_processed_time": job_start_time}
        )

        print("AWS RUN SUCCESS 🚀")
        print("New files:", new_files)


# =========================
# ENTRY POINT
# =========================
def main():
    # Try Glue args first, fallback to .env
    try:
        args = getResolvedOptions(sys.argv, ['ENV'])
        ENV = args['ENV']
    except:
        ENV = os.getenv("ENV", "local")

    if ENV == "local":
        config = {
            "landing": os.getenv("LANDING_PATH"),
            "watermark": os.getenv("WATERMARK_PATH"),
            "batch": os.getenv("BATCH_PATH"),
        }
        run(mode="local", config=config)

    elif ENV == "prod":
        run(mode="aws")

    else:
        raise ValueError(f"Unknown ENV: {ENV}")


if __name__ == "__main__":
    main()