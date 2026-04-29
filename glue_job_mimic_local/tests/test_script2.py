import json
from src.script2 import transform, read_local_metadata, read_s3_metadata

# import os
# os.environ["PYSPARK_PYTHON"] = "python"
# import findspark
# findspark.init()
# =====================================
# TEST 1: transform() core logic
# =====================================
def test_transform_basic(spark):
    data = [
        ("TXN1001", "C101", "2500.75", "2026-04-28T09:15:00Z", "INDIA", "P100", "2", "SUCCESS"),
    ]

    columns = [
        "transaction_id", "custId", "txnAmt", "transaction_timestamp",
        "country", "product_id", "quantity", "status"
    ]

    df = spark.createDataFrame(data, columns)

    result = transform(df, "batch_1", "2026-04-28")

    # ✅ Column rename
    assert "customer_id" in result.columns
    assert "transaction_amount" in result.columns

    # ✅ Type conversion
    dtypes = dict(result.dtypes)
    assert dtypes["transaction_amount"] == "double"
    assert dtypes["quantity"] == "int"

    # ✅ Metadata columns
    assert "batch_id" in result.columns
    assert "process_date" in result.columns

    row = result.collect()[0]

    assert row["customer_id"] == "C101"
    assert row["transaction_amount"] == 2500.75
    assert row["batch_id"] == "batch_1"


# =====================================
# TEST 2: transform() empty input
# =====================================
def test_transform_empty(spark):
    df = spark.createDataFrame([], """
        transaction_id STRING,
        custId STRING,
        txnAmt STRING,
        transaction_timestamp STRING,
        country STRING,
        product_id STRING,
        quantity STRING,
        status STRING
    """)

    result = transform(df, "batch_1", "2026-04-28")

    assert result.count() == 0


# =====================================
# TEST 3: read_local_metadata
# =====================================
def test_read_local_metadata(tmp_path):
    file = tmp_path / "meta.json"

    data = {
        "files": ["file1.csv"],
        "batch_id": "b1",
        "process_date": "2026-04-28"
    }

    file.write_text(json.dumps(data))

    result = read_local_metadata(str(file))

    assert result == data


# =====================================
# TEST 4: read_s3_metadata (mock)
# =====================================
def test_read_s3_metadata():
    class MockS3:
        def get_object(self, Bucket, Key):
            return {
                "Body": MockBody()
            }

    class MockBody:
        def read(self):
            return json.dumps({
                "files": ["file1.csv"],
                "batch_id": "b1",
                "process_date": "2026-04-28"
            }).encode()

    s3 = MockS3()

    metadata_path = "s3://bucket/path/file.json"

    result = read_s3_metadata(s3, metadata_path)

    assert result["batch_id"] == "b1"