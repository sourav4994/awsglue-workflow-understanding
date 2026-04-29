from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("data/output")

df.show(truncate=False)
df.printSchema()