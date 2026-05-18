# 1. Define the Window Specification for the Row Number


from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("GapsAndIslands").getOrCreate()

# Matching the image data exactly
data = [
    ("A", 1), ("A", 2), ("A", 3), ("A", 5), ("A", 6), ("A", 8), ("A", 9),
    ("B", 11),
    ("C", 1), ("C", 2), ("C", 3)
]
columns = ["Group", "Sequence"]
df = spark.createDataFrame(data, columns)
windowSpec = Window.partitionBy("Group").orderBy("Sequence")

# 2. Generate row number and calculate the island_id difference
df_with_islands = df.withColumn("row_num", F.row_number().over(windowSpec)) \
                    .withColumn("island_id", F.col("Sequence") - F.col("row_num"))

# 3. Group by multiple columns to find Min and Max
final_result = df_with_islands.groupBy("Group", "island_id").agg(
    F.min("Sequence").alias("Min_Sequence"),
    F.max("Sequence").alias("Max_Sequence")
).drop("island_id") \
 .orderBy("Group", "Min_Sequence") # Clean up and sort

final_result.show()