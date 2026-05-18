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
# 1. Register the DataFrame as a SQL table
df.createOrReplaceTempView("Emp")

# 2. Run the exact same SQL code inside spark.sql()
spark_sql_result = spark.sql("""
    WITH RankedSequences AS (
        SELECT 
            `Group`,
            Sequence,
            ROW_NUMBER() OVER(PARTITION BY `Group` ORDER BY Sequence) AS row_num
        FROM Emp
    ),
    IslandGroups AS (
        SELECT 
            `Group`,
            Sequence,
            (Sequence - row_num) AS island_id
        FROM RankedSequences
    )
    SELECT 
        `Group`,
        MIN(Sequence) AS Min_Sequence,
        MAX(Sequence) AS Max_Sequence
    FROM IslandGroups
    GROUP BY `Group`, island_id
    ORDER BY `Group`, Min_Sequence
""")

spark_sql_result.show()