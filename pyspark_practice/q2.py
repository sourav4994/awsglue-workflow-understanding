data = [
    ("A", "X", 75),
    ("A", "Y", 75),
    ("A", "Z", 80),
    ("B", "X", 90),
    ("B", "Y", 91),
    ("B", "Z", 75)
]

colums=['sname','sub','marks']


# Find the total marks of the top 2 subjects for each student based on their marks

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('appname').getOrCreate()
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
schema=StructType([StructField('sname',StringType()),StructField('sub',StringType()),StructField('marks',IntegerType())])

df=spark.createDataFrame(data,schema)
df.show()
from pyspark.sql.functions import col,row_number
from pyspark.sql.window import Window
window_spec = Window.partitionBy("sname").orderBy(col("marks").desc())
df_ranked = df.withColumn("rank", row_number().over(window_spec))

df_filter=df_ranked.filter(col('rank')<=2)
df_filter.show()
from pyspark.sql.functions import sum

result = df_filter.groupBy("sname").agg(
    sum("marks").alias("total_marks")
)
result.show()