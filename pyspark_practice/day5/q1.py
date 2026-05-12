# Write an SQL query to swap all 'f' and 'm' values in the gender column of the users table. Change all 'f' values to 'm' and all 'm' values to 'f', without using any intermediate temporary tables.

[(1, 'f'),
 (2, 'm'),
 (3, 'f'),
 (4, 'm'),
 (5, 'm')]

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import when, col

spark = SparkSession.builder.appName('SwapGender').getOrCreate()

# Sample data
data = [(1, 'f'),
 (2, 'm'),
 (3, 'f'),
 (4, 'm'),
 (5, 'm')]

spark_df = spark.createDataFrame(data, schema=StructType([
    StructField('id', IntegerType(), True),     
    StructField('gender', StringType(), True)
]))

spark_df.withColumn('swapped_gender',when(col('gender')=='f','m').otherwise('f')).select(col('id'),col('swapped_gender')).show()