# 7.Scenario: You are given a People table that contains records of individuals with an id and number_of_people. Write an SQL query to display the records where there are three or more consecutive rows with ids and the number_of_people is greater than or equal to 100 for each of those rows.

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number,count
spark=SparkSession.builder.appName('ConsecutiveRows').getOrCreate()

# Sample data
data=[
    (1, 150),
    (2, 120),
    (3, 110),
    (4, 90),
    (5, 200),
    (6, 130),
    (7, 120),
    (8, 140),
    (9, 100)
]

columns=['id', 'number_of_people']
schema=StructType([
    StructField('id', IntegerType(), True),
    StructField('number_of_people', IntegerType(), True)
])

df=spark.createDataFrame(data, schema)


cte_df=df.filter(col('number_of_people')>=100).withColumn('grp_assign',col('id') - row_number().over(Window.orderBy('id'))).filter(col('number_of_people')>=100)

cte_df.show()
unique_grp=cte_df.groupBy('grp_assign').agg(count(col('id')>=3).alias('unique_grp_num'))

cte_df.join(unique_grp,cte_df['grp_assign']==unique_grp['grp_assign']).filter(col('unique_grp_num')>=3).select('id','number_of_people').show()
