# You are given two tables, Table1 and Table2, with the following data:
# Table1:
# Question:
# Using the above tables, identify the number of records returned for the following SQL joins:
# 1.
# Inner Join
# 2.
# Left Join
# 3.
# Right Join
# 4.
# Full Outer Join
# 5.
# Cross Join


table1 = [(1,), (1,), (2,), (None,), (None,)]
table2 = [(1,), (3,), (None,)]

# col2='id'

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType ,StructField ,IntegerType
schema=StructType([StructField('id',IntegerType(),True)])
spark=SparkSession.builder.appName('firstApp').getOrCreate()
df1=spark.createDataFrame(table1,schema)
df2=spark.createDataFrame(table2,schema)

joined_df=df1.join(df2,df1['id']==df2['id'],'inner')
df1.show()
joined_df.show()