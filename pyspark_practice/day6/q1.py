# You have a Transaction_Tbl table with the following structure:
# Your task is to display all columns from this table, along with the maximum transaction amount (MaxTranAmt) for each customer (CustID) and the ratio of each transaction amount (TranAmt) to the maximum transaction amount for that customer

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F  
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType

data = [
    (1001, 20001, 10000, "2020-04-25"),
    (1001, 20002, 15000, "2020-04-25"),
    (1001, 20003, 80000, "2020-04-25"),
    (1001, 20004, 20000, "2020-04-25"),
    (1002, 30001, 7000,  "2020-04-25"),
    (1002, 30002, 15000, "2020-04-25"),
    (1002, 30003, 22000, "2020-04-25")
]

# 2. Schema using StructType and StructField
schema = StructType([
    StructField("CustID", IntegerType(), True),
    StructField("TranID", LongType(), True),
    StructField("TranAmt", IntegerType(), True),
    StructField("TranDate", StringType(), True)
])


# 3. Create SparkSession
spark = SparkSession.builder.appName("TransactionAnalysis").getOrCreate()
# 4. Create DataFrame
df = spark.createDataFrame(data, schema)
# 5. Calculate MaxTranAmt and Ratio
result_df = df.withColumn("MaxTranAmt", F.max("TranAmt").over(Window.partitionBy("CustID"))) \
              .withColumn("Ratio", F.col("TranAmt") / F.col("MaxTranAmt"))
# 6. Show the result    
result_df.show()

