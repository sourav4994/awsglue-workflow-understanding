# Write a query to find the top three customers by total revenue within each region.

# WITH customer_revenue AS (
#     SELECT
#         region,
#         customer_name,
#         SUM(revenue) AS total_revenue
#     FROM sales
#     GROUP BY region, customer_name
# ),

# ranked_customers AS (
#     SELECT
#         region,
#         customer_name,
#         total_revenue,
#         ROW_NUMBER() OVER (
#             PARTITION BY region
#             ORDER BY total_revenue DESC
#         ) AS rn
#     FROM customer_revenue
# )

# SELECT
#     region,
#     customer_name,
#     total_revenue
# FROM ranked_customers
# WHERE rn <= 3;
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("app").getOrCreate()

# Hardcoded data
data = [
    (1, 101, "Alice",   "North", 500),
    (2, 102, "Bob",     "North", 700),
    (3, 103, "Charlie", "North", 400),
    (4, 101, "Alice",   "North", 300),
    (5, 104, "David",   "North", 900),

    (6, 201, "Eva",     "South", 1000),
    (7, 202, "Frank",   "South", 600),
    (8, 203, "Grace",   "South", 750),
    (9, 201, "Eva",     "South", 200),
    (10,204, "Helen",   "South", 500),

    (11,301, "Ivy",     "West", 1200),
    (12,302, "Jack",    "West", 400),
    (13,303, "Ken",     "West", 950),
    (14,304, "Leo",     "West", 300),
    (15,301, "Ivy",     "West", 100)
]

# Schema
schema = T.StructType([
    T.StructField("order_id", T.IntegerType(), True),
    T.StructField("customer_id", T.IntegerType(), True),
    T.StructField("customer_name", T.StringType(), True),
    T.StructField("region", T.StringType(), True),
    T.StructField("revenue", T.IntegerType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Step 1: total revenue per customer within region
customer_revenue = df.groupBy(
    "region",
    "customer_name"
).agg(
    F.sum("revenue").alias("total_revenue")
)

# Step 2: window specification
window_spec = Window.partitionBy("region") \
                    .orderBy(F.col("total_revenue").desc())

# Step 3: ranking
ranked_df = customer_revenue.withColumn(
    "rn",
    F.row_number().over(window_spec)
)

# Step 4: top 3 customers per region
result = ranked_df.filter(
    F.col("rn") <= 3
)

result.show()