# Given a table with sales data, write a query to find consecutive days with decreasing revenue

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('app').getOrCreate()

data = [
    ("2026-05-01", 1000),
    ("2026-05-02", 900),
    ("2026-05-03", 850),
    ("2026-05-04", 950),
    ("2026-05-05", 800),
    ("2026-05-06", 700),
    ("2026-05-07", 750)
]

schema = T.StructType([
    T.StructField('date', T.StringType()),
    T.StructField('price', T.IntegerType())
])

df = spark.createDataFrame(data, schema)

df = df.withColumn(
    "date",
    F.to_date("date", "yyyy-MM-dd")
)

window_spec = Window.orderBy(F.col('date').asc())

df = df.withColumn(
    "prev_price",
    F.lag("price").over(window_spec)
)

filter_df = df.filter(
    F.col('price') < F.col('prev_price')
)

filter_df.show()


# -- CREATE TABLE sales (
# --     sale_date DATE,
# --     revenue INT
# -- );

# -- INSERT INTO sales (sale_date, revenue)
# -- VALUES
# -- ('2026-05-01', 1000),
# -- ('2026-05-02', 900),
# -- ('2026-05-03', 850),
# -- ('2026-05-04', 950),
# -- ('2026-05-05', 800),
# -- ('2026-05-06', 700),
# -- ('2026-05-07', 750);


# -- with cte as (
# -- select *,lag(revenue) over(order by sale_date asc) as prev_day_revenu from sales)
# -- select * from cte where prev_day_revenu>revenue;