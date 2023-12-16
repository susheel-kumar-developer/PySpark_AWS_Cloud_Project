from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data = [(1, "A", 10), (1, "B", 20), (2, "A", 30), (2, "B", 40)]
schema = ["id","col1","col2"]
df = spark.createDataFrame(data=data,schema=schema)
df.show(truncate=False)

new_df = df.groupBy("id").pivot("col1").agg(first("col2"))

# new_df = df.groupBy("id").pivot("col1").agg(first("col1"))
new_df.show(truncate=False)

