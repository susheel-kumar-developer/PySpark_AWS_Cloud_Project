from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

created_at = '2021-01-13T01:00:00Z'
schema = ["dt"]

df = spark.createDataFrame(data=created_at_data,schema=schema)
df.show()
