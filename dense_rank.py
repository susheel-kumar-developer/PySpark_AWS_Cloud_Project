from pyspark.sql import Window, types, SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()


df=spark.createDataFrame([1,2,3,3,2,1,4],types.IntegerType())
w=Window.orderBy("Value")

df.withColumn("Dense",dense_rank().over(w))
df.select("Value").show()

