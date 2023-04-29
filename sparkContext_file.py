from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

print(spark.sparkContext)
print(spark.version)
print(spark.sparkContext.version)
