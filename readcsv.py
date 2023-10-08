from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()\

data="C:\\bigdata\\drivers\\nullval.csv"

df = spark.read.format("csv").option("header","true").load(data)

# df.show(truncate=False)

df.na.drop(how="any").show(truncate=False)

# ndf.show(truncate=False)
