from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

csvData = "C:\\bigdata\\drivers\\asl.csv"

df = spark.read.format("csv").option("header",True).option("inferSchema",True).load(csvData)

df1 = df.where(col("Age") > 34).cache()

df1.show(truncate=False)
