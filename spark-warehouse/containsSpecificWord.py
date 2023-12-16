from pyspark.sql import *
from pyspark.sql.functions import *
import regex

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()


data = [("Mnnit",1990,12),("Allahabd",1550,16),("Meerut",1200,19),("Delhi",1200,19),("NY",1300,20)]

schema = ["Name","Year","KM"]
df = spark.createDataFrame(data=data, schema=schema)
df.withColumn("Comments",lit("Noida like Summer"))\
    .withColumn(col("Comments"),lit("Banglore"))\
    .withColumn(col("Comments"),lit("Chennai like winter")).show()
# print("Showing actual result",str(df.count()))
# df.show(truncate=False)
# df[df["KM"].contains("6") | df["KM"].contains("19")].show()




