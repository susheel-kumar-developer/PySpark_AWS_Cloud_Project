from pyspark.sql import *
from pyspark.sql.functions import *
import regex

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()


data = [("Mnnit",1990,12),("Allahabd",1550,16),("Meerut",1200,19),("Delhi",1200,19),("NY",1300,20)]

schema = ["Name","Year","KM"]
df = spark.createDataFrame(data=data, schema=schema)
print("Showing actual result",str(df.count()))
df.show(truncate=False)
# df[df["KM"].contains("16") | df["KM"].contains("19")].show()
# df.select("KM").show(truncate=False)
# df.select("KM")

distinct_df = df.distinct()
print("Showing result after distinct rows", str(distinct_df.count()))
distinct_df.show(truncate=False)

dropDublicate_df = df.dropDuplicates(["Year","KM"])

print("Showing result after droping dublicates", str(dropDublicate_df.count()))
dropDublicate_df.show(truncate=False)