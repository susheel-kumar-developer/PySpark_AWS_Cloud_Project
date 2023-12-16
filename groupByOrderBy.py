from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

sampleData = [("david", 1000, "NY"),("daved", 5000, "NY"),("Hary", 1800, "NY"),("hari",2000,"NY"),("Jon",4000,"CF")]


Schema = ["name", "salary", "state"]


df =spark.createDataFrame(data=sampleData,schema=Schema)

df.groupBy("state").agg(sum("salary")).alias("sumOfSalary").show()

# df.show(truncate=False)


