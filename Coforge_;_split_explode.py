from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

data = "C:\\bigdata\\drivers\\Coforge.csv"

df = spark.read.format("csv").option("header",True).load(data)

# Without rdd we can not see use .getNumPartition() because some of transformation depend on rdd not on dataframe
# print(df.rdd.getNumPartitions())

# df below
# id|value
# 1|Name:Prashant;salary:1344443;role:DE
# 2|Name:Shrishti;age:27;org:Facebook;city:Banglore
# output below
# id,key,value
# 1,Name,Prashant
# 1,salary,1344443
# 1,role,DE
# 2,Name,Shrishti
# 2,age,27
# 2,org,Facebook
# 2,city,Banglore

# df.withColumn("value",split(df["value"],';')).select("id",explode("value")).show(truncate=False)

# df.show()