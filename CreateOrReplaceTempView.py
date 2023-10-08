from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()




data1 =[("archan",35,"Joya"),("Susheel",32,"Amroha")]
df1 = spark.createDataFrame(data1,schema=["name","age","location"])


data = [("Susheel","Amroha",29),("Bro","delhi",22)]
df = spark.createDataFrame(data, schema=["Name","Location","Age"])

# print(df.rdd.getNumPartitions())
# print(spark.getActiveSession())

df.createOrReplaceTempView("temp")
res = spark.sql("select * from temp")
res.show()

df1.createOrReplaceTempView("secontab")
res1 = spark.sql("select * from secondtab")
res1.show()


