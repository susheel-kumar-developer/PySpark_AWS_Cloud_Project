from pyspark.sql import *
from pyspark.sql.functions import *
import pandas as pd


spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
#here we are using local file system
data ="C:\\bigdata\\drivers\\emailsmay4.txt"

#will use hadoop file system
#data = "hdfs://localhost:9000/data/asl.csv"

spark.sparkContext.setLogLevel("ERROR")

sc = spark.sparkContext

rddc = sc.textFile(data)

print("Get num partition as per my laptop core->",rddc.getNumPartitions())

res = rddc.filter(lambda x:"@" in x).map(lambda x:x.split(" ")).map(lambda x:(x[0],x[-1]))
# take() is action to display all number of records
#collect() will display all records but not recommended
#if data is structure format like name and email convert to that to rdd to run sql query

cols = ["name","email"]
df = res.toDF(cols)

df1 = res.toDF(cols).toPandas().to_csv("output",header=True)


#Add columns in dataframe using .withColumn
res = df.withColumn("age",lit("test"))\
    .withColumn("username",substring_index(col("email"),"@",1))\
    .withColumn("email",substring_index(col("email"),"@",-1))\
    .groupBy(col("email")).count()
#Datafram will show .... if dataframe variable have more than 20 character so that why we are using truncate=False
res.show(truncate=False)
# for x in res.take(9):
#     print(x)

print("get num partition->",df.rdd.getNumPartitions())

