from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data="C:\\bigdata\\drivers\\donations.csv"
ardd = spark.sparkContext.textFile(data)
fst = ardd.first()
pro = ardd.filter(lambda x:x!=fst).map(lambda x:x.split(",")).map(lambda x:(x[0],int(x[2]))).reduceByKey(lambda x,y:x+y)
#reduceByKey working on only key,value format data only

for x in pro.take(9):
    print(x)