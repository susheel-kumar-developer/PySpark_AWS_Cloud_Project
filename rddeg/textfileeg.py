from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data="C:\\bigdata\\drivers\\asl.csv"
sc=spark.sparkContext
#second way to create rdd ... sc.textFile... textFile internally use hadoop stream api ... read any external data (local system, hdfs system or s3, or ...)
#textFile ...convert external data convert to rdd ..
srdd = sc.textFile(data)
head = srdd.first()
pro = srdd.filter(lambda x:x!=head).map(lambda x:x.split(",")).filter(lambda x:int(x[1])>50)

for x in pro.take(9):
    print(x)