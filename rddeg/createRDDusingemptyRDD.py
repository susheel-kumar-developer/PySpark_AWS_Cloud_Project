from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

data ="C:\\bigdata\\drivers\\emailsmay4.txt"

rdd = spark.sparkContext.emptyRDD()
rdd1 = spark.sparkContext.textFile(data)
print(rdd1.getNumPartitions())