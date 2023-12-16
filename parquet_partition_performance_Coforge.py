from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark  import SparkContext, SparkConf
import os, shutil

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
# print(spark.sparkContext.getConf().getAll())
# conf = (SparkConf().setMaster("yarn"))
# sc = SparkContext(conf=conf)
# print(sc._jsc.hadoopConfiguration())

# data = "C:\\bigdata\\drivers\\utlc_movies_1GB.csv"
data = "C:\\bigdata\\drivers\\asl.csv"

df = spark.read.format("csv").option("header",True).option("InferSchema",True).load(data)
# by default parquet file size 128MB if we 1GB file so iit will divide 1GB= 1024MB/128MB = approximate 8 partition
df.write.mode("overwrite").parquet("a.parquet")
# df.write.mode("overwrite").partitionBy("rating").parquet("b.parquet")
# df.write.parquet("s3a://<folder_name>/<file_name>/file.parquet")
# df.write.json("b.json")
# df.write.orc('a.orc')
# dfread = spark.read.parquet("b.parquet")
# avora_df= spark.read.
# dfread.createOrReplaceTempView("parquetTbl")
# spark.sql("select * from parquetTbl limit 5").show(truncate=False)

# shutil.rmtree('a.parquet')
# print(df.rdd.getNumPartitions())
# df.show(5,truncate=False)
# print(size(data))
size = os.path.getsize(data)/1000000000
# print(size)

