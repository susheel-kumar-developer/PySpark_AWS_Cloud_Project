from pyspark.sql import *
from pyspark.sql.functions import *

from timeit import default_timer as timer

import time

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

data1 ="C:\\bigdata\\drivers\\complexxmldata.xml"

data2 = "C:\\bigdata\\drivers\\asl.csv"

df1 = spark.read.format("xml").option("rowTag","catalog_item").option("path",data1).load()

df1.createOrReplaceTempView("tab1")

start_time = time.time()
res = spark.sql("select tab1._gender from tab1")
end_time = time.time()

print("Total time use by above query :{}".format(end_time-start_time))

df2 = spark.read.format("csv").option("header","true").option("columnNameOfCorruptRecord","wrong").option("inferSchema","true").load(data2)

df2.show()

df2.createOrReplaceTempView("tab2")

# df2.explain(mode="formatted")

res2 = spark.sql("select * from tab2")

# res2.show()



