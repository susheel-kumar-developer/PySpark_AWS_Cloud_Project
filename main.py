from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

data1 ="C:\\bigdata\\drivers\\complexxmldata.xml"

data2 = "C:\\bigdata\\drivers\\asl.csv"

df1 = spark.read.format("xml").option("rowTag","catalog_item").option("path",data1).load()

df1.createOrReplaceTempView("tab1")

res = spark.sql("select tab1._gender from tab1")

df2 = spark.read.format("csv").option("header","true").option("path",data2).load()

df2.createOrReplaceTempView("tab2")

res2 = spark.sql("select * from tab2")

res2.show()



