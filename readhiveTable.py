from pyspark.sql import *
from pyspark.sql.functions import *
import os
import sys

spark = SparkSession.builder.master("local[2]").appName("test1").enableHiveSupport().getOrCreate()

host="jdbc:mysql://mysqldb.cwkqaojgxfrd.ap-south-1.rds.amazonaws.com:3306/newdb?useSSL=false"
df=spark.read.format("jdbc").option("url",host).option("user","myuser").option("password","mypassword").option("driver","com.mysql.cj.jdbc.Driver").option("dbtable","asl").load()
ndf=df.distinct()
# ndf.write.mode("append").format("parquet").saveAsTable("asltab1")
ndf.show()
