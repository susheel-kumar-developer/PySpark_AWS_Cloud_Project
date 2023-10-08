from pyspark.sql import *
from pyspark.sql.functions import *
import os
import sys

spark = SparkSession.builder.master("local[2]").appName("test1").enableHiveSupport().getOrCreate()

host="jdbc:mysql://database-1.csqzkkzjx3z0.us-east-2.rds.amazonaws.com:3306/mysqldb?useSSL=false"
df=spark.read.format("jdbc").option("url",host).option("user","myuser").option("password","mypassword").option("driver","com.mysql.cj.jdbc.Driver").option("dbtable","asl").load()
ndf=df.distinct()

# hive table is not in local system so that why hivemetastore error will coming....
# ndf.write.mode("append").format("parquet").saveAsTable("asltab")
ndf.show()
