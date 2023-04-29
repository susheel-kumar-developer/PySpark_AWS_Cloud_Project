from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
#snowflake little expensive especially when u r processing data at that time it's too expensive.
url=""

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
sfOptions = {
  "sfURL" : "klingeq-yn24889.snowflakecomputing.com",
  "sfUser" : "SKMJMCA",
  "sfPassword" : "9580SUsh!",
  "sfDatabase" : "susheeldb",
  "sfSchema" : "public",
  "sfWarehouse" : "SMALL_CLUSTER"
}
#ndf=spark.read.format(SNOWFLAKE_SOURCE_NAME).option("sfURL","")
# df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
#   .options(**sfOptions) \
#   .option("dbtable", "nep") \
#   .load()
# df.show()
data="C:\\bigdata\\drivers\\bank-full.csv"
adf=spark.read.format("csv").option("header","true").option("sep",";").option("inferSchema","true").load(data)
adf.show()
adf.write.mode("append").format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable","tempbank").save()

#https://mvnrepository.com/artifact/net.snowflake/snowflake-ingest-sdk/1.1.0
#https://mvnrepository.com/artifact/net.snowflake/spark-snowflake_2.12/2.11.1-spark_3.1
#https://mvnrepository.com/artifact/net.snowflake/snowflake-jdbc/3.13.26