from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

data="C:\\bigdata\\drivers\\donations.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
#current_dated
# df_date = df.withColumn()
# df_date.show()
df=df.withColumn("dt",to_date(col("dt"),"d-M-yyyy"))
# df_date = df.withColumn("dt",year("dt"))
# df_date = df.withColumn("dt",month("dt"))
df_date = df.withColumn("dt",dayofmonth("dt"))
df_date.show()

new_df = spark.createDataFrame([('1990-01-20 10:30:00',)],['new_date'])
new_df = new_df.withColumn("new_date",to_date(col("new_date"),"d-M-yyyy"))
# new_df.collect()

# df = spark.createDataFrame([('1997-02-28 10:30:00',)], ['t'])
df = spark.createDataFrame([('1997-02-28 10:30:00',)], ['t'])

# df=df.withColumn()
#based on input data u hae to mention format like dd-mm-yyyy or dd/m/yy
#i want whats the last day of the month (let eg: u have  13-01-2020 ... in jan u have 31 days, but 14-02-2022 last day 28-02 ... last day feb 28 only
# df.show()
# df.printSchema()
# ndf=df.withColumn("today", current_date()).withColumn("ts",current_timestamp()).withColumn("tm", split(col("ts")," ")[1]).withColumn("dtdiff",datediff(col("today"),col("dt"))).withColumn("diffdate",col("today")-col("dt"))\
#     .withColumn("lastday",last_day(col("dt")))
# ndf.show(truncate=False)
#by default spark consider as if any data yyyy-MM-dd format than only it's called date... the spark cunderstand only yyyy-MM-dd format
#but this original data u have "venu,10-01-2021,1000" mens dd-MM-yyyy so spark unable to understand so convert dd-MM-yyyy to spark understandabe format
#thats y ur using to_ddate() funciton.... this function convert string, or timestimstamp to spark understandable format  means yyyy-dd-MM

# create_at =