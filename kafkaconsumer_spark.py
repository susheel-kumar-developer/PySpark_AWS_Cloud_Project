from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
df= spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "indpak") \
  .load()
df.printSchema()

ndf1=df.selectExpr("CAST(value AS STRING)").select(split(col("value"), " ").alias("lg"))

ndf = ndf1.select(col("lg").getItem(0).alias("ip"), col("lg").getItem(3).alias("date"),
                 col("lg").getItem(5).alias("req"),col("lg").getItem(9).alias("ref"))

ndf.printSchema()
#ndf.writeStream.outputMode("append").format("console").start().awaitTermination()

def foreach_batch_function(df):
    cdf = df.withColumn("ip", regexp_replace(col("ip"),"\"",""))
    cdf.write.mode('append').format('jdbc') \
            .option("url", "jdbc:mysql://mysqldb.cwkqaojgxfrd.ap-south-1.rds.amazonaws.com:3306/newdb?useSSL=false") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", "kafkaSparkLivedata") \
            .option("user", "myuser") \
            .option("password", "mypassword") \
            .save()
    pass


ndf.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()