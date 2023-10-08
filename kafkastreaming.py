from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
df= spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "may25") \
  .option("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")\
  .option("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") \
  .load()


ndf1=df.selectExpr("CAST(value AS STRING)")
#sch =  StructType().add("name", "string").add("email", "string").add("city","string")
sch = StructType([
    StructField("Name", StringType(), nullable=True),
    StructField("Email", StringType(), nullable=True),
    StructField("City", StringType(), nullable=True)
])
#ndf = ndf1.select(from_json("value", sch).alias("parsed_value")).select("parsed_value.*")
ndf=df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(get_json_object("json_str", "$"), sch).alias("parsed_value")) \
    .select("parsed_value.*")

ndf.printSchema()
#ndf.printSchema()
ndf.writeStream.outputMode("append").format("console").start().awaitTermination()