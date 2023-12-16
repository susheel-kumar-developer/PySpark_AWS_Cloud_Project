from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()


log_data = [ Row(timestamp="2023-09-11 12:00:00", user_id=1), Row(timestamp="2023-09-11 13:30:00", user_id=2), Row(timestamp="2023-09-11 14:45:00", user_id=1), Row(timestamp="2023-09-11 16:15:00", user_id=3)]

custom_schema = StructType([StructField("timestamp",TimestampType(),True), StructField("user_id",IntegerType(),True)]
    )

df = spark.createDataFrame(data=log_data, schema=custom_schema)
df = df.withColumn("timestamp_field", to_timestamp(df["timestamp_field"], 'yyyy-MM-dd HH:mm:ss'))
df.printSchema()
