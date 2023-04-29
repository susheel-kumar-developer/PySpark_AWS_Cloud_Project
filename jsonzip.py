from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
#data="s3://susheeldb/zip/zips.json"
data="C:\\bigdata\\drivers\\zips.json"
#op="s3://susheeldb/output/zipresult"
df=spark.read.format("json").option("mode","DROPMALFORMED").load(data)
df=df.withColumnRenamed("_id","id").withColumn("lang",col("loc")[0]).withColumn("lati",col("loc")[1])
df.show()
#df.write.format("csv").option("header","true").save(op)