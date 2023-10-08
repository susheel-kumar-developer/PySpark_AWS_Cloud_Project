from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()



data="C:\\bigdata\\drivers\\zips.json"
df = spark.read.option("json").option("multiline","true").load(data)

data2="C:\\bigdata\\drivers\\zips_write.json"
df.write.mode("overwrite").json(data2)
df1=spark.read.format("json").option("multiline","true").load(data2)

print(df1.collect())
#
# [Row(_id='01001', city='AGAWAM', loc=[-72.622739, 42.070206], pop=15338, state='MA')]
# df.show(truncate=False)
# df.printSchema()
# print(df.collect())

array_items = df.collect()
# print(array_items)
# print("Count of records:", str(df.count()))
# print("Take first row:", type(df.take(1)))
# print("First array element", type(df.first()))
