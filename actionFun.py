from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

col1=["seqno","name"]

data=[(1,'sus'),(2,'ku'),(3,'maa')]

df=spark.createDataFrame(data=data,schema=col1)
# df.printSchema()
def fun(df):
    print("Seqno:",df.seqno,"Name:", df.name)

df.foreach(fun)