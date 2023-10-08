from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructField,IntegerType,StringType

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

data=[(1234,"Gajraula")]

customStruct=StructType([StructField("id",IntegerType(),True),StructField("name",StringType(),True)])

df = spark.createDataFrame(data=data,schema=customStruct)

df.printSchema()
df.show()
