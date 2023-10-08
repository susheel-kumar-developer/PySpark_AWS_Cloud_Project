from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# Converting function to UDF
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()


columns = ["Seqno","Name"]
data = [("1", "john jones"),
    ("2", "tracey smith"),
    ("3", "amy sanders")]

df = spark.createDataFrame(data=data,schema=columns)

df.show(truncate=False)

def convertCase(str):
    resStr=""
    arr = str.split(" ")
    for x in arr:
       resStr= resStr + x[0:1].upper() + x[1:len(x)] + " "
    return resStr

convertUDF = udf(lambda z: convertCase(z),StringType())

# Converting function to UDF
# StringType() is by default hence not required
convertUDF = udf(lambda z: convertCase(z))


df.select(col("Seqno"), \
    convertUDF(col("Name")).alias("Name") ) \
   .show(truncate=False)

def upperCase(str):
    return str.upper()

upperCaseUDF = udf(lambda z:upperCase(z),StringType())

df.withColumn("Cureated Name", upperCaseUDF(col("Name"))) \
  .show(truncate=False)


""" Using UDF on SQL """
spark.udf.register("convertUDF", convertCase,StringType())
df.createOrReplaceTempView("NAME_TABLE")
spark.sql("select Seqno, convertUDF(Name) as Name from NAME_TABLE") \
     .show(truncate=False)


@udf(returnType=StringType())
def upperCase(str):
    return str.upper()

df.withColumn("Cureated Name", upperCase(col("Name"))) \
.show(truncate=False)
