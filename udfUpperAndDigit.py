from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

slen = udf(lambda s: len(s), IntegerType())

@udf
def to_upper(s):
    if s is not None:
        return s.upper()

@udf(returnType=IntegerType())
def add_one(x):
    if x is not None:
        return x + 1


df = spark.createDataFrame([(1, "John Doe", 21)], ("id", "name", "age"))
df.select(slen("name").alias("slen(name)"), to_upper("name"), add_one("age")).show()

