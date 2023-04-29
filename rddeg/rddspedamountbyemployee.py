from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

data = "C:\\bigdata\\drivers\\emp_sal_expensives.csv"

rddfile = spark.sparkContext.textFile(data)

skip = rddfile.first()
data = rddfile.filter(lambda x:x!=skip).map(lambda x:x.split(",")).map(lambda x:((x[0],x[1]),int(x[2]))).reduceByKey(lambda x,y:(x+y))

for key,value in data.collect():
    print(key,value)