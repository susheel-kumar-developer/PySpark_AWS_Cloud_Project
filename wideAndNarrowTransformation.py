from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

# Example of Narrow Transformation: map, filter, flatMap, union, coalesce, repartition (without shuffling).
# Example of Wide Transformation: groupByKey, reduceByKey, sortByKey, join, cogroup, distinct, repartition (with shuffling).

columns = ["Name","Age"]

rdd = spark.sparkContext.parallelize(columns)



# for element in rdd2.collect():
#     print(element)
data = [("harry",10),("jhon",20)]
schema = ["Name", "Age"]
df = spark.createDataFrame(data=data, schema=schema)
df.show()
# df1 = df.rdd.map(lambda x:(x[1]*2)).toDF()

# emptyRdd = SparkContext.emptyRDD(self)
# print("is Empty rdd:->",str(emptyRdd.isEmpty()))

# df1.show()

