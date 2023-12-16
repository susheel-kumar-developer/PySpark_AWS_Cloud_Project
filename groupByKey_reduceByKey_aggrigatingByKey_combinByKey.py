from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark import SparkContext

# spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

sc = SparkContext("local","GroupByKey")

data = [("abc",1),("abc",1),("bcd",2),("bcd",3)]

rdd = sc.parallelize(data)


# d = rdd.distinct().collect()
# print("Distinct values",d)

result_groupByKey = rdd.groupByKey().collect()
for k,v in result_groupByKey:
    print(f"groupByKey, {k}:{list(v)}")

result_reduceByKey = rdd.reduceByKey(lambda x,y:x+y).collect()

for k,v in result_reduceByKey:
    print(f"reduceByKey->,{k}:{v}")

result_aggregateByKey = rdd.aggregateByKey(0, lambda x,y:x+y, lambda x,y:x+y).collect()

for k,v in result_aggregateByKey:
    print(f"aggregateByKey->{k}:{v}")


