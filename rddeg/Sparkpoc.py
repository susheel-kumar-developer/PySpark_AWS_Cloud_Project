from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
#data="C:\\bigdata\\drivers\\asl.csv"
#ways to create rdd ... 2 ways ...
#sparkContext only create rdd ....
sc = spark.sparkContext
#there are 2 ways to create rdd, first approach sc.parallaelize ...
#parallelize .... take java/scala/python elements convert to rdd ..
arr = [1,2,3,4,5,9,12,32,91,12]
#arr is java object/python object ... convert to rdd
nrdd = sc.parallelize(arr)
#map is spark transformation ... apply a logic/function on top of each & every element ...
#map how many input elmeents u have same number of output elements u ll get

#filter is one spark transformation .. apply a logic on top of all elemebts based on bool value .
# or filter .... based on bool value filter the results.. it's same like select * from tab where ....

res=nrdd.map(lambda x:x*x).filter(lambda x:x%2==0)
for x in res.collect():
    print(x)


