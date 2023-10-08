from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
# data="C:\\bigdata\\drivers\\donations.csv"
data="C:\\bigdata\\drivers\\donations.csv"

ardd = spark.sparkContext.textFile(data)

skip = ardd.first()
cols = skip.split(",")
print(skip)
#df = ardd.filter(lambda x:x!=fst).map(lambda x:x.split(",")).toDF(["name","dt","cty"])
df = ardd.filter(lambda x:x!=skip).map(lambda x:x.split(",")).toDF(cols)
#df.show()
df.createOrReplaceTempView("tab")
res = spark.sql("select name, sum(amount) cnt from tab group by name order by cnt desc").withColumnRenamed("cnt","counts")
res.show()
op="C:\\bigdata\\drivers\\result"
res.coalesce(1).write.mode("overwrite").format("json").save(op)
#toDF used only 2 purpose 1) rdd convert to dataframe, 2) rename all columns