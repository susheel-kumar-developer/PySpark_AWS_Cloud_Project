from pyspark.sql import *
from pyspark.sql.functions import *
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc=spark.sparkContext
data="C:\\bigdata\\drivers\\asl.csv"
drdd=sc.textFile(data)
# skip = drdd.first()
# res=drdd.filter(lambda x:x!=skip)
res=drdd.zipWithIndex().filter(lambda x:x[1]>=5).map(lambda x:x[0])
# .zipWithIndex().map(lambda x:x[0]).filter(lambda x:x[1]>=5).map(lambda x:x[0])
df=spark.read.csv(res,header=True)
df.show()

# print(df.tail(2))
# pandasDF = df.limit(10).toPandas()

# print(pandasDF)
#for i in res.collect():
#    print(i)
#df=spark.read.format("csv").option("header","true").load(data)
#df.show()