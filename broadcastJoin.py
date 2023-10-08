from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

dataCsv= "C:\\bigdata\\drivers\\bigFileasl.csv"
dataCsvSmall="C:\\bigdata\\drivers\\aslModify.csv"

dfCsvLarg=spark.read.format("csv").option("header","true").load(dataCsv)
dfCsvSmall=spark.read.format("csv").option("header","true").load(dataCsvSmall)
# dfCsvSmall.show()

dropCol=dfCsvLarg.drop("City","Marks")
dropCol.show()

# dfSmall=broadcast(dfCsvSmall)

# test using left, right, inner join in broadcast
# result_df=dfCsvLarg.join(dfSmall,"Age","cross")

# result_df.select(count(result_df.Age)).show()
# result_df.show()
# result_df.printSchema()
# result_df.explain(extended=False)


#

