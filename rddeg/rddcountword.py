from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

data = "C:\\bigdata\\drivers\\emailsmay4.txt"

rddfile = spark.sparkContext.textFile(data)

rddc = rddfile.filter(lambda x:'@' in x).map(lambda x:x.split(" ")).map(lambda x:(x[0],x[-1]))

df = rddc.toDF(["name","email"])

df.createOrReplaceTempView("tab")
res = spark.sql("select * from tab where email like '%gmail.com%'")
res.show(truncate=False)

# +------------+--------------------+
# |        name|               email|
# +------------+--------------------+
# |        Balu|hereisbalaji4u@gm...|
# |           K|  venki241@gmail.com|
# |    Mohammad|mtmohdtahir@gmail...|
# |        Ajit| jitmnk007@gmail.com|
# |     Raghava|raghavendratiruna...|
# |      Ranjit|ranjitpatil789@gm...|
# |       parag| paragsoft@gmail.com|
# |        anuj|anuj.itindia@gmai...|
# | Chiranjeevi|eswar.nalla70@gma...|
# |      Khasim| Khasim519@gmail.com|
# |Praveenkumar|Sathyapraveen.sk@...|
# |     Naushad|naushad520@gmail.com|
# |      Mastan|mastanetl4@gmail.com|
# |        Balu|srinivaschintala1...|
# |     PRIYESH|n.priyesh1989@gma...|
# |   Manjunath|manjunath.ptr@gma...|
# |   Chandrika|sreechandu207@gma...|
# |Praveenkumar|Sathyapraveen.sk@...|



