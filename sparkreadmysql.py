from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
from pyspark.sql import *

# jdbc:mysql://database-1.csqzkkzjx3z0.us-east-2.rds.amazonaws.com:3306/mysqldb?useSSL=false
host="jdbc:mysql://database-1.csqzkkzjx3z0.us-east-2.rds.amazonaws.com:3306/mysqldb?useSSL=false"
'''df=spark.read.format("jdbc").option("url",host).option("user","myuser").option("password","mypassword").option("driver","com.mysql.cj.jdbc.Driver").option("dbtable","asl").load()
ndf=df.distinct()
ndf.show()'''
#ndf.write.mode("append").format("parquet").saveAsTable("asltab")
data="C:\\bigdata\\drivers\\bank-full.csv"
df=spark.read.format("csv").option("header","true").option("sep",";").option("inferSchema","true").load(data)
df.show()
df.write.mode("append").format("jdbc").option("url",host).option("user","myuser").option("password","mypassword").option("driver","com.mysql.cj.jdbc.Driver").option("dbtable","wbdata").save()
