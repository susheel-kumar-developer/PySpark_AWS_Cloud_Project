from pyspark.sql import *
from pyspark.sql.functions import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

host="jdbc:mysql://database-2.cl2ylb4j7cra.ap-south-1.rds.amazonaws.com:3306/mysqldb"

spark.conf.set("spark.sql.session.timeZone", "IST")

df = spark.read.format("jdbc").option("url",host).option("dbtable","emp").option("user","myuser").option("password","mypassword").option("driver","com.mysql.jdbc.Driver").load()

# data cleaning & processing
ndf = df.na.fill(0).withColumn("today", current_timestamp()).where(col("sal")>2000)
ndf.show(truncate=False)

