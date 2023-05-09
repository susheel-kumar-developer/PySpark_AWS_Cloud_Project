from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
#tabs=["asl", "banktab", "dept","emp", "wbdata"]

spark.sparkContext.setLogLevel("ERROR")
myhost="jdbc:mysql://mysqldb.cwkqaojgxfrd.ap-south-1.rds.amazonaws.com:3306/newdb?useSSL=FALSE"
msurl="jdbc:sqlserver://database-1.cgnwnyon2j9e.ap-south-1.rds.amazonaws.com:1433;databaseName=susheeldb"
qry="select table_name from information_schema.tables where TABLE_SCHEMA='newdb'"

all=spark.read.format("jdbc").option("url",myhost).option("user","myuser").option("password","mypassword").option("driver","com.mysql.cj.jdbc.Driver").option("query",qry).load()
tabs=[x[0] for x in all.collect()]
for x in tabs:
    print("reading data from table: ",x)
    df=spark.read.format("jdbc").option("url",myhost).option("user","myuser").option("password","mypassword").option("driver","com.mysql.cj.jdbc.Driver").option("dbtable",x).load()
    df.show()
    #df.write.format("jdbc").option("url", msurl).option("user", "msuser").option("password", "mspassword").option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").option("dbtable", x).save()

