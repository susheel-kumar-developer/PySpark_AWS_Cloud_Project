from pyspark.sql import *
from pyspark.sql.functions import *
# import pyspark.sql.functions as F
from pyspark.sql.functions import spark_partition_id

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

# When the data are not balanced between workers, we call the data “skewed.”
# Skewed data means uneven utilization of compute and memory resources

# Sample data for "emp" table
emp_data = [(1, 101, "John", 50000),
            (2, 102, "Alice", 60000),
            (3, 101, "Bob", 55000)]
emp_schema = ["empid", "manager_id", "emp_name", "salary"]
df = spark.createDataFrame(emp_data, schema=emp_schema)
df.groupBy(spark_partition_id()).count().show()

df.rdd.glom().collect()

