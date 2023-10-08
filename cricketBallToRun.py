from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data="C:\\bigdata\\drivers\\balls-overs.txt"
score_df = spark.read.csv(data, header=True,inferSchema=True)

# calculate the total runs for each ball and the cumulative sum of runs
overs_df = score_df.withColumn('overs', floor(score_df['balls'] / 6) + 1)

# Group by overs and calculate the total runs scored in each over
score_agg_df = overs_df.groupBy('overs').agg(sum('runs').alias('runs'))

# Calculate the cumulative score
score_agg_df = score_agg_df.withColumn('score', sum('runs').over(Window.orderBy('overs')))

# Show the final result
score_agg_df.show()