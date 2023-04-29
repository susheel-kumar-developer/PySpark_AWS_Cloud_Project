# from pyspark.sql import *
# from pyspark.sql.functions import *
#
# spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

import boto3

s3 = boto3.resource('s3')

for bucket in s3.buckets.all():
    print(bucket)
