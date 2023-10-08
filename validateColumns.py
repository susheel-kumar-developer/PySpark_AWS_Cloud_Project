from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import numpy as np

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()

data = [
    ('venu,32,hyd'),
    ('satya,34,hyderabad'),
    ('john,25,newyork'),
    ('jane,30,losangeles,usa'),
    ('tom,28,mumbai')
]

# Define the schema
#schema = "name STRING, age INT, city STRING"
sch = StructType().add("name", StringType()).add("age", StringType()).add("city",StringType())

# Create a DataFrame from the input data
df = spark.createDataFrame(data, sch)

# Split the record into separate columns using the comma as the delimiter


# Create a new DataFrame with the parsed columns
parsed_df = df

# Filter out the malformed records using a null check
malformed_records = parsed_df.filter(
    col("name").isNull() | col("age").isNull() | col("city").isNull()
)

# Filter out the valid records
valid_records = parsed_df.filter(
    col("name").isNotNull() & col("age").isNotNull() & col("city").isNotNull()
)

# Show the separated records
print("Malformed Records:")
malformed_records.show(truncate=False)

print("Valid Records:")
valid_records.show(truncate=False)