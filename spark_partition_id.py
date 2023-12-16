import pandas as pd
import numpy as np
from pyspark.sql.functions import spark_partition_id
from pyspark.sql import *

# Create some dummy data

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
def create_dummy_data():

    data = np.vstack([np.random.randint(0, 5, size=10),
                      np.random.random(10)])

    df = pd.DataFrame(data.T, columns=["id", "values"])

    return spark.createDataFrame(df)

def show_partition_id(df):
    """Helper function to show partition."""
    return df.select(*df.columns, spark_partition_id().alias("pid")).show()

df1 = create_dummy_data()
df2 = create_dummy_data()
show_partition_id(df1)
show_partition_id(df2)

