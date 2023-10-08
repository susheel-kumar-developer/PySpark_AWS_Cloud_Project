from pyspark.sql import *
from pyspark.sql.functions import *
import pandas as pd
import numpy as np

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

'''df=pd.DataFrame(
    {
        "A": 1.0,
        "B": pd.Timestamp("20130102"),
        "C": pd.Series(1, index=list(range(4)), dtype="float32"),
        "D": np.array([3] * 4, dtype="int32"),
        "E": pd.Categorical(["test", "train", "test", "train"]),
        "F": "foo",
    }
)
#print(df)
#python dataframe convert to spark dataframe
sdf = spark.createDataFrame(df)
#sdf.show()
ndf = sdf.groupBy(col("E")).count()
#spark dataframe convert to python pandas dataframe.
pdf = ndf.toPandas()
print(pdf)
print(type(pdf))
'''
url="https://raw.githubusercontent.com/cs109/2014_data/master/countries.csv"
c=pd.read_csv(url)
#print(c)
#convert pandas df to spark dataframe
df=spark.createDataFrame(c)
#df.show()
#df.printSchema()
res=df.groupBy(col("region")).agg(count("country").alias("cnt"),collect_list(col("country")).alias("countries"))
res.show(10,truncate=False)
op="C:\\bigdata\\pandasspark.json"
#res.write.mode("overwrite").format("json").save(op)
pres = res.toPandas().to_csv(op)

