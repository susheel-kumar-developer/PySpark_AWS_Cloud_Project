from pyspark.sql import *
from pyspark.sql.functions import *
import pandas as pd
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
url="https://raw.githubusercontent.com/spe301/Sales-Scientist/main/datasets/leadsML.csv"
# https://raw.githubusercontent.com/cs109/2014_data/master/countries.csv
livedata=pd.read_csv(url)

# create spark dataframe
df=spark.createDataFrame(livedata)

df.show()