from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()

def load_data(spark, format, filepath, options=None):
    if options is None:
        options = {}

    if format.lower() == 'csv':
        return spark.read.format('csv').options(header='True', inferSchema='True', **options).load(filepath)
    elif format.lower() == 'json':
        return spark.read.format('json').options(**options).load(filepath)
    elif format.lower() == 'orc':
        return spark.read.format('orc').options(**options).load(filepath)
    elif format.lower() == 'parquet':
        return spark.read.format('parquet').options(**options).load(filepath)
    elif format.lower() == 'xml':
        return spark.read.format('xml').options(rowTag='books', **options).load(filepath)
    else:
        raise ValueError(f"Unsupported format: {format}")
data = "C:\\bigdata\\drivers\\asl.csv"

df = load_data(spark, 'csv', data)
df.show()
