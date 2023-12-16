from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()


data = [("Harry",29),("John",30),("David",35)]

schema = ["Name","Age"]

try:
    df = spark.createDataFrame(data,schema)
    df.show()
    df.printSchema()
    # res = 1/0
    # print(res)
    with open("filename",'r') as f:
        df = f.readline()
# except Exception as error:
#     print(f" getting error due to schema type: {str(error)}")

except FileNotFoundError as e:
    print(f" Getting error {str(e)}")

except PermissionError as e:
    print(f"getting permission error {str(e)}")

except ValueError as e:
    print(f"{str(e)}")
except ZeroDivisionError as z:
    print(f" getting zero division error-> {str(z)}")

finally:
    spark.stop()

