from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data="C:\\Users\\skmjm\\Downloads\\world_bank.json"

df=spark.read.format("json").option("multiLine","false").load(data)

df=df.withColumn("theme_namecode", explode(col("theme_namecode")))\
 .withColumn("sector_namecode", explode(col("sector_namecode")))\
 .withColumn("sector", explode(col("sector")))\
 .withColumn("projectdocs", explode(col("projectdocs")))\
 .withColumn("mjtheme", explode(col("mjtheme")))\
 .withColumn("mjtheme_namecode", explode(col("mjtheme_namecode")))\
 .withColumn("mjsector_namecode",explode(col("mjsector_namecode")))\
 .withColumn("majorsector_percent",explode(col("majorsector_percent")))

df= df.withColumn("theme_namecode_code", col("theme_namecode.code"))\
 .withColumn("theme_namecode_name", col("theme_namecode.name")).drop("theme_namecode")\
 .withColumn("theme1_name" , col("theme1.Name"))\
.withColumn("theme1_percent", col("theme1.Percent")).drop("theme1")\
 .withColumn("sector_namecode_code", col("sector_namecode.code"))\
 .withColumn("sector_namecode_name", col("sector_namecode.name")).drop("sector_namecode")\
 .withColumn("sector1_name", col("sector1.Name")).withColumn("sector1_percent", col("sector1.Percent")).drop("sector1")\
 .withColumn("sector2_name", col("sector2.Name")).withColumn("sector2_percent", col("sector2.Percent")).drop("sector2")\
 .withColumn("sector3_name", col("sector3.Name")).withColumn("sector3_percent", col("sector3.Percent")).drop("sector3")\
 .withColumn("sector4_name", col("sector4.Name")).withColumn("sector4_percent", col("sector4.Percent")).drop("sector4")\
 .withColumn("sectorName", col("sector.Name")).drop("sector")\
 .withColumn("project_abstract_cdata", col("project_abstract.cdata")).drop("project_abstract")\
    .withColumn("projectdocsdocdate",col("projectdocs.DocDate"))\
    .withColumn("projectdocsDocType",col("projectdocs.DocType"))\
    .withColumn("projectdocsDocTypeDesc",col("projectdocs.DocTypeDesc"))\
    .withColumn("projectdocsDocURL",col("projectdocs.DocUrl"))\
    .withColumn("projectdocsEntryID",col("projectdocs.EntityID")).drop("projectdocs")\
    .withColumn("mjtheme_namecode_code",col("mjtheme_namecode.code"))\
    .withColumn("mjtheme_namecode_name",col("mjtheme_namecode.name")).drop("mjtheme_namecode")\
    .withColumn("mjsector_namecode_code",col("mjsector_namecode.code"))\
    .withColumn("mjsector_namecode_name",col("mjsector_namecode.name")).drop("mjsector_namecode")\
    .withColumn("majorsector_percentName",col("majorsector_percent.Name"))\
    .withColumn("majorsector_percent_Perecent",col("majorsector_percent.Percent")).drop("majorsector_percent")\
    .withColumn("idoid",col("`_id`.`$oid`")).drop("_id")


df.show()
df.printSchema()