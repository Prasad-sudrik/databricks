# Databricks notebook source
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode


# COMMAND ----------

BASE_URL = "https://dragonball-api.com/api"
ENDPOINT = "characters"

PAGE_SIZE = 100


# COMMAND ----------

all_items = []
page = 1

while True:
    url = f"{BASE_URL}/{ENDPOINT}"
    print("hello")
    params = {
        "page": page,
        "limit": PAGE_SIZE
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    
    payload = response.json()
    items = payload.get("items", [])
    
    if not items:
        break
    
    all_items.extend(items)
    print(f"Fetched page {page}, records: {len(items)}")
    
    page += 1


# COMMAND ----------

from pyspark.sql.types import StructField,StructType,IntegerType,StringType

# COMMAND ----------

schema = StructType([
    StructField("id",IntegerType(),False),
    StructField("name",StringType(),False),
    StructField("ki",StringType(),False),
    StructField("maxKi",StringType(),False),
    StructField("race",StringType(),False),
    StructField("gender",StringType(),False),
    StructField("description",StringType(),False),
    StructField("image",StringType(),False),   
    StructField("affiliation",StringType(),False),
    StructField("deletedAt",StringType(),True)
])

# COMMAND ----------

df = spark.createDataFrame(all_items,schema=schema)

# COMMAND ----------

display(df)

# COMMAND ----------

print(all_items)

# COMMAND ----------

df.repartition(14)

# COMMAND ----------

df.write.mode("overwrite").option("header",True).csv("/FileStore/raw/DragonBallZ/")

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE raw_DragonBallZ;

# COMMAND ----------

rdf = spark.read.csv("/FileStore/raw/DragonBallZ/")
rdf.write.format("delta").mode("overwrite").save("/FileStore/raw/delta/DragonBallZ/")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE raw_DragonBallZ
# MAGIC USING DELTA
# MAGIC OPTIONS (
# MAGIC   path "dbfs:/FileStore/raw/delta/DragonBallZ/"
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw_DragonBallZ;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE ref_DragonBallZ
# MAGIC SHALLOW CLONE raw_DragonBallZ
# MAGIC LOCATION "dbfs:/FileStore/ref/DragonBallZ/";
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/raw/delta/DragonBallZ/

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE raw_DragonBallZ;

# COMMAND ----------

dbutils.fs.rm('/FileStore/raw/DragonBallZ/',True)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw_DragonBallZ;

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE raw_DragonBallZ;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE raw_DragonBallZ AS \
# MAGIC id INT,
# MAGIC name string,
# MAGIC ki string,
# MAGIC maxKi string,
# MAGIC race string,
# MAGIC description string,
# MAGIC image string,
# MAGIC affiliation string,
# MAGIC deletedAt string,
# MAGIC  USING DELTA LOCATION '/FileStore/raw/DragonBallZ/'

# COMMAND ----------

df.write.format("csv").location("/FileStore/raw/DragonBallZ/")

# COMMAND ----------


