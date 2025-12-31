# Databricks notebook source
print("Hello")

# COMMAND ----------

df = spark.read.json("dbfs:/data/dataset_ch7/flight_time.json")

# COMMAND ----------

mdf = df.select(["ARR_TIME","DEST_CITY_NAME","DISTANCE"])

# COMMAND ----------

mdf.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE flights;

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY names;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREAtE TABLE names
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/user/hive/warehouse/names/'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM DELTA.`dbfs:/user/hive/warehouse/names/`

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/names/

# COMMAND ----------

# DBTITLE 1,Create Delta Table
# df.write.format("delta").saveAsTable("flights")

# COMMAND ----------

# DBTITLE 1,Enable Delta Cache
spark.conf.set("spark.databricks.io.cache.enabled","true")

# COMMAND ----------

spark.conf.get("spark.databricks.io.cache.enabled")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM names;

# COMMAND ----------

# MAGIC %md
# MAGIC Preload and Cache Entire Delta Table into Local DisK

# COMMAND ----------

# MAGIC %sql
# MAGIC CACHE SELECT * FROM names;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM names WHERE Year = 2020

# COMMAND ----------


