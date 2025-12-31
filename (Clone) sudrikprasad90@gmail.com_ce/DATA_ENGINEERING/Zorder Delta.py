# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/scd2Demo/_delta_log/

# COMMAND ----------

dbutils.fs.rm('/FileStore/tables/scd2Demo/',True)

# COMMAND ----------

# Remove everything including hidden files
dbutils.fs.rm('/FileStore/tables/scd2Demo', recurse=True)

# Verify it's gone
try:
    dbutils.fs.ls('/FileStore/tables/scd2Demo')
    print("Directory still exists!")
except:
    print("Directory successfully removed")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS scd2Demo;
# MAGIC
# MAGIC -- Then create it
# MAGIC CREATE TABLE scd2Demo (
# MAGIC   pk1 INT,
# MAGIC   pk2 STRING,
# MAGIC   dim1 INT,
# MAGIC   dim2 INT,
# MAGIC   dim3 INT,
# MAGIC   dim4 INT,
# MAGIC   active_status STRING,
# MAGIC   start_date TIMESTAMP,
# MAGIC   end_date TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/FileStore/tables/scd2Demo'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from scd2Demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into scd2Demo values (111,'Unit1',200,500,800,400,'Y',current_timestamp(),'9999-12-31');
# MAGIC insert into scd2Demo values (222,'Unit2',900,Null,700,100,'Y',current_timestamp(),'9999-12-31');
# MAGIC insert into scd2Demo values (333,'Unit3',300,900,250,650,'Y',current_timestamp(),'9999-12-31');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from scd2Demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into scd2Demo values (666,'Unit1',200,500,800,400,'Y',current_timestamp(),'9999-12-31');
# MAGIC insert into scd2Demo values (777,'Unit2',900,Null,700,100,'Y',current_timestamp(),'9999-12-31');
# MAGIC insert into scd2Demo values (888,'Unit3',300,900,250,650,'Y',current_timestamp(),'9999-12-31');

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from scd2Demo where pk1=777;

# COMMAND ----------

# MAGIC %md
# MAGIC ## No files deleted

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/scd2Demo/

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/scd2Demo/_delta_log/

# COMMAND ----------

# MAGIC %sql
# MAGIC update scd2Demo
# MAGIC set dim1 = 100 where pk1 = 666;

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/scd2Demo/

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extra file is created for updated record/ latest json log file will tell to use which file to refer for latest data.

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history scd2Demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Zorder Optimize

# COMMAND ----------

# MAGIC %md
# MAGIC #### zorder re-arrainges the data in the files based on a column ,so we have to look in less no. of file when we read/query the data.As we have only one file we cannot see over here
# MAGIC ![](path)

# COMMAND ----------

# Set target size to 128 MB
spark.conf.set("spark.databricks.delta.optimize.maxFileSize", "2000")

# Run the optimize command
spark.sql("OPTIMIZE scd2Demo ZORDER BY pk1")


# COMMAND ----------

# %sql
# OPTIMIZE scd2Demo
# ZORDER BY pk1

# COMMAND ----------

# MAGIC %md
# MAGIC #### As below shown it has removed 5 files and added 1 file to replace those files (files will be still present but json transaction file will point to latest added file now)

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled","false")

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM scd2Demo RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/scd2Demo/

# COMMAND ----------

d = spark.read.format("delta").load("dbfs:/FileStore/tables/scd2Demo/part-00000-f0cc50bc-a57a-4eb5-994d-3f433f4614e4-c000.snappy.parquet")

# COMMAND ----------



# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/scd2Demo/_delta_log/

# COMMAND ----------

jDF = spark.read.json('dbfs:/FileStore/tables/scd2Demo/_delta_log/00000000000000000008.json')
display(jDF)

# COMMAND ----------

jDF = spark.read.json('dbfs:/FileStore/tables/scd2Demo/_delta_log/00000000000000000009.json')
display(jDF)

# COMMAND ----------

print("hello")

# COMMAND ----------


