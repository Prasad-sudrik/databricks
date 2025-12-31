# Databricks notebook source
from delta.tables import *

DeltaTable.create(spark) \
    .tableName("delta_internal_demo") \
    .addColumn("emp_id","INT") \
    .addColumn("emp_name","STRING") \
    .addColumn("gender","STRING") \
    .addColumn("salary","INT") \
    .addColumn("Dept","STRING") \
    .property("description","table created for demp purpose") \
    .location("/FileStore/tables/delta/arch_demo") \
    .execute()

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /FileStore/tables/delta/arch_demo/_delta_log/

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC head /FileStore/tables/delta/arch_demo/_delta_log/00000000000000000000.json

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_internal_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into delta_internal_demo values(100,"Stephen","M",2000,"IT");

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /FileStore/tables/delta/arch_demo/_delta_log/

# COMMAND ----------

# MAGIC %md
# MAGIC After insertion the json transaction file will contain metadata related to operation
# MAGIC like 
# MAGIC - Operation -> "WRITE"
# MAGIC - "minValues" for all the columns
# MAGIC -  "maxValues" for all the columns
# MAGIC -  "nullCount" for all the columns

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC head /FileStore/tables/delta/arch_demo/_delta_log/00000000000000000001.json

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into delta_internal_demo values (200,"Philipp","M",8000,"HR");
# MAGIC insert into delta_internal_demo values (300,"Lara","F",6000,"SALES");

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC head /FileStore/tables/delta/arch_demo/_delta_log/00000000000000000002.json

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC head /FileStore/tables/delta/arch_demo/_delta_log/00000000000000000002.json

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into delta_internal_demo values(100,"Stephen","M",2000,"IT");
# MAGIC insert into delta_internal_demo values (200,"Philipp","M",8000,"HR");
# MAGIC insert into delta_internal_demo values (300,"Lara","F",6000,"SALES");
# MAGIC
# MAGIC insert into delta_internal_demo values(100,"Stephen","M",2000,"IT");
# MAGIC insert into delta_internal_demo values (200,"Philipp","M",8000,"HR");
# MAGIC insert into delta_internal_demo values (300,"Lara","F",6000,"SALES");

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /FileStore/tables/delta/arch_demo/_delta_log/

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from delta_internal_demo where emp_id = 100;

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /FileStore/tables/delta/arch_demo/_delta_log/

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /FileStore/tables/delta/arch_demo/_delta_log/

# COMMAND ----------

# MAGIC %md
# MAGIC Even after the deleting the 3 records from table still the data file(parquet) is residing in the location because as per delta lake architecture it follows soft delete pattern.In the log file it will be mentioned that we don't have to consider three files out of this nine. it has default time duration of 7 days.it will be removed after 7 days. but that parameter is configurable

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC head /FileStore/tables/delta/arch_demo/_delta_log/00000000000000000010.json

# COMMAND ----------

display(spark.read.parquet("/FileStore/tables/delta/arch_demo/_delta_log/00000000000000000010.checkpoint.parquet"))

# COMMAND ----------

# MAGIC %sql
# MAGIC update  delta_internal_demo set emp_name = 'Rohan' where emp_id = 200;

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /FileStore/tables/delta/arch_demo/_delta_log/

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC head /FileStore/tables/delta/arch_demo/_delta_log/00000000000000000011.json

# COMMAND ----------

display(spark.read.parquet("/FileStore/tables/delta/arch_demo/part-00000-21b6f608-4e64-405e-8d4f-098b37878eda-c000.snappy.parquet"))

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /FileStore/tables/delta/arch_demo/

# COMMAND ----------


