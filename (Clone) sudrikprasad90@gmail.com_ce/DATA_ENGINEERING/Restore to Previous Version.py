# Databricks notebook source
# MAGIC %sql
# MAGIC select * from scd2Demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS scd2Demo USING DELTA LOCATION '/FileStore/tables/scd2Demo/'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from scd2Demo;

# COMMAND ----------

from delta import *
targetTable = DeltaTable.forPath(spark,"/FileStore/tables/scd2Demo")

# COMMAND ----------

display(targetTable.history())

# COMMAND ----------

targetTable.restoreToVersion(3)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from scd2Demo;

# COMMAND ----------

display(targetTable.history())

# COMMAND ----------

print("hello world")

# COMMAND ----------


