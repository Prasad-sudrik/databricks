# Databricks notebook source
# MAGIC %sql
# MAGIC describe history scd2demo;

# COMMAND ----------

df = spark.read \
    .format("delta") \
    .option("timestampAsOf","2025-12-08T16:20:35.000+00:00") \
    .table("scd2Demo")
display(df)

# COMMAND ----------

df = spark.read \
    .format("delta") \
    .option("versionAsOf",3) \
    .load("/FileStore/tables/scd2Demo")
display(df)

# COMMAND ----------

df = spark.read \
    .format("delta") \
    .option("versionAsOf",3) \
    .table("scd2Demo")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM scd2demo VERSION AS OF 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/FileStore/tables/scd2Demo` VERSION AS OF 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM scd2demo TIMESTAMP AS OF '2025-12-08T16:20:36.000+00:00';

# COMMAND ----------


