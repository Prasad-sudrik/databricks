# Databricks notebook source
# MAGIC %md
# MAGIC 1. Show the list of directories in DBFS root storage

# COMMAND ----------

# MAGIC %fs ls dbfs:/

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 3. show content on the local file system
# MAGIC

# COMMAND ----------

# MAGIC %fs ls file:/

# COMMAND ----------

# MAGIC %md
# MAGIC 4 Show all mounted locations

# COMMAND ----------

# MAGIC %fs mounts

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS dev;

# COMMAND ----------

base_path = "dbfs:/databricks-datasets/power-plant/data/"

power_plant_path = f"{base_path}"

display(dbutils.fs.ls(base_path))

# COMMAND ----------

# Read TSV files into a DataFrame
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", "\t") \
    .load(f"{base_path}")

# Display the DataFrame to verify
display(df)


# COMMAND ----------


