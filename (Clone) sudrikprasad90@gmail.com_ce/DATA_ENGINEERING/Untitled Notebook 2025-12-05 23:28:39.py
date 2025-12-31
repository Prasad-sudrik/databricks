# Databricks notebook source
# MAGIC %md
# MAGIC ## Method 1: Pyspark

# COMMAND ----------

from delta.tables import *

DeltaTable.create(spark) \
    .tableName("employee_demo") \
    .addColumn("emp_id","INT") \
    .addColumn("emp_name","STRING") \
    .addColumn("gender","STRING") \
    .addColumn("salary","INT") \
    .addColumn("Dept","STRING") \
    .property("description","table created for demo purpose") \
    .location("/FileStore/tables/delta/createtable") \
    .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from employee_demo;

# COMMAND ----------

DeltaTable.createIfNotExists(spark) \
    .tableName("employee_demo") \
    .addColumn("emp_id","INT") \
    .addColumn("emp_name","STRING") \
    .addColumn("gender","STRING") \
    .addColumn("salary","INT") \
    .addColumn("Dept","STRING") \
    .execute()

# COMMAND ----------

DeltaTable.createOrReplace(spark) \
    .tableName("employee_demo") \
    .addColumn("emp_id","INT") \
    .addColumn("emp_name","STRING") \
    .addColumn("gender","STRING") \
    .addColumn("salary","INT") \
    .addColumn("Dept","STRING") \
    .property("description","table created for demo purpose") \
    .location("/FileStore/tables/delta/createtable") \
    .execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method 2: SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE employee_demo (
# MAGIC   emp_id INT,
# MAGIC   emp_Name STRING,
# MAGIC   gender STRING,
# MAGIC   salary INT,
# MAGIC   dept STRING
# MAGIC ) USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if NOT EXISTS employee_demo (
# MAGIC   emp_id INT,
# MAGIC   emp_Name STRING,
# MAGIC   gender STRING,
# MAGIC   salary INT,
# MAGIC   dept STRING
# MAGIC ) USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE employee_demo (
# MAGIC   emp_id INT,
# MAGIC   emp_Name STRING,
# MAGIC   gender STRING,
# MAGIC   salary INT,
# MAGIC   dept STRING
# MAGIC ) USING DELTA
# MAGIC LOCATION '/FileStore/tables/delta/createtable'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method 3: Using Dataframe

# COMMAND ----------

employee_data = [(100,"Stephen","M",2000,"IT"),
                 (200,"Phillip","M",8000,"HR"),
                 (300,"Lara","F",6000,"SALES")
                 ]
employee_schema = ["emp_id","emp_name","gender","salary","dept"]

df = spark.createDataFrame(data=employee_data,schema=employee_schema)

display(df)

# COMMAND ----------

# Create table in the metastore using DataFrame's schema and write data to it
df.write.format("delta").saveAsTable("default.employee_demo1")

# COMMAND ----------

# MAGIC %md
# MAGIC Schema of delta table will be same of dataframe

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo1;

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ 1. Save DataFrame as a Delta Table at a Location (External Delta Table)

# COMMAND ----------

df.write \
  .format("delta") \
  .mode("overwrite") \
  .save("dbfs:/data/sales_delta")


# COMMAND ----------

# MAGIC %md
# MAGIC ✅ This creates:
# MAGIC
# MAGIC - A Delta Lake table structure
# MAGIC
# MAGIC - _delta_log folder
# MAGIC
# MAGIC - Parquet data files
# MAGIC   at this exact location.

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ 2. Register This Location as a SQL Table (So You Can Query It)

# COMMAND ----------

# MAGIC %md
# MAGIC After saving:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE sales
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/data/sales_delta';
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Now you can run:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sales;

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ This is now a fully managed Delta table in the Metastore pointing to your location.

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ 3. Save and Register in ONE Step (Recommended)

# COMMAND ----------

df.write \
  .format("delta") \
  .mode("overwrite") \
  .option("path", "dbfs:/data/sales_delta") \
  .saveAsTable("sales")


# COMMAND ----------


