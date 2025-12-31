# Databricks notebook source
manynumber = spark.range(1,10000)

# COMMAND ----------

dbutils.fs.rm("dbfs:/user/hive/warehouse/people_delta/",True)

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DeltaExample").getOrCreate()

# Sample dataframe
data = [
    (1, "Alice", 30),
    (2, "Bob", 25),
    (3, "Charlie", 35)
]

columns = ["id", "name", "age"]
df = spark.createDataFrame(data, columns)

# Write as Delta table (managed table)
df.write.format("delta").saveAsTable("default.people_delta")

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forName(spark, "default.people_delta")
deltaTable.history().display(truncate=False)

# COMMAND ----------

display(dbutils.fs.mkdirs("dbfs:/data/dttable"))

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/data/dttable

# COMMAND ----------

data = [
    (1, "Alice", 30),
    (2, "Bob", 25),
    (3, "Charlie", 35)
]

columns = ["id", "name", "age"]

df = spark.createDataFrame(data, columns)
df.show()


# COMMAND ----------

df.write.format("delta").mode("overwrite").save("dbfs:/data/dttable")


# COMMAND ----------

# MAGIC %fs ls dbfs:/data/dttable/

# COMMAND ----------

# MAGIC %fs ls dbfs:/data/dttable/_delta_log/

# COMMAND ----------

# MAGIC %fs ls dbfs:/data/dttable/_delta_log/00000000000000000000.json

# COMMAND ----------

df = spark.read.json("dbfs:/data/dttable/_delta_log/00000000000000000000.json")
df.display(truncate=False)


# COMMAND ----------

df = spark.read.json("dbfs:/data/dttable/_delta_log/00000000000000000001.json")
df.display(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM PARQUET.`dbfs:/FileStore/data/shoppinginvoices/invoices_1_100.parquet`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE invoices
# MAGIC AS
# MAGIC SELECT * FROM PARQUET.`dbfs:/FileStore/data/shoppinginvoices/invoices_1_100.parquet`

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY invoices

# COMMAND ----------

j = spark.read.json("dbfs:/user/hive/warehouse/invoices/_delta_log/00000000000000000000.json")

# COMMAND ----------

j.display(False)

# COMMAND ----------

# MAGIC %sql 
# MAGIC INSERT INTO invoices 
# MAGIC SELECT * FROM PARQUET.`dbfs:/FileStore/data/shoppinginvoices/invoices_101_200.parquet`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MIN(customer_id),MAX(customer_id),COUNT(*) from invoices;

# COMMAND ----------

dbutils.fs.cp(
    "dbfs:/user/hive/warehouse/invoices/",
    "dbfs:/FileStore/my_table_export/",
    recurse=True
)


# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/my_table_export/"))

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select * from invoices where customer_id = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC update invoices 
# MAGIC set quantity = 10
# MAGIC where customer_id = 1;

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/invoices/

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY invoices;

# COMMAND ----------

j = spark.read.json("dbfs:/user/hive/warehouse/invoices/_delta_log/00000000000000000002.json")

# COMMAND ----------

j.display(False)

# COMMAND ----------

spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL delta.`dbfs:/user/hive/warehouse/invoices`

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE invoices SET TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE invoices;
# MAGIC

# COMMAND ----------

# MAGIC %sql DELETE FROM invoices WHERE customer_id = 99;

# COMMAND ----------


