# Databricks notebook source
dbutils.fs.rm("/FileStore/tables/delta/path_employee_demo",True);

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE employee_demo;

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
    .location("/FileStore/tables/delta/path_employee_demo") \
    .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into employee_demo values(100,"Stephen","M",2000,"IT");

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCHEMA EVOLUTION

# COMMAND ----------

from pyspark.sql.types import StructField,StringType,IntegerType,StringType
employee_data = [(200,"Phillip","M",8000,"HR","test_demo")]

employee_schema = StructType([  \
    StructField("emp_id",IntegerType(),False),  \
    StructField("emp_name",StringType(),True),  \
    StructField("gender",StringType(),True),    \
    StructField("salary",IntegerType(),False),  \
    StructField("dept",StringType(),True),  \
    StructField("additionalColumn",StringType(),True)   \
])

df = spark.createDataFrame(data=employee_data,schema=employee_schema)

display(df)

# COMMAND ----------

df.write.format("delta").mode("append").saveAsTable("employee_demo")

# COMMAND ----------

df.write.option("mergeSchema","true").format("delta").mode("append").saveAsTable("employee_demo")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC CACHE SELECT * FROM names
