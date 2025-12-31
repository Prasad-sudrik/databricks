# Databricks notebook source
from delta.tables import *

DeltaTable.create(spark) \
.tableName("employee_demo") \
.addColumn("emp_id","INT") \
.addColumn("emp_name","STRING") \
.addColumn("gender","STRING") \
.addColumn("salary","INT") \
.addColumn("Dept","STRING") \
.property("description","table created for demo purpose") \
.location("/Filestore/tables/delta/path_employee_demo") \
.execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Style Insert

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into employee_demo values(100,"Stephen","M",2000,"IT");

# COMMAND ----------

display(spark.sql("select * from employee_demo"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dataframe Insert

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.types import StructField,StringType,IntegerType,StringType

employee_data = [(200,"Philipp","M",8000,"HR")]

employee_schema = StructType([ \
    StructField("emp_id",IntegerType(),False), \
    StructField("emp_name",StringType(),True), \
    StructField("gender",StringType(),True), \
    StructField("salary",IntegerType(),True), \
    StructField("dept",StringType(),True) \
   ])

df = spark.createDataFrame(data=employee_data,schema=employee_schema)
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo;

# COMMAND ----------

df.write.format("delta").mode("append").saveAsTable("employee_demo")

# COMMAND ----------

df.write.insertInto('employee_demo',overwrite=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo;

# COMMAND ----------

df.createOrReplaceTempView('delta_data')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into employee_demo
# MAGIC select * from delta_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_demo;

# COMMAND ----------


