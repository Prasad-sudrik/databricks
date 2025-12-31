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
# MAGIC select * from employee_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into employee_demo values(400,"Rohan","M",5000,"IT");
# MAGIC insert into employee_demo values(500,"Lara","F",8000,"HR");
# MAGIC insert into employee_demo values(600,"Mike","M",6000,"SALES");
# MAGIC insert into employee_demo values(700,"Sarah","F",5000,"IT");

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into employee_demo values(700,"Serena","F",9000,"HR");

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM employee_demo WHERE emp_id = 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employee_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM delta.`/Filestore/tables/delta/path_employee_demo` WHERE emp_id = 200;

# COMMAND ----------

spark.sql("delete from employee_demo where emp_id = 300")

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forName(spark,'employee_demo')
deltaTable.delete("emp_id = 400")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employee_demo;

# COMMAND ----------

deltaTable.delete(col("emp_id") == 600)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employee_demo;

# COMMAND ----------


