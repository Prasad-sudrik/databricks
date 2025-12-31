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
# MAGIC insert into employee_demo values(400,"Rohan","M",5000,"IT");
# MAGIC insert into employee_demo values(500,"Lara","F",8000,"HR");
# MAGIC insert into employee_demo values(600,"Mike","M",6000,"SALES");
# MAGIC insert into employee_demo values(700,"Sarah","F",5000,"IT");
# MAGIC insert into employee_demo values(800,"Mark","F",5000,"SALES");

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE employee_demo SET salary = 10000 WHERE emp_name = 'Mark';

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE delta.`/Filestore/tables/delta/path_employee_demo` SET salary = 12000 WHERE emp_name = 'Mark';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employee_demo;

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark,"/Filestore/tables/delta/path_employee_demo")
deltaTable.update(condition="emp_name = 'Mark'",set={"salary":"15000"})

# COMMAND ----------

deltaTable.update(
    condition=col('emp_name') == 'Mark',
    set = {"Dept":lit('IT')}
)

# COMMAND ----------


