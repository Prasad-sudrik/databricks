# Databricks notebook source
dbutils.fs.head("/databricks-datasets/retail-org/company_employees/company_employees.csv")

# COMMAND ----------

# Create Parameter dept
dbutils.widgets.text("dept","","Department Name")

# COMMAND ----------

#Get the parameter value in variable_dept
_dept = dbutils.widgets.get("dept")

# COMMAND ----------

#print parameter variable
print(_dept)

# COMMAND ----------

# Read all teh data through Spark
df = spark.read.csv("/databricks-datasets/retail-org/company_employees/company_employees.csv",header=True)

# COMMAND ----------

display(df)

# COMMAND ----------

df_filtered = df.where(f"upper(department) = upper('{_dept}') and active_record = '1'")

# COMMAND ----------

display(df_filtered)

# COMMAND ----------

# write data to delta table with department name
_count = df_filtered.count()
if _count >0:
    df_filtered.write.mode("overwrite").saveAsTable(f"bronze.dept_{_dept}")
    print(f"Data written for {_dept} ")
    display(spark.table(f"bronze.dept_{_dept}"))
else:
    print(f"NO data for {_dept}")

# COMMAND ----------

# Exit status as --number of records written
dbutils.notebook.exit(_count)
