# Databricks notebook source
myname = dbutils.widgets.get("Name")
print(myname)

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/employees.csv

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TableAndViewExamples").getOrCreate()

file_path = "dbfs:/FileStore/tables/employees.csv"

df=spark.read.option("header","true").csv(file_path)

df.show()

# COMMAND ----------

table_name = "my_schema.employees"
df.write.format("delta").mode("overwrite").saveAsTable(table_name)

print(f"Table '{table_name}' created successfully")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from my_schema.employees

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW default.view_1 AS
# MAGIC SELECT FIRST_NAME,LAST_NAME,SALARY FROM my_schema.employees
# MAGIC WHERE SALARY < 3500

# COMMAND ----------

# MAGIC %sql
# MAGIC drop view default.view_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE default.employee_salary AS
# MAGIC SELECT EMPLOYEE_ID, FIRST_NAME,LAST_NAME,SALARY FROM default.employees_csv
# MAGIC WHERE SALARY >= 3300 AND EMPLOYEE_ID > 200

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.employee_salary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.employees_csv
# MAGIC ORDER BY EMPLOYEE_ID desc
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DEPARTMENT_ID, SUM(SALARY)
# MAGIC FROM default.employees_csv
# MAGIC GROUP BY DEPARTMENT_ID
# MAGIC ORDER BY DEPARTMENT_ID ASC

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TableAndViewExamples").getOrCreate()

# COMMAND ----------

data = [(1,"Alice",30),(2,"Bob",25),(3,"Charlie",35)]

columns = ['id',"name","age"]
df = spark.createDataFrame(data,schema=columns)
df.write.format("delta").saveAsTable("managed_table_example")

# COMMAND ----------

data = [(1,"Batman","Bruce Wayne"),(2,"Superman","Clark Kent"),(3,"Flash","Barry Allen")]
columns = ["id","Hero","Name"]

dataframe2 = spark.createDataFrame(data,schema=columns)

display(dataframe2)

dataframe2.write.format("delta").saveAsTable("my_schema.SuperHeroes")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from my_schema.superheroes;

# COMMAND ----------

spark.sql("""CREATE OR REPLACE VIEW my_schema.view_superheroes AS 
          SELECT Hero from my_schema.superheroes """)

# COMMAND ----------

df = spark.sql("select * from my_schema.view_superheroes")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM managed_table_example;

# COMMAND ----------


# dbutils.fs.ls("dbfs:/")

# Path to the folder in DBFS
folder_path = "dbfs:/data/external_location"

# Create the folder
# folder created to store table at an external locations
dbutils.fs.mkdirs(folder_path)

print(f"Folder created at: {folder_path}")





# COMMAND ----------

# create managed table data to external location
external_path = "dbfs:/data/external_location"

spark.sql("""
    SELECT *
    FROM managed_table_example
          """).write.format("delta").mode("overwrite").save(external_path)

# COMMAND ----------

# create an table based on external location referencing the saved data
# spark.sql("""
#     DROP TABLE external_table_example ;
#           """)

# Path to the folder in DBFS
# folder_path = "dbfs:/data/external_location/"

# # Delete the folder and its contents
# dbutils.fs.rm(folder_path, recurse=True)

# print(f"Deleted folder: {folder_path}")

external_path = "dbfs:/data/external_location"
# List the contents of the directory to confirm the presence of _delta_log
# dbutils.fs.ls(external_path)

spark.sql("""
    CREATE TABLE external_table_example
    USING DELTA
    LOCATION 'dbfs:/data/external_location'
          """)

print("External table created successfully.")

# COMMAND ----------

# Query the external table
external_table_df = spark.sql("SELECT * FROM external_table_example")
external_table_df.show()


# COMMAND ----------

# List the contents of the directory to confirm the presence of _delta_log
dbutils.fs.ls(external_path)


# COMMAND ----------

schemas = spark.sql("SHOW SCHEMAS")
schemas.show()

# COMMAND ----------

# MAGIC %sql 
# MAGIC create schema new_schema;

# COMMAND ----------

# dropping the external table does not delete the data from the external location which can be mnt point/databricks 


spark.sql("""
DROP TABLE default.external_table_example
""")

print("table dropped successfully")

# COMMAND ----------

dbutils.fs.ls("dbfs:/data/external_location")

# COMMAND ----------

# MAGIC %md
# MAGIC **Managed Table:** The Location field points to the default table directory (e.g., dbfs:/user/hive/warehouse).
# MAGIC
# MAGIC **External Table:** The Location field points to a custom directory outside the default location. (e.g., dbfs:/data/external_location/)
# MAGIC
# MAGIC **View:** The Type field will say VIEW.

# COMMAND ----------

# MAGIC %fs ls dbfs:/data/

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED my_schema.employees;
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE samples.nyctaxi.trips;

# COMMAND ----------

display(_sqldf)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------


