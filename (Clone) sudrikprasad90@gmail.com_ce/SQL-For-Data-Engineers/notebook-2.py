# Databricks notebook source


folder_path = "dbfs:/FileStore/shared_uploads/sudrikprasad90@gmail.com/dataset_ch7"

dbutils.fs.mkdirs(folder_path)

# COMMAND ----------

# dbutils.fs.mv("dbfs:/FileStore/flight_time.json","dbfs:/data/dataset_ch7" , recurse=True)
# dbutils.fs.mv("dbfs:/FileStore/people.json","dbfs:/data/dataset_ch7" , recurse=True)
# dbutils.fs.mv("dbfs:/FileStore/people_2.json","dbfs:/data/dataset_ch7" , recurse=True)
# dbutils.fs.mv("dbfs:/FileStore/people_3.json","dbfs:/data/dataset_ch7" , recurse=True)


# COMMAND ----------

# dbutils.fs.mv("dbfs:/FileStore/shared_uploads/sudrikprasad90@gmail.com/people.json", "dbfs:/FileStore/shared_uploads/sudrikprasad90@gmail.com/dataset_ch7", recurse=False)

# COMMAND ----------

# dbutils.fs.mv("dbfs:/FileStore/shared_uploads/sudrikprasad90@gmail.com/people_2.json", "dbfs:/FileStore/shared_uploads/sudrikprasad90@gmail.com/dataset_ch7", recurse=False)

# COMMAND ----------

# dbutils.fs.mv("dbfs:/FileStore/shared_uploads/sudrikprasad90@gmail.com/people_3.json", "dbfs:/FileStore/shared_uploads/sudrikprasad90@gmail.com/dataset_ch7", recurse=False)

# COMMAND ----------

# dbutils.fs.mkdirs("dbfs:/data/dataset_ch7")


# COMMAND ----------

# %fs rm "dbfs:/data/people_3.json"

# COMMAND ----------

# dbutils.fs.rmdirs("dbfs:/FileStore/dataset_ch7")

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TableAndViewExamples").getOrCreate()

base_dir = "dbfs:/data"
# spark.sql(f"CREATE CATALOG IF NOT EXISTS dev")
spark.sql(f"CREATE DATABASE IF NOT EXISTS demo_db")
flight_schema_ddl = """FL_DATE DATE,OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, ORIGIN_CITY_NAME STRING,DEST STRING,DEST_CITY_NAME STRING, CRS_DEP_TIME INT,DEP_TIME INT,WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED STRING, DISTANCE INT"""

flight_time_df = (spark.read.format("json")
                  .schema(flight_schema_ddl)
                  .option("dateFormat","M/d/y")
                  .load(f"{base_dir}/dataset_ch7/flight_time.json")
                  )

# COMMAND ----------


spark.sql(f"CREATE DATABASE IF NOT EXISTS demo_db")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- default format is DELTA
# MAGIC CREATE TABLE IF NOT EXISTS demo_db.flight_time_tbl(
# MAGIC FL_DATE date,
# MAGIC OP_CARRIER string,
# MAGIC OP_CARRIER_FL_NUM integer,
# MAGIC ORIGIN string,
# MAGIC ORIGIN_CITY_NAME string,
# MAGIC DEST string,
# MAGIC DEST_CITY_NAME string,
# MAGIC CRS_DEP_TIME integer,
# MAGIC DEP_TIME integer,
# MAGIC WHEELS_ON integer,
# MAGIC TAXI_IN integer,
# MAGIC CRS_ARR_TIME integer,
# MAGIC ARR_TIME integer,
# MAGIC CANCELLED string,
# MAGIC DISTANCE integer
# MAGIC ) USING DELTA;

# COMMAND ----------

dbutils.fs.rm("dbfs:/user/hive/warehouse/demo_db.db/flight_time_tbl",True)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- we only created a table with a schema no data is present at the moment
# MAGIC select * from demo_db.flight_time_tbl;

# COMMAND ----------

# MAGIC %md
# MAGIC **2. Load data into delta table**

# COMMAND ----------

flight_time_df.write.format("delta").mode("append").saveAsTable("demo_db.flight_time_tbl")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from demo_db.flight_time_tbl;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended demo_db.flight_time_tbl

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE employee_salary
# MAGIC USING PARQUET
# MAGIC LOCATION 'dbfs:/user/hive/warehouse/employee_salary';
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `hive_metastore`; select * from `default`.`employee_salary` limit 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended default.employee_salary

# COMMAND ----------

display(_sqldf)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended demo_db.flight_time_tbl;

# COMMAND ----------

from delta import DeltaTable

(DeltaTable.createOrReplace(spark)
        .tableName("demo_db.flight_time_tbl")
        .addColumn("FL_DATE","DATE")
 )

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Read delta table using dataframe api

# COMMAND ----------

spark.read.format("delta").table("demo_db.flight_time_tbl").display()

# COMMAND ----------

a = [12,34,32]
for i in a:
    print(i)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/user/hive/warehouse/demo_db.db/people/

# COMMAND ----------

dbutils.fs.rm("dbfs:/user/hive/warehouse/demo_db.db/people/",True)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE demo_db.people(
# MAGIC   id INT,
# MAGIC   firstName STRING,
# MAGIC   lastName STRING,
# MAGIC   birthDate STRING
# MAGIC ) USING DELTA;  
# MAGIC
# MAGIC INSERT OVERWRITE TABLE demo_db.people
# MAGIC SELECT id,fname as firstName,lname as lastName,dob as birthDate
# MAGIC FROM JSON.`/data/dataset_ch7/people.json`;
# MAGIC
# MAGIC SELECT * FROM demo_db.people;

# COMMAND ----------

# MAGIC %sql 
# MAGIC DELETE FROM demo_db.people WHERE firstName = "M David"

# COMMAND ----------



# COMMAND ----------

from delta import DeltaTable

people_dt = DeltaTable.forName(spark,"demo_db.people")

people_dt.delete("firstName = 'abdul'")

# COMMAND ----------

import pyspark.sql.functions as f

people_dt.update(
    condition="birthDate = '1975-05-25'",
    set={"firstName":f.initcap("firstName"),"lastName":f.initcap("lastName")}
)

people_dt.toDF().display()

# COMMAND ----------

# MAGIC %md
# MAGIC Merge the given dataframe into the delta table

# COMMAND ----------

source_df = spark.read.format("json").load("dbfs:/data/dataset_ch7/people.json")
display(source_df)

# COMMAND ----------

flight_time_df = (spark.read.format("json")
                  .load("dbfs:/data/dataset_ch7/people.json")
                  )
display(flight_time_df)

# COMMAND ----------

(people_dt.alias("tgt")
 .merge(source_df.alias("src"),"src.id=tgt.id")
 .whenMatchedDelete(condition = "tgt.firstName='Kailash' and tgt.lastName='Patil'")
 .whenMatchedUpdate(condition= "tgt.id=101",set = {"tgt.birthDate":"src.dob"})
 .whenMatchedUpdate(set = {"tgt.id":"src.id","tgt.firstName":"src.fname","tgt.lastName":"src.lname","tgt.birthDate":"src.dob"})
 .whenNotMatchedInsert(values = {"tgt.id":"src.id","tgt.firstName":"src.fname","tgt.lastName":"src.lname","tgt.birthDate":"src.dob"})
 ).execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo_db.people;

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/databricks-datasets/learning-spark-v2/sf-fire/

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/databricks-datasets/learning-spark-v2/

# COMMAND ----------


