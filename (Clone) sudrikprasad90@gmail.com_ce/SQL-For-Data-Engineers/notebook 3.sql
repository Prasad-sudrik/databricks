-- Databricks notebook source

CREATE DATABASE IF NOT EXISTS hive_metastore.demo_db

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS hive_metastore.my_schema

-- COMMAND ----------

DROP DATABASE hive_metastore.my_schema

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from delta.tables import DeltaTable
-- MAGIC DeltaTable.isDeltaTable(spark, "dbfs:/user/hive/warehouse/demo_db.db/people")
-- MAGIC

-- COMMAND ----------

DESCRIBE EXTENDED  demo_db.people

-- COMMAND ----------



-- COMMAND ----------

drop table demo_db.people

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/demo_db.db/people",True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.json("dbfs:/data/dataset_ch7/people.json")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.saveAsTable("demo_db.people")

-- COMMAND ----------

drop table demo_db.people

-- COMMAND ----------


CREATE OR REPLACE TABLE demo_db.people(
  id INT,
  firstName STRING,
  lastName STRING,
  birthDate STRING
) USING DELTA

-- COMMAND ----------

select * from demo_db.people;

-- COMMAND ----------

DESCRIBE EXTENDED demo_db.people

-- COMMAND ----------

INSERT OVERWRITE TABLE demo_db.people
SELECT id, fname as firstName, lname as lastName, dob as birthDate
FROM JSON.`/data/dataset_ch7/people.json`

-- COMMAND ----------

select * from demo_db.people;

-- COMMAND ----------

delete from demo_db.people where firstName = "M David"

-- COMMAND ----------

update demo_db.people 
set firstName = initCap(firstName),
lastName = initCap(lastName)
where birthDate = '1975-05-25'

-- COMMAND ----------

merge into demo_db.people tgt
using (select id,fname as firstName,lname as lastName,dob as birthDate      from json.`/data/dataset_ch7/people.json`) src
on tgt.id = src.id
when matched and tgt.firstName = "Kailash" then
  delete
when matched then
  update set tgt.birthDate = src.birthDate
when not matched then
  insert *

-- COMMAND ----------

DESCRIBE HISTORY demo_db.people

-- COMMAND ----------

SELECT * FROM demo_db.people

-- COMMAND ----------

SELECT * FROM demo_db.people VERSION AS OF 1

-- COMMAND ----------

SELECT * FROM demo_db.people TIMESTAMP AS OF '2025-01-01T06:48:04.000+00:00'

-- COMMAND ----------

SELECT * FROM demo_db.people TIMESTAMP AS OF '2025-01-01T06:47:04.000+00:00'

-- COMMAND ----------

delete from demo_db.people;

-- COMMAND ----------

select * from demo_db.people

-- COMMAND ----------

describe history demo_db.people

-- COMMAND ----------

restore table demo_db.people to timestamp as of '2025-01-01T07:12:49.000+00:00'

-- COMMAND ----------

select * from demo_db.people

-- COMMAND ----------

describe history demo_db.people

-- COMMAND ----------

insert into table demo_db.people values (104,'Prasad','Sudrik','1990-09-02');

-- COMMAND ----------

insert into table demo_db.people values (105,'Virat','Kolhi','1986-09-12');

-- COMMAND ----------

insert into table demo_db.people values (106,'Rohit','sharma','1989-08-22');

-- COMMAND ----------

update demo_db.people set birthDate = '2001-08-02' where firstName='Prasad'

-- COMMAND ----------

describe history demo_db.people

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.format("delta").option("versionAsOf","1").table("demo_db.people").display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.format("delta").option("timestampAsOf","2025-01-01T07:23:44.000+00:00").table("demo_db.people").display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from delta import DeltaTable
-- MAGIC
-- MAGIC people_dt = DeltaTable.forName(spark,"demo_db.people")
-- MAGIC
-- MAGIC people_dt.restoreToVersion(1)

-- COMMAND ----------


