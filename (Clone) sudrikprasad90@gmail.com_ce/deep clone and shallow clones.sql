-- Databricks notebook source
-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC
-- MAGIC spark = SparkSession.builder.appName("my-session").getOrCreate()

-- COMMAND ----------

show catalogs

-- COMMAND ----------

use samples.nyctaxi

-- COMMAND ----------

show tables in samples.tpch

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC spark.catalog.tableExists("samples.nyctaxi.trips")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC spark.catalog.tableExists("samples.nyctaxi.random")

-- COMMAND ----------

create schema if not exists hive_metastore.bronze

-- COMMAND ----------

use hive_metastore

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/bronze.db/emp/_delta_log/", recurse=True)
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/bronze.db/emp/", recurse=True)

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS hive_metastore.bronze;

-- COMMAND ----------

DROP TABLE hive_metastore.bronze.emp

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS hive_metastore.bronze.emp (
  emp_id int,
  emp_name string,
  dept_code string,
  salary double
) USING DELTA

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(_sqldf)

-- COMMAND ----------

use hive_metastore

-- COMMAND ----------

SHOW DATABASES IN hive_metastore;

-- %sql
SHOW TABLES IN hive_metastore.bronze; -- After ensuring 'bronze' exists

-- COMMAND ----------

INSERT INTO hive_metastore.bronze.emp (emp_id, emp_name, dept_code, salary) VALUES (1001,'Shubham','D102',10000);
INSERT INTO hive_metastore.bronze.emp (emp_id, emp_name, dept_code, salary) VALUES (1002,'Ramesh','D101',12000);
INSERT INTO hive_metastore.bronze.emp (emp_id, emp_name, dept_code, salary) VALUES (1003,'Suresh','D101',5000);
INSERT INTO hive_metastore.bronze.emp (emp_id, emp_name, dept_code, salary) VALUES (1004,'Rahim','D102',12000);

-- COMMAND ----------

select * from hive_metastore.bronze.emp;

-- COMMAND ----------

desc extended hive_metastore.bronze.emp

-- COMMAND ----------

select * from hive_metastore.bronze.emp;

-- COMMAND ----------

select * from hive_metastore.bronze.emp@v1

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.mounts())

-- COMMAND ----------

-- create temperory & Permanent Views
CREATE TEMPORARY VIEW emp_temp_vw
AS
SELECT * FROM hive_metastore.bronze.emp 
WHERE dept_code = 'D101';

-- COMMAND ----------

select * from emp_temp_vw;

-- COMMAND ----------

CREATE OR REPLACE VIEW hive_metastore.bronze.emp_vw
AS
SELECT * FROM hive_metastore.bronze.emp
WHERE dept_code = 'D102';

-- COMMAND ----------

-- CTAS for Delta Table
CREATE TABLE hive_metastore.bronze.emp_ctas
AS
SELECT * FROM hive_metastore.bronze.emp;

-- COMMAND ----------

select * from  hive_metastore.bronze.emp_ctas

-- COMMAND ----------

describe extended  hive_metastore.bronze.emp_ctas;

-- COMMAND ----------

describe history  hive_metastore.bronze.emp_ctas

-- COMMAND ----------

-- DEEP CLONE -metadata & data // data will be from latest version of from which you have done the deep clone
CREATE TABLE hive_metastore.bronze.emp_dc DEEP CLONE hive_metastore.bronze.emp

-- COMMAND ----------

describe extended hive_metastore.bronze.emp_dc

-- COMMAND ----------

describe history hive_metastore.bronze.emp_dc
-- check operationParameters -- isShallow false,sorVer - 4

-- COMMAND ----------

-- SHALLOW CLONE --metadata only

CREATE TABLE hive_metastore.bronze.emp_sc SHALLOW CLONE hive_metastore.bronze.emp;

-- COMMAND ----------

describe extended hive_metastore.bronze.emp_sc

-- COMMAND ----------

describe history hive_metastore.bronze.emp_sc;
-- check operationParameters -- isShallow true ,sorVer - 4

-- COMMAND ----------

select * from hive_metastore.bronze.emp_sc

-- COMMAND ----------

-- UPDATING the SOURCE TABLE)emp) Does'nt effect SHALLOW table

-- COMMAND ----------

INSERT INTO hive_metastore.bronze.emp (emp_id, emp_name, dept_code, salary) VALUES (1005,'Pooja','D101',25000);

-- COMMAND ----------

select * from bronze.emp

-- COMMAND ----------

select * from bronze.emp_sc;
-- no change in shallow table only when source table is vaccumed only then the shallow table is affected

-- COMMAND ----------

-- MAGIC %md
-- MAGIC If inserted or updated a shallow table no change on source table (emp) but new datafile for shallow table will be created.

-- COMMAND ----------

-- INSERT Into SHALLOW TABLE
INSERT INTO hive_metastore.bronze.emp_sc (emp_id, emp_name, dept_code, salary) VALUES (1006,'Pooja','D101',25000);

-- COMMAND ----------

describe history bronze.emp_sc

-- COMMAND ----------


