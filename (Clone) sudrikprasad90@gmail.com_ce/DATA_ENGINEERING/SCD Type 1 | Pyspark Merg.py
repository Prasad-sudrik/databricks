# Databricks notebook source
print("hello")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

schema = StructType([ \
        StructField("emp_id",IntegerType(),True), \
         StructField("name",StringType(),True), \
        StructField("city",StringType(),True), \
        StructField("country",StringType(),True), \
        StructField("contact_no",IntegerType(),True) \
                     ])


# COMMAND ----------

data = [(1000,"Micheal","Columbus","USA",689546323)]

df = spark.createDataFrame(data=data,schema=schema)
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dim_employee (
# MAGIC   emp_id INT,
# MAGIC   name STRING,
# MAGIC   city STRING,
# MAGIC   country STRING,
# MAGIC   contact_no INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "/FileStore/tables/delta_merge"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_employee

# COMMAND ----------

# MAGIC %md
# MAGIC ## METHOD 1 SQL

# COMMAND ----------

df.createOrReplaceTempView("source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from source_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO dim_employee as target
# MAGIC USING source_view as source
# MAGIC on target.emp_id = source.emp_id
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC   target.name = source.name,
# MAGIC   target.city = source.city,
# MAGIC   target.country = source.country,
# MAGIC   target.contact_no = source.contact_no
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (emp_id,name,city,country,contact_no) VALUES (emp_id,name,city,country,contact_no)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_employee;

# COMMAND ----------

data = [(1000,"Michael","Chicago","USA",689546323),(2000,"Nancy","New York","USA",76345902)]

df = spark.createDataFrame(data=data,schema=schema)
display(df)

# COMMAND ----------

df.createOrReplaceTempView("source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from source_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_employee;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO dim_employee as target
# MAGIC USING source_view as source
# MAGIC on target.emp_id = source.emp_id
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC   target.name = source.name,
# MAGIC   target.city = source.city,
# MAGIC   target.country = source.country,
# MAGIC   target.contact_no = source.contact_no
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC INSERT (emp_id,name,city,country,contact_no) VALUES (emp_id,name,city,country,contact_no)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_employee;

# COMMAND ----------

data = [(2000,"Sarah","New York","USA",76345902),(3000,"David","Atlanta","USA",563456787)]

df = spark.createDataFrame(data=data,schema=schema)
display(df)

# COMMAND ----------

from delta.tables import *
delta_df = DeltaTable.forPath(spark,'/FileStore/tables/delta_merge')

# COMMAND ----------

delta_df.alias("target").merge(
    source=df.alias("source"),
    condition="target.emp_id = source.emp_id "
).whenMatchedUpdate(set=
    {
        "name":"source.name",
        "city":"source.city",
        "country":"source.country",
        "contact_no":"source.contact_no"
    }
).whenNotMatchedInsert(values =
    {
        "emp_id":"source.emp_id",      
        "name":"source.name",
        "city" : "source.city",
        "country" : "source.country",
        "contact_no":"source.contact_no"
    }          
                       ).execute()

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dim_employee;

# COMMAND ----------

print("Hello World")

# COMMAND ----------

data = [
    ("James",None,"Smith","M"),
    ("Micheal","Rose",None,"M"),
    ("Robert",None,"Williams","M"),
    ("Maria","Anne","Jones","F"),
    ("Jen","Mary","Brown","F")
]

schema = "firstname string,middlename string,lastname string,gender string"
df = spark.createDataFrame(data=data,schema=schema)
display(df)

# COMMAND ----------

jdf_multi.unpersist()

# COMMAND ----------

df_multi = df.select(concat_ws("-","firstname","middlename","lastname","gender").alias("FullInfo"))
df_multi.show(truncate=False)

# COMMAND ----------

df_multi.explain()

# COMMAND ----------

m = df.withColumn("dummy",lit("nothing"))

# COMMAND ----------

m.explain()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/databricks-datasets/weather/

# COMMAND ----------

k = spark.read.option("header",True).csv("dbfs:/databricks-datasets/weather/")

# COMMAND ----------

display(k)

# COMMAND ----------

wk = spark.read.parquet("/")

# COMMAND ----------

f = (
    
)
