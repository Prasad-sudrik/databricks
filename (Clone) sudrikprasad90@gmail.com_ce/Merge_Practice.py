# Databricks notebook source
# MAGIC %fs mounts

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create SparkSession
spark = SparkSession.builder.getOrCreate()

# Sample data
data = [
    ("John", "HR", 28, "New York"),
    ("Alice", "IT", 34, "San Francisco"),
    ("Bob", "Finance", 45, "Chicago"),
    ("Eve", "Marketing", 29, "Boston")
]

# Define schema
schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Department", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("City", StringType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)


# COMMAND ----------

# Write DataFrame as Delta format
df.write.format("delta").mode("overwrite").save("/tmp/four_column_data_delta")


# COMMAND ----------

# dbutils.fs.rm("dbfs:/tmp",True)

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/"))

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS four_column_table
    USING DELTA
    LOCATION '/tmp/four_column_data_delta'
""")


# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended `default`.`four_column_table`;

# COMMAND ----------

from pyspark.sql import Row

# Source has two extra columns: "Salary" and "UpdatedBy"
source_data = [
    Row("John", "HR", 30, "Seattle", 70000, "admin"),
    Row("Alice", "IT", 34, "San Francisco", 80000, "admin"),
    Row("Nina", "Legal", 41, "Austin", 90000, "hr")
]

columns = ["Name", "Department", "Age", "City", "Salary", "UpdatedBy"]
source_df = spark.createDataFrame(source_data, schema=columns)

# COMMAND ----------

from delta.tables import DeltaTable

# Load target table
target = DeltaTable.forPath(spark, "/tmp/four_column_data_delta")

# Merge
target.alias("t").merge(
    source_df.alias("s"),
    "t.Name = s.Name"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()


# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from default.four_column_table;

# COMMAND ----------

from pyspark.sql import Row

# Source has two extra columns: "Salary" and "UpdatedBy"
source_data = [
    Row("John", "HR", 20, "Seattle", 1000, "admin"),
    Row("Alice", "IT", 20, "San Francisco", 1000, "admin"),
    Row("Nina", "Legal", 20, "Austin", 1000, "hr")
]

columns = ["Name", "Department", "Age", "City", "Salary", "UpdatedBy"]
source_df = spark.createDataFrame(source_data, schema=columns)

# COMMAND ----------

from delta.tables import DeltaTable

# Load target table
target = DeltaTable.forPath(spark, "/tmp/four_column_data_delta")

# Merge
target.alias("t").merge(
    source_df.alias("s"),
    "t.Name = s.Name"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from default.four_column_table;

# COMMAND ----------

from pyspark.sql.functions import lit

# Read the original table
df = spark.table("default.four_column_table")

# Add the new columns (with null or default values)
df_updated = df.withColumn("Salary", lit(None).cast("int")) \
               .withColumn("UpdatedBy", lit(None).cast("string"))

# Overwrite the data in the table
df_updated.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("default.four_column_table")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.four_column_table;

# COMMAND ----------

newdf = spark.read.format("delta").load("/tmp/four_column_data_delta")

# COMMAND ----------

display(newdf)

# COMMAND ----------

from pyspark.sql import Row

# New data with 2 existing names and 2 new ones, plus extra columns
new_data = [
    Row("John", "HR", 31, "Seattle", 72000, "admin"),     # existing → update
    Row("Alice", "IT", 34, "San Francisco", 85000, "bot"),# existing → update
    Row("Nina", "Legal", 40, "Austin", 90000, "hr"),       # new → insert
    Row("Liam", "R&D", 27, "Denver", 68000, "ops")         # new → insert
]

columns = ["Name", "Department", "Age", "City", "Salary", "UpdatedBy"]
source_df = spark.createDataFrame(new_data, schema=columns)

source_df.show()


# COMMAND ----------

from delta.tables import DeltaTable
target = DeltaTable.forPath(spark, "/tmp/four_column_data_delta")

target.alias("t").merge(
    source_df.alias("s"),
    "t.Name = s.Name"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.four_column_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.four_column_table version as of 1;

# COMMAND ----------


