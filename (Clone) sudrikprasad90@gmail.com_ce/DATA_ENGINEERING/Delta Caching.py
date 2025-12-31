# Databricks notebook source
from pyspark.sql.types import StructType,StructField,IntegerType,StringType

data =[
    (2020, 'Liam', 'USA', 'M', 19659),
    (2020, 'Noah', 'USA', 'M', 18252),
    (2020, 'Oliver', 'USA', 'M', 14147),
    (2020, 'Elijah', 'USA', 'M', 12708),
    (2020, 'James', 'USA', 'M', 12367),
    (2020, 'Olivia', 'USA', 'F', 17535),
    (2020, 'Emma', 'USA', 'F', 15581),
    (2020, 'Ava', 'USA', 'F', 13084),
    (2020, 'Sophia', 'USA', 'F', 12976),
    (2020, 'Isabella', 'USA', 'F', 12066),
    
    (2021, 'Liam', 'USA', 'M', 20272),
    (2021, 'Noah', 'USA', 'M', 18739),
    (2021, 'Oliver', 'USA', 'M', 14616),
    (2021, 'Elijah', 'USA', 'M', 12945),
    (2021, 'James', 'USA', 'M', 12367),
    (2021, 'Olivia', 'USA', 'F', 17728),
    (2021, 'Emma', 'USA', 'F', 15433),
    (2021, 'Charlotte', 'USA', 'F', 13285),
    (2021, 'Amelia', 'USA', 'F', 12952),
    (2021, 'Sophia', 'USA', 'F', 12976),
    
    (2022, 'Liam', 'USA', 'M', 20456),
    (2022, 'Noah', 'USA', 'M', 18739),
    (2022, 'Oliver', 'USA', 'M', 14616),
    (2022, 'Elijah', 'USA', 'M', 12522),
    (2022, 'Lucas', 'USA', 'M', 11501),
    (2022, 'Olivia', 'USA', 'F', 16728),
    (2022, 'Emma', 'USA', 'F', 14435),
    (2022, 'Charlotte', 'USA', 'F', 13285),
    (2022, 'Amelia', 'USA', 'F', 12976),
    (2022, 'Sophia', 'USA', 'F', 12496),
    
    (2023, 'Liam', 'USA', 'M', 20456),
    (2023, 'Noah', 'USA', 'M', 18621),
    (2023, 'Oliver', 'USA', 'M', 14741),
    (2023, 'James', 'USA', 'M', 12367),
    (2023, 'Elijah', 'USA', 'M', 11979),
    (2023, 'Olivia', 'USA', 'F', 16384),
    (2023, 'Emma', 'USA', 'F', 14136),
    (2023, 'Charlotte', 'USA', 'F', 13527),
    (2023, 'Amelia', 'USA', 'F', 12976),
    (2023, 'Sophia', 'USA', 'F', 12321),
    
    (2024, 'Liam', 'USA', 'M', 20802),
    (2024, 'Noah', 'USA', 'M', 18995),
    (2024, 'Oliver', 'USA', 'M', 14838),
    (2024, 'James', 'USA', 'M', 12504),
    (2024, 'Benjamin', 'USA', 'M', 11690),
    (2024, 'Olivia', 'USA', 'F', 16573),
    (2024, 'Emma', 'USA', 'F', 14265),
    (2024, 'Charlotte', 'USA', 'F', 13744),
    (2024, 'Amelia', 'USA', 'F', 13208),
    (2024, 'Sophia', 'USA', 'F', 12456),
    
    # Additional countries
    (2023, 'Muhammad', 'UK', 'M', 4661),
    (2023, 'Noah', 'UK', 'M', 4382),
    (2023, 'Oliver', 'UK', 'M', 3946),
    (2023, 'Olivia', 'UK', 'F', 3758),
    (2023, 'Amelia', 'UK', 'F', 3462),
    
    (2023, 'Liam', 'Canada', 'M', 1847),
    (2023, 'Noah', 'Canada', 'M', 1726),
    (2023, 'Olivia', 'Canada', 'F', 1658),
    (2023, 'Emma', 'Canada', 'F', 1432),
    (2023, 'Charlotte', 'Canada', 'F', 1289),
]

schema = StructType([
    StructField("Year",IntegerType()),
    StructField("First_Name",StringType()),
    StructField("Country",StringType()),
    StructField("Gender",StringType()),
    StructField("Count",IntegerType()),
])

df = spark.createDataFrame(data=data,schema=schema)
display(df)

# COMMAND ----------

df.write.format("delta").saveAsTable("names")

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled","true")

# COMMAND ----------

spark.conf.get("spark.databricks.io.cache.enabled")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM names;

# COMMAND ----------

# MAGIC %md
# MAGIC Preload and Cache Entire Delta Table into Local Disk

# COMMAND ----------

# MAGIC %sql
# MAGIC CACHE SELECT * from names;

# COMMAND ----------

# MAGIC %md
# MAGIC ## This time Select Comes from Local Disk

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from names;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM names WHERE Year = 2027;

# COMMAND ----------


