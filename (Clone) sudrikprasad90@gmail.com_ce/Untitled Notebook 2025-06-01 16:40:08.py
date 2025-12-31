# Databricks notebook source
from pyspark.sql import SparkSession
# Initialize Spark session
myspark = SparkSession.builder.appName("YourAppName").getOrCreate()

# COMMAND ----------

myspark.conf.set("spark.databricks.io.cache.enabled",False)

# COMMAND ----------


