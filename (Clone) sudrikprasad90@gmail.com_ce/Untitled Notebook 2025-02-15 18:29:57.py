# Databricks notebook source
pip install croniter

# COMMAND ----------

from croniter import croniter
from datetime import datetime

cron_expr = "*/5 * * * *"
base_time = datetime.now()

cron = croniter(cron_expr,base_time)
print(base_time)

print(cron.get_next(datetime))
print(cron.get_next(datetime))

# COMMAND ----------

print(cron.get_prev(datetime))

# COMMAND ----------

# MAGIC %md
# MAGIC check data matches a cron schedule

# COMMAND ----------

print(cron.get_next(float))

# COMMAND ----------

cron2 = croniter('0 | * * *',datetime.now())
print(cron2.get_next(datetime))

# COMMAND ----------

# MAGIC %debug
# MAGIC

# COMMAND ----------

import pandas as pd

# Create a sample dataframe
data = {
    'Name': ['Alice', 'Bob', 'Charlie', 'David', 'Emma', 'Frank', 'Grace', 'Hannah'],
    'Age': [25, 30, 35, 40, 22, 45, 28, 32],
    'Department': ['HR', 'IT', 'Finance', 'IT', 'Marketing', 'Finance', 'HR', 'Marketing'],
    'Salary': [60000, 75000, 85000, 95000, 55000, 90000, 62000, 70000],
    'Experience': [2, 5, 8, 10, 1, 12, 3, 6]
}

df = pd.DataFrame(data)

# COMMAND ----------

print(df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df_skip = df.filter("Department = 'HR'")
print(df_skip)

# COMMAND ----------

display(df_skip)
