# Databricks notebook source
# Run the child notebook with Parameter -- sales or production or office
dbutils.notebook.run(
    "Write_Emp_Data",
    600,
    {"dept" : "sales"}
)
