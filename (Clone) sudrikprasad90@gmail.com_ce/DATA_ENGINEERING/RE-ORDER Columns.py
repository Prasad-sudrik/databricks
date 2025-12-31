# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE COLUMNREORDERDEMO (col1 LONG, col2 LONG, col3 LONG, col4 LONG, col5 LONG,
# MAGIC                                           col6 LONG, col7 LONG, col8 LONG, col9 LONG, col10 LONG,
# MAGIC                                           col11 LONG, col12 LONG, col13 LONG, col14 LONG, col15 LONG,
# MAGIC                                           col16 LONG, col17 LONG, col18 LONG, col19 LONG, col20 LONG,
# MAGIC                                           col21 LONG, col22 LONG, col23 LONG, col24 LONG, col25 LONG,
# MAGIC                                           col26 LONG, col27 LONG, col28 LONG, col29 LONG, col30 LONG,
# MAGIC                                           col31 LONG, col32 LONG, col33 LONG, col34 LONG, col35 LONG)
# MAGIC                                   USING DELTA
# MAGIC                                   LOCATION '/FileStore/tables/reordercolumn'

# COMMAND ----------

simpleData = (
                (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35), \
                (10,20,30,40,50,60,70,80,90,100,110,120,130,140,150,160,170,180,190,200,210,220,230,240,250,260,270,280,290,300,310,320,330,340,350)
)

columns = ["col1","col2","col3","col4","col5","col6","col7","col8","col9","col10","col11","col12","col13","col14","col15","col16","col17","col18","col19","col20","col21","col22","col23","col24","col25","col26","col27","col28","col29","col30","col31","col32","col33","col34","col35"]

df = spark.createDataFrame(data = simpleData,schema=columns)
df.display()

# COMMAND ----------

df.write.format("delta").mode("append").saveAsTable("ColumnReorderDemo")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM columnreorderdemo;

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/reordercolumn/_delta_log/

# COMMAND ----------

# MAGIC %fs
# MAGIC head /FileStore/tables/reordercolumn/_delta_log/00000000000000000001.json

# COMMAND ----------

# DBTITLE 1,ReOrder Column 35 to first
# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE columnreorderdemo CHANGE COLUMN col35 FIRST

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from columnreorderdemo;

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/reordercolumn/_delta_log/

# COMMAND ----------

# MAGIC %fs
# MAGIC head /FileStore/tables/reordercolumn/_delta_log/00000000000000000002.json

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE columnreorderdemo CHANGE COLUMN col35 AFTER COL10

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESC EXTENDED  columnreorderdemo;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from columnreorderdemo;

# COMMAND ----------

simpleData = (
               (100,200,300,400,500,600,700,800,900,1000,1100,1200,1300,1400,1500,1600,1700,1800,1900,2000,2100,2200,2300,2400,2500,2600,2700,2800,2900,3000,3100,3200,3300,3400,350), \
                  (1000,2000,3000,4000,5000,6000,7000,8000,9000,10000,11000,12000,13000,14000,15000,16000,17000,18000,19000,20000,21000,22000,23000,24000,25000,26000,27000,28000,29000,30000,31000,32000,33000,34000,35000)
)
columns = ["col1","col2","col3","col4","col5","col6","col7","col8","col9","col10","col11","col12","col13","col14","col15","col16","col17","col18","col19","col20","col21","col22","col23","col24","col25","col26","col27","col28","col29","col30","col31","col32","col33","col34","col35"]

df = spark.createDataFrame(data = simpleData,schema=columns)
df.display()

# COMMAND ----------

df.write.format("delta").mode("append").saveAsTable("columnreorderdemo")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/reordercolumn/_delta_log/

# COMMAND ----------

# MAGIC %fs
# MAGIC head /FileStore/tables/reordercolumn/_delta_log/00000000000000000004.json 

# COMMAND ----------

# MAGIC %fs
# MAGIC head /FileStore/tables/reordercolumn/_delta_log/00000000000000000001.json 

# COMMAND ----------

# DBTITLE 1,Recompute Old Delta Data Files
# MAGIC %scala
# MAGIC import com.databricks.sql.transaction.tahoe._
# MAGIC import org.apache.spark.sql.catalyst.TableIdentifier
# MAGIC import com.databricks.sql.transaction.tahoe.stats.StatisticsCollection
# MAGIC
# MAGIC val tableName = "ColumnReorderDemo"
# MAGIC val deltaLog = DeltaLog.forTable(spark,TableIdentifier(tableName))
# MAGIC
# MAGIC StatisticsCollection.recompute(spark,deltaLog)

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/reordercolumn/_delta_log/

# COMMAND ----------

# MAGIC %fs head /FileStore/tables/reordercolumn/_delta_log/00000000000000000005.json  

# COMMAND ----------


