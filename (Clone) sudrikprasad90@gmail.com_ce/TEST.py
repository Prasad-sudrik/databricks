# Databricks notebook source
n = [1,2,3]
sum = 0
for i in n:
    sum += i
print(sum)

# COMMAND ----------

from pyspark.sql import SparkSession;

spark = SparkSession.builder.master("local[1]").appName("myspark").getOrCreate()

# COMMAND ----------

dataList = [("Surya",10,"Ind"),("Butler",150,"England"),("Rahul",80,"Ind")]
rdd_player = spark.sparkContext.parallelize(dataList)
rdd_player.collect()

# COMMAND ----------

List = ["Germany India USA","USA India Russia","India Brazil Canada China"]
rdd = spark.sparkContext.parallelize(List)
wordCount = rdd.flatMap(lambda line: line.split(" "))
wordCount = wordCount.map(lambda item:(item,1))
wordCount = wordCount.reduceByKey(lambda a,b: a+b)
wordCount.foreach(lambda x: print(x))

for item in wordCount.collect():
    print(item[0])
# wordCount.collect()
# wordCount = wordCount.map
# wordCount = wordCount.reduceByKey((a,b)=>a+b)
# wordCount.foreach(println)

# COMMAND ----------

data = [
    (1, "Alice", "HR"),
    (2, "Bob", "Finance"),
    (1, "Alice", "HR"),      # duplicate
    (3, "Alice", "Finance")
]

df = spark.createDataFrame(data,["id","name","dept"])

# COMMAND ----------

# display(df)
df = df.dropDuplicates(["id"])


# COMMAND ----------

raw_rdd = spark.sparkContext.textFile('dbfs:/FileStore/data/matches.csv')

# COMMAND ----------

print(raw_rdd.take(2))


# COMMAND ----------

header_rdd = raw_rdd.first()

# COMMAND ----------

raw_rdd.first()

# COMMAND ----------

no_header_rdd = raw_rdd.filter(lambda x:x != header_rdd)

# COMMAND ----------

city_rdd = no_header_rdd.map(lambda x:x.split(",")[2])

# COMMAND ----------

city_rdd.first()

# COMMAND ----------

city_rdd = city_rdd.filter(lambda x :x!='')

# COMMAND ----------

map_rdd = city_rdd.map(lambda x:(x,1))

# COMMAND ----------

matches_rdd = map_rdd.reduceByKey(lambda a,b: a+b)

# COMMAND ----------

matches_rdd.take(4)

# COMMAND ----------

print(no_header_rdd.take(2))

# COMMAND ----------

sort_dd = matches_rdd.map(lambda x: (x[1],x[0])).sortByKey(False) # for descending - False
sort_dd.take(5)

# COMMAND ----------

sort_dd.collect()

# COMMAND ----------

for i in sort_dd.collect():
    print(i)

# COMMAND ----------

no_header_rdd.collect()

winner_rdd = no_header_rdd.map(lambda x:x.split(",")[10])

# COMMAND ----------

winner_rdd.take(1)
winner_rdd = winner_rdd.map(lambda x:(x,1))
winner_rdd.take(5)

# COMMAND ----------



# COMMAND ----------

winners_count = winner_rdd.reduceByKey(lambda a,b:a+b)
winners_count.take(5)

# COMMAND ----------

winners_count = winners_count.map(lambda x: (x[1],x[0]))
winners_count.take(5)

# COMMAND ----------

winners_count = winners_count.sortByKey(False)
winners_count.take(5)

# COMMAND ----------

x = no_header_rdd.map(lambda x:x.split(" "))

# COMMAND ----------

'dbutils.fs.rm("dbfs:/user/hive/warehouse/employee_salary/_delta_log",True)

# COMMAND ----------

# MAGIC %md
# MAGIC #repartition and coalesce

# COMMAND ----------

list1 = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]

# COMMAND ----------

rdd = spark.sparkContext.parallelize(list1)

# COMMAND ----------

rdd.getNumPartitions()

# COMMAND ----------

rdd.set

# COMMAND ----------

rdd.saveAsTextFile('dbfs:/data/mynewsample')

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/data/mynewsample"))

# COMMAND ----------

rdd1 = rdd.repartition(4)
rdd1.saveAsTextFile('dbfs:/data/mynewsample_repart')

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/data/mynewsample_repart"))

# COMMAND ----------

# MAGIC %fs head dbfs:/data/mynewsample_repart/part-00002

# COMMAND ----------


# import pandas as pd

# df1 = pd.read_csv("/dbfs/FileStore/shared_uploads/sudrikprasad90@gmail.com/spotify_tracks.csv")

# df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/sudrikprasad90@gmail.com/spotify_tracks-1.csv")

dbutils.fs.rm("dbfs:/FileStore/shared_uploads/sudrikprasad90@gmail.com/spotify_tracks-1.csv")

# To delete a directory and all its contents recursively:


# COMMAND ----------

df1.show()

# COMMAND ----------

# Check available catalogs first
spark.sql("SHOW CATALOGS").show()

# Create schema in your workspace catalog (usually 'main' or your custom catalog)
spark.sql("CREATE SCHEMA IF NOT EXISTS spark_catalog.my_schema")


# COMMAND ----------

display(dbutils.fs.ls("dbfs:/user/hive/warehouse/demo_db.db/people/"))

# COMMAND ----------

rdd1 = rdd.repartition(2)
